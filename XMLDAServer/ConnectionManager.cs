//        Copyright (c) 2017-2018, Semyon A. Chertkov (semyonc@gmail.com)
//        All rights reserved.
//
//        This program is free software: you can redistribute it and/or modify
//        it under the terms of the GNU General Public License as published by
//        the Free Software Foundation, either version 3 of the License, or
//        any later version.

using System;
using System.Text;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Opc;
using Opc.Da;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.ServiceModel;

namespace XMLDAServer
{
    public class ConnectionHandle: IDisposable
    {

        Connection connection;

        public ConnectionHandle(Connection _connection)
        {
            connection = _connection;            
        }

        public string ServerUrl
        {
            get
            {
                return connection.serverUrl;
            }
        }

        public string UID
        {
            get
            {
                return connection.GetUID();
            }
        }

        public Opc.Da.Server Server
        {
            get
            {
                return connection.server;
            }
        }

        public Opc.Da.ServerStatus Status
        {
            get
            {
                return connection.status;
            }
        }

        public string LocalID
        {
            get
            {
                return connection.localId;
            }
        }

        public string[] SupportedLocales
        {
            get
            {
                return connection.supportedLocales;
            }
        }

        public Subscription CreateSubscription()
        {
            return new Subscription(connection);
        }
       

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    ConnectionManager.SharedInstance.Deactivate(connection);
                    connection = null;
                }

                // TODO: free unmanaged resources (unmanaged objects) and override a finalizer below.
                // TODO: set large fields to null.

                disposedValue = true;
            }
        }

        // TODO: override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources.
        // ~ServerResource() {
        //   // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
        //   Dispose(false);
        // }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            // TODO: uncomment the following line if the finalizer is overridden above.
            //GC.SuppressFinalize(this);
        }
        #endregion
    }


    public class Connection
    {
        internal object lockRoot;

        public volatile bool active;

        public string serverUrl;

        public Opc.Da.Server server;
        public Opc.Da.ServerStatus status;
        public string localId;
        public string[] supportedLocales;

        public Connection(string aServerUrl, Opc.Da.Server aServer)
        {
            lockRoot = new object();
            active = true;
            serverUrl = aServerUrl;
            server = aServer;
            Ping();
        }

        public string GetUID()
        {
            return String.Format("0x{0:X8}", server.GetHashCode());
        }

        public void Ping()
        {
            localId = server.GetLocale();
            supportedLocales = server.GetSupportedLocales();
            status = server.GetStatus();
        }

        public bool Validate()
        {
            try
            {
                ServiceManager.LogMessage("[PingConnections] Validate server {0} handle {1}", serverUrl, GetUID());
                Ping();                
            }
            catch (Exception ex)
            {
                ServiceManager.LogMessage("[PingConnections] Error validating {0}: {1}",
                    GetUID(), ex.Message);
                return false;
            }
            return true;
        }
  
        public void LogServerStatus()
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine("Server OK!");
            sb.AppendFormat("Server start time: {0}\n", status.StartTime);
            sb.AppendFormat("Server current time: {0}\n", status.CurrentTime);
            sb.AppendFormat("Last update time: {0}\n", status.LastUpdateTime);
            sb.AppendFormat("Product Version: {0}\n", status.ProductVersion);
            if (status.StatusInfo != null)
                sb.AppendFormat("Status info: {0}\n", status.StatusInfo);
            if (status.VendorInfo != null)
                sb.AppendFormat("Vendor info: {0}\n", status.VendorInfo);
            ServiceManager.LogMessage(sb.ToString());
        }

        public bool HaveSubscriptions
        {
            get
            {
                return (server.Subscriptions != null) && (server.Subscriptions.Count > 0);
            }
        }

        public bool TryEnter(int timeout = 300)
        {
            bool lockTaken = false;
            Monitor.TryEnter(lockRoot, timeout, ref lockTaken);
            return lockTaken;
        }

        public void Enter()
        {            
            Monitor.Enter(lockRoot);
        }

        public void Exit()
        {
            Monitor.Exit(lockRoot);
        }
    }

    public class ConnectionManager
    {
        private static ConnectionManager sharedInstance = null;

        public static ConnectionManager SharedInstance
        {
            get
            {
                if (sharedInstance == null)
                    sharedInstance = new ConnectionManager();
                return sharedInstance;
            }
        }

        private class Counter
        {
            public volatile int numConnections = 0;
        }

        private Timer timer;
        private object watchDogMutex = new object();

        private CancellationTokenSource source;
        private CancellationToken token;

        public bool IsShutdown { get { return token.IsCancellationRequested; } }

        private ConcurrentDictionary<string, ConcurrentQueue<Connection>> servers;
        private ConcurrentDictionary<string, Counter> counters;
        private ConcurrentDictionary<string, Subscription> subscriptions;

        private ConnectionManager()
        {
            source = new CancellationTokenSource();
            token = source.Token;

            servers = new ConcurrentDictionary<string, ConcurrentQueue<Connection>>();
            counters = new ConcurrentDictionary<string, Counter>();
            subscriptions = new ConcurrentDictionary<string, Subscription>();
        }

        public IEnumerable<Subscription> GetSubscriptions(Connection connection)
        {
            foreach (Subscription subscription in subscriptions.Values)
                if (subscription.Connection == connection)
                    yield return subscription;
        }

        internal void AddSubscription(Subscription subscription)
        {
            subscriptions.TryAdd(subscription.ServerSubHandle, subscription);
            ServiceManager.LogMessage(String.Format("Added subscription {0} (KeepAlive = {1})", 
                subscription.ServerSubHandle, subscription.PingTimestamp));
        }

        internal void RemoveSubscription(Subscription subscription)
        {
            subscriptions.TryRemove(subscription.ServerSubHandle, out subscription);
            ServiceManager.LogMessage(String.Format("Removed subscription {0}", subscription.ServerSubHandle));
        }

        public void StartWatchDog()
        {
            ServiceManager.LogMessage("Start watch dog with {0} ms period", ServiceManager.SharedInstance.WatchDogPeriod);
            timer = new Timer(new TimerCallback(WatchDog), null,
                ServiceManager.SharedInstance.WatchDogPeriod, ServiceManager.SharedInstance.WatchDogPeriod);
        }


        private void WatchDog(object state)
        {            
            if (Monitor.TryEnter(watchDogMutex, 100))
            {
                if (IsShutdown)
                    return;
                DateTime now = DateTime.Now;
                foreach (var record in servers)
                {
                    Counter counter = counters[record.Key];
                    ConcurrentQueue<Connection> queue = record.Value;
                    foreach (Connection connection in queue)
                    {
                        if (connection.TryEnter())
                        {
                            if (connection.active)
                            {
                                if (!connection.Validate() || (!connection.HaveSubscriptions && 
                                        counter.numConnections > ServiceManager.SharedInstance.MinConnections))
                                    DestroyConnection(connection);
                                else
                                {
                                    if (connection.HaveSubscriptions)
                                        foreach (Subscription subscription in GetSubscriptions(connection))
                                            if (subscription.PingTimestamp < now || !subscription.GetState())
                                                subscription.Stop();
                                }
                            }
                            connection.Exit();
                        }
                    }
                    if (counter.numConnections == 0)
                    {
                        ServiceManager.LogMessage("[WatchDog] No idle connection found for {0}. Try reconnect", record.Key);
                        try
                        {
                            Connection connection = CreateConnection(record.Key);
                            connection.LogServerStatus();
                            queue.Enqueue(connection);
                        } catch
                        {
                            ServiceManager.LogMessage("[WatchDog] Fail reconnect to server {0}", record.Key);
                        }
                    }
                }
                Monitor.Exit(watchDogMutex);
                timer.Change(ServiceManager.SharedInstance.WatchDogPeriod, 0);
            }            
        }

        private Connection CreateConnection(string serverUrl)
        {
            ServiceManager.LogMessage("Connecting to {0}...", serverUrl);
            URL url = new URL(serverUrl);
            Opc.Da.Server server = null;
            try
            {
                server = new Opc.Da.Server(new OpcCom.Factory(), url);
                Stopwatch stopwatch = new Stopwatch();
                stopwatch.Start();
                try
                {
                    server.Connect();
                    Opc.Da.ServerStatus serverStatus = server.GetStatus();
                    if (serverStatus.ServerState != Opc.Da.serverState.running)
                        throw new Exception(String.Format("Bad server state ({0})", serverStatus.ServerState));
                    Connection connection = new Connection(serverUrl, server);
                    server = null;
                    stopwatch.Stop();
                    ServiceManager.LogMessage("Successful created new connection {0}: {1} ({2} ms)",
                        connection.GetUID(), serverUrl, stopwatch.ElapsedMilliseconds);
                    Counter counter = counters[serverUrl];
                    Interlocked.Increment(ref counter.numConnections);
                    return connection;
                }
                catch (Exception ex)
                {
                    stopwatch.Stop();
                    ServiceManager.LogMessage("Failed create new connection to {0}: {1} ({2} ms)",
                        serverUrl, ex.Message, stopwatch.ElapsedMilliseconds);
                    throw new FaultException(ex.Message, new FaultCode("E_FAIL"));
                }
            }
            finally
            {
                if (server != null)
                {
                    if (server.IsConnected)
                        server.Disconnect();
                    server.Dispose();
                }
            }
        }
       
        private void DestroyConnection(Connection connection)
        {
            try
            {
                if (connection.server != null)
                {
                    Counter counter = counters[connection.serverUrl];
                    if (connection.server.Subscriptions != null && 
                        connection.server.Subscriptions.Count > 0)
                    {
                        foreach (Subscription subscription in GetSubscriptions(connection))
                            subscription.Stop();
                    }
                    if (connection.server.IsConnected)
                    {
                        ServiceManager.LogMessage("Closed connection to server {0} at {1}", 
                            connection.GetUID(), connection.serverUrl);
                        connection.server.Disconnect();
                    }
                    Interlocked.Decrement(ref counter.numConnections);
                    connection.server.Dispose();
                }
            }
            catch (Exception ex)
            {
                ServiceManager.LogMessage("Exception in shutdown OPC server {0} at {1}:\n",
                    connection.GetUID(), ex.ToString());
            }
            connection.active = false;
            connection.server = null;
        }

        public bool RegisterServer(string serverUrl)
        {
            if (servers.ContainsKey(serverUrl))
                return true;
            servers.TryAdd(serverUrl, new ConcurrentQueue<Connection>());
            counters.TryAdd(serverUrl, new Counter());
            try
            {
                ServiceManager.LogMessage("Attempt to validate connection {0} ...", serverUrl);
                Connection connection = CreateConnection(serverUrl);
                connection.LogServerStatus();
                servers[serverUrl].Enqueue(connection);                
                return true;
            }
            catch 
            {                
                return false;
            }
        }

        public void Shutdown()
        {
            ServiceManager.LogMessage("Start shutdown all servers...");
            source.Cancel();
            // stopping watchDog
            timer.Change(Timeout.Infinite, 0);
            Monitor.Enter(watchDogMutex);
            // Complete all request
            while (OperationInvoker.InvokeBalancer > 0)
                Thread.Sleep(300);
            // shutdown all connections async 
            List<Task> taskList = new List<Task>();
            foreach (ConcurrentQueue<Connection> queue in servers.Values)
            {
                while (queue.Count > 0)
                {
                    Connection connection;
                    if (queue.TryDequeue(out connection))
                    {
                        var task = Task.Factory.StartNew(() => {
                            lock (connection.lockRoot)
                                if (connection.active)
                                    DestroyConnection(connection);
                        });
                        taskList.Add(task);
                    }
                }
            }
            Task.WaitAll(taskList.ToArray());
        }

        public ConnectionHandle Open(string serverUrl)
        {
            Connection connection;
            ConcurrentQueue<Connection> queue = servers[serverUrl];
            Counter counter = counters[serverUrl];
            while (true)
            {
                if (IsShutdown)
                    throw new FaultException("The server is an shutdown state...", new FaultCode("E_SERVERSTATE"));
                if (queue.TryDequeue(out connection))
                {
                    connection.Enter();
                    if (connection.active)
                        return new ConnectionHandle(connection);
                    else
                    {
                        connection.Exit();
                    }
                }
                else
                {
                    if (ServiceManager.SharedInstance.MaxConnections == 0 || // Unlimited
                        counter.numConnections < ServiceManager.SharedInstance.MaxConnections)
                        break;
                    // Waiting while polled connections are released
                    Thread.Sleep(1000);
                }
            }
            // No free connections. Try to reconnect
            ServiceManager.LogMessage("No idle connections found for {0}", serverUrl);
            connection = CreateConnection(serverUrl);
            connection.Enter();
            return new ConnectionHandle(connection);
        }

        public void Deactivate(Connection connection)
        {
            connection.Exit();
            servers[connection.serverUrl].Enqueue(connection);
        }

        public Subscription OpenSubscription(string serverSubHandle)
        {
            if (IsShutdown)
                throw new FaultException("The server is an shutdown state...", new FaultCode("E_SERVERSTATE"));
            Subscription subscription;
            if (!subscriptions.TryGetValue(serverSubHandle, out subscription))
                return null;
            //Connection connection = subscription.Connection;
            //if (!connection.TryEnter())
            //    throw new FaultException(String.Format(
            //        "Requested connection for ServerSubHandle {0} is busy", serverSubHandle), new FaultCode("E_BUSY"));            
            return subscription;
        }        
    }
}
