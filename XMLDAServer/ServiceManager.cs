//        Copyright (c) 2017-2018, Semyon A. Chertkov (semyonc@gmail.com)
//        All rights reserved.
//
//        This program is free software: you can redistribute it and/or modify
//        it under the terms of the GNU General Public License as published by
//        the Free Software Foundation, either version 3 of the License, or
//        any later version.

using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.ServiceModel;
using System.ServiceModel.Description;
using System.Web;
using System.IO;
using System.Threading;
using System.Text;
using Opc;

namespace XMLDAServer
{
    public class ServiceManager
    {
        private static ServiceManager _sharedInstance = new ServiceManager();

        public static ServiceManager SharedInstance 
        {
            get
            {
                return _sharedInstance;
            }
        }

        private class Mapping
        {
            public string ServerUrl { get; set; }
            public bool NeedSanitizeItemName { get; set; }

            public string Codepage { get; set; }
        }

        private Dictionary<string, Mapping> opcMapping;

        private string serviceUrl;
        private bool logToFile;
        private string logPath;
        private int watchDogPeriod;
        private int minConnections;
        private int maxConnections;
        private int bufferLimit;
        private int maxHoldTime;
        private long maxReceivedMessageSize;

        private ServiceManager()
        {
            opcMapping = new Dictionary<string, Mapping>();
            logPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "log.txt");
            if (File.Exists(logPath))
                File.Delete(logPath);
        }

        public static int memory_get_usage()
        {
            long ws = System.Diagnostics.Process.GetCurrentProcess().WorkingSet64;
            if (ws > Int32.MaxValue) return Int32.MaxValue;
            return (int)ws;
        }

        private void WriteLogLine(string line)
        {
            Console.WriteLine(line);
            if (logToFile)
            {
                try
                {
                    FileStream output = new FileStream(logPath, FileMode.Append, FileAccess.Write, FileShare.Write);
                    try
                    {
                        StreamWriter writer = new StreamWriter(output, Encoding.UTF8);
                        writer.WriteLine(line);
                        writer.Close();
                    }
                    finally
                    {
                        output.Close();
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("[WriteLogLine] exception = {0}", ex.ToString());
                }
            }
        }

        public static void LogMessage(string message)
        {
            int mem = memory_get_usage() / (1024 * 1024);
            int id = Thread.CurrentThread.ManagedThreadId;
            StringReader reader = new StringReader(message);
            string line;
            bool firstLine = true;
            while ((line = reader.ReadLine()) != null)
            {
                if (firstLine)
                {
                    _sharedInstance.WriteLogLine(String.Format("{0:R} {1:X5} {2,5:F1}M: {3}", DateTime.Now, mem, id, line));
                    firstLine = false;
                }
                else
                    _sharedInstance.WriteLogLine(String.Format("\t\t{0}", line));
            }
            reader.Close();            
        }

        public static void LogMessage(string fmt, params object [] args)
        {
            LogMessage(String.Format(fmt, args));
        }

        public string IniFileName
        {
            get
            {
                return Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "servers.ini");
            }
        }

        public string ServiceUrl
        {
            get
            {
                return serviceUrl;
            }
        }

        public int WatchDogPeriod
        {
            get
            {
                return watchDogPeriod;
            }
        }

        public int MinConnections
        {
            get
            {
                return minConnections;
            }
        }

        public int MaxConnections
        {
            get
            {
                return maxConnections;
            }
        }

        public int BufferLimit
        {
            get
            {
                return bufferLimit;
            }
        }

        public int MaxHoldTime
        {
            get
            {
                return maxHoldTime;
            }
        }

        public long MaxReceivedMessageSize
        {
            get
            {
                return maxReceivedMessageSize;
            }
        }

        public void Init()
        {
            IniFile iniFile = new IniFile(IniFileName);
            logToFile = iniFile.GetIniBoolean("serviceHost", "logToFile", true);
            if (logToFile) LogMessage("Log started.");
            serviceUrl = iniFile.GetIniString("serviceHost", "address", "");
            watchDogPeriod = iniFile.GetIniNumber("serviceHost", "watchDogPeriod", 65000);
            minConnections = iniFile.GetIniNumber("serviceHost", "minConnections", 2);
            LogMessage("minConnections = {0}", minConnections);
            maxConnections = iniFile.GetIniNumber("serviceHost", "maxConnections", 5);
            bufferLimit = iniFile.GetIniNumber("serviceHost", "bufferLimit", 1000);
            maxHoldTime = iniFile.GetIniNumber("serviceHost", "maxHoldTime", 10);
            if (maxConnections > 0)
                LogMessage("maxConnections = {0}", maxConnections);
            else
                LogMessage("maxConnections is unlimited");
            maxReceivedMessageSize = iniFile.GetIniNumber("serviceHost", "maxReceivedMessageSize", 1024 * 1024);
        }

        public delegate void ServerEnumeratorDelegate(string prefix, string url, NameValueCollection parameters);

        public void EnumServers(ServerEnumeratorDelegate iter)
        {
            IniFile iniFile = new IniFile(IniFileName);
            string[] alias = iniFile.GetEntryNames("alias");
            if (alias != null)
            {
                foreach (string opcAlias in alias)
                {
                    string serverUrl = iniFile.GetIniString("alias", opcAlias, null);
                    if (string.IsNullOrEmpty(serverUrl))
                        continue;
                    string[] parts = serverUrl.Split('?');
                    NameValueCollection parameters;
                    if (parts.Length == 2)
                        parameters = HttpUtility.ParseQueryString("?" + parts[1]);
                    else
                        parameters = new NameValueCollection();
                    iter(opcAlias, parts[0], parameters);
                }
            }
        }

        public void RegisterEndpoint(ServiceEndpoint endpoint, string serverUrl, NameValueCollection parameters)
        {
            string codepage = parameters["codepage"];
            opcMapping.Add(endpoint.Address.ToString(), new Mapping() { ServerUrl = serverUrl,
                NeedSanitizeItemName = (codepage != null), Codepage = codepage });
        }

        public string ServerUrlForChannel(IContextChannel channel)
        {
            bool needSanitizeItemName;
            string codePage;
            return ServerUrlForChannel(channel, out needSanitizeItemName, out codePage);
        }

        public string ServerUrlForChannel(IContextChannel channel, out bool needSanitizeItemName, out string codePage)
        {
            Mapping mapping;
            if (opcMapping.TryGetValue(channel.LocalAddress.ToString(), out mapping))
            {
                needSanitizeItemName = mapping.NeedSanitizeItemName;
                codePage = mapping.Codepage;
                return mapping.ServerUrl;
            }
            throw new InvalidOperationException("Can't resolve OPC Server adderss by OperationContext");
        }
    }
}
