//        Copyright (c) 2017-2018, Semyon A. Chertkov (semyonc@gmail.com)
//        All rights reserved.
//
//        This program is free software: you can redistribute it and/or modify
//        it under the terms of the GNU General Public License as published by
//        the Free Software Foundation, either version 3 of the License, or
//        any later version.

using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Opc.Da;

namespace XMLDAServer
{
    public class Subscription
    {
        enum AggregationType
        {
            First,
            Last,
            Avg,
            Min,
            Max,
            Count,
            Sum,
            S3600
        }

        class ManagedItem
        {
            public Item item;
            public ItemValueResult value;
            public List<ItemValueResult> acc;
            public List<ItemValueResult> buffer;
            public bool minuteFilter;
            public bool updated;
            public bool dirty;
            public AggregationType aggr;

            public ManagedItem(Item item, string query, bool buffered, bool minuteFilter)
            {
                this.item = item;
                if ((item.EnableBufferingSpecified && item.EnableBuffering) || buffered || minuteFilter)
                    buffer = new List<ItemValueResult>();
                this.minuteFilter = minuteFilter;
                acc = new List<ItemValueResult>();
                aggr = AggregationType.First;
                if (query == null) query = "";
                if (query.EndsWith("/FIRST"))
                    aggr = AggregationType.First;
                else if (query.EndsWith("/LAST"))
                    aggr = AggregationType.Last;
                else if (query.EndsWith("/AVG"))
                    aggr = AggregationType.Avg;
                else if (query.EndsWith("/SUM"))
                    aggr = AggregationType.Sum;
                else if (query.EndsWith("/COUNT"))
                    aggr = AggregationType.Count;
                else if (query.EndsWith("/MAX"))
                    aggr = AggregationType.Max;
                else if (query.EndsWith("/MIN"))
                    aggr = AggregationType.Min;
                else if (query.EndsWith("/S3600"))
                    aggr = AggregationType.S3600;
            }

            public ItemValueResult Value
            {
                get
                {
                    if (value == null)
                        return CreateBadResult();
                    updated = false;
                    return value;
                }
            }  
            
            public void Add(ItemValueResult value)
            {
                if (!IsGoodValue(value) || // Do not accumulate the bad values
                    (acc.Count == 1 && !IsGoodValue(acc[0])))
                    acc.Clear();
                acc.Add(value);
                dirty = true;
            }

            public void Update()
            {
                value = GetValue();
                acc.Clear();
                dirty = false;
                updated = true;
            }

            private ItemValueResult GetValue()
            {
                if (acc.Count == 0)
                    return CreateBadResult();
                if (acc.Count == 1)
                    return acc[0];
                DateTime last_ts;
                switch (aggr)
                {
                    case AggregationType.Min:
                        return Select((ItemValueResult value, ref ItemValueResult res) => {
                            if (SafeToDouble(value.Value) < SafeToDouble(res.Value))
                                res = value;
                        });

                    case AggregationType.Max:
                        return Select((ItemValueResult value, ref ItemValueResult res) => {
                            if (SafeToDouble(value.Value) > SafeToDouble(res.Value))
                                res = value;
                        });

                    case AggregationType.Count:
                        return CreateResult(Reduce((value) => 1.0, out last_ts), last_ts);

                    case AggregationType.Sum:
                        return CreateResult(Reduce((value) => SafeToDouble(value.Value), out last_ts), last_ts);

                    case AggregationType.Avg:
                        return CreateResult(Reduce((value) => SafeToDouble(value.Value), out last_ts) / acc.Count, last_ts);

                    case AggregationType.S3600:
                        return CreateResult(Reduce((value) => SafeToDouble(value.Value) / 3600, out last_ts), last_ts);

                    case AggregationType.Last:
                        return Select((ItemValueResult value, ref ItemValueResult res) => {
                            if (value.Timestamp >= res.Timestamp)
                                res = value;
                        });

                    default:
                        return Select((ItemValueResult value, ref ItemValueResult res) => {
                            if (value.Timestamp >= res.Timestamp)
                                res = value;
                        });
                }
            }

            private ItemValueResult CreateResult(object value, DateTime timestamp)
            {
                ItemValueResult res = new ItemValueResult(item);
                res.Timestamp = timestamp;
                res.TimestampSpecified = true;
                res.Value = value;
                res.Quality = Quality.Good;
                res.QualitySpecified = true;
                res.ResultID = Opc.ResultID.S_OK;
                return res;
            }

            private ItemValueResult CreateBadResult()
            {
                ItemValueResult res = new ItemValueResult(item);
                res.TimestampSpecified = false;
                res.Quality = Quality.Bad;
                res.QualitySpecified = true;
                res.ResultID = Opc.ResultID.S_OK;
                return res;
            }

            private delegate void SelectDelegate(ItemValueResult value, ref ItemValueResult res);

            private delegate double RollupDelegate(ItemValueResult value);

            private ItemValueResult Select(SelectDelegate del)
            {
                ItemValueResult res = null;
                if (acc.Count > 0)
                {
                    res = new ItemValueResult(acc[0]);
                    for (int i = 1; i < acc.Count; i++)
                        del(acc[i], ref res);
                }
                return res;
            }

            private double Reduce(RollupDelegate del, out DateTime timestamp)
            {
                double res = 0;
                timestamp = DateTime.MinValue;
                for (int i = 0; i < acc.Count; i++)
                {
                    var value = acc[i];
                    if (value.TimestampSpecified && timestamp < value.Timestamp)
                        timestamp = value.Timestamp;
                    res += del(value);
                }
                return res;
            }
        }

        private Connection connection;
        private ISubscription innerSubscription;

        private ManagedItem[] managedItems;
        private ConcurrentDictionary<string, ManagedItem> itemIndex;
        private object pingLock = new object();
        private DateTime pingTimestamp;
        private long ackTimestamp;
        private Timer timer;

        private bool minuteFilter;

        public readonly AutoResetEvent changed = new AutoResetEvent(false);

        public DateTime PingTimestamp
        {
            get
            {
                lock (pingLock)
                    return pingTimestamp;
            }
        }

        public Connection Connection { get { return connection; } }

        public string ClientHandle { get; set; }

        public float Deadband { get; set; }

        public int PingRate { get; set; }

        public string Locale { get; set; }

        public string Name { get; set; }

        public string ServerSubHandle { get; set; }

        public int UpdateRate { get; set; }

        public bool Active { get; set; }

        public int KeepAlive { get; set; }

        public bool Buffered { get; set; }

        public bool DataBufferOverflow { get; set; }


        public Subscription(Connection connection)
        {
            ServerSubHandle = Guid.NewGuid().ToString("N");
            Active = true;
            this.connection = connection;
        }

        public void Pulse()
        {
            lock (pingLock)
            {
                pingTimestamp = DateTime.Now + TimeSpan.FromMilliseconds(PingRate);
                ServiceManager.LogMessage("Subsciption {0} - update KeepAlive to {1}", ServerSubHandle, pingTimestamp);
            }
        }

        public void Start()
        {
            SubscriptionState state = new SubscriptionState();
            state.ClientHandle = ClientHandle;
            state.ServerHandle = ServerSubHandle;
            state.Deadband = Deadband;
            state.Locale = Locale;
            state.UpdateRate = UpdateRate;
            state.Active = Active;
            state.KeepAlive = KeepAlive;
            innerSubscription = connection.server.CreateSubscription(state);
            innerSubscription.DataChanged += InnerSubscription_DataChanged;
            pingTimestamp = DateTime.Now + TimeSpan.FromMilliseconds(PingRate);
            ConnectionManager.SharedInstance.AddSubscription(this);
            if (ClientHandle == "1C") // 1C hack
                minuteFilter = true;
        }

        public void Stop()
        {
            if (innerSubscription != null)
            {
                try
                {
                    connection.server.CancelSubscription(innerSubscription);
                }
                catch (Exception ex)
                {
                    ServiceManager.LogMessage("[Subscription] Error CancelSubscription in Stop() {0}: {1}\n",
                        ServerSubHandle, ex.ToString());
                }
                ConnectionManager.SharedInstance.RemoveSubscription(this);
                innerSubscription = null;
            }
            if (timer != null)
            {
                timer.Dispose();
                timer = null;
            }
        }

        public bool GetState()
        {
            try
            {
                SubscriptionState state = innerSubscription.GetState();
                Deadband = state.Deadband;
                UpdateRate = state.UpdateRate;
                KeepAlive = state.KeepAlive;
                Locale = state.Locale;
                Active = state.Active;
            }
            catch (Opc.ResultIDException ex)
            {
                ServiceManager.LogMessage("[Subscription] Error GetState() {0}: {1}", ServerSubHandle, ex.Message);
                return false;
            }
            return true;
        }

        private static bool IsGoodValue(ItemValueResult valueRes)
        {
            if (valueRes != null && valueRes.ResultID.Succeeded() &&
                valueRes.QualitySpecified && valueRes.TimestampSpecified)
            {
                if (valueRes.Timestamp != null && valueRes.Quality != null)
                    return (valueRes.Quality.QualityBits == Opc.Da.qualityBits.good);
            }
            return false;
        }

        //private bool Acknowledgment(List<ItemValueResult> values, ItemValueResult newValue)
        //{
        //    if (minuteFilter && values.Count > 0)
        //    {
        //        ItemValueResult oldValue = values[0];
        //        if (IsGoodValue(oldValue) && IsGoodValue(newValue))
        //        {
        //            DateTime timestamp1 = new DateTime(oldValue.Timestamp.Year, oldValue.Timestamp.Month,
        //                oldValue.Timestamp.Day, oldValue.Timestamp.Hour, oldValue.Timestamp.Minute, 0);
        //            DateTime timestamp2 = new DateTime(newValue.Timestamp.Year, newValue.Timestamp.Month,
        //                newValue.Timestamp.Day, newValue.Timestamp.Hour, newValue.Timestamp.Minute, 0);
        //            return (timestamp1 != timestamp2);
        //        }
        //    }
        //    return true;
        //}

        private void Acknowledgment(object state)
        {
            if (1.0 * (DateTime.Now.Ticks - ackTimestamp) / TimeSpan.TicksPerMinute > 0.998)
            {
                bool updated = false;
                foreach (ManagedItem managedItem in itemIndex.Values)
                    lock (managedItem)
                    {
                        if (managedItem.dirty)
                        {
                            managedItem.Update();
                            updated = true;
                        }
                    }
                if (updated)
                    changed.Set();
                SetAckTimestamp();
            }
            timer.Change(250, 0);
        }

        internal static double SafeToDouble(object value)
        {
            try
            {
                if (value != null)
                    return Convert.ToDouble(value);
            }
            catch (Exception ex)
            {
                ServiceManager.LogMessage("[Subscription::SafeToDouble] {0} was thrown:\n{1}",
                    ex.GetType().Name, value.ToString());
            }
            return 0;
        }

        private bool GetItem(ItemValueResult[] values, int index, out ManagedItem res)
        {
            if (itemIndex.TryGetValue(values[index].ClientHandle.ToString(), out res))
                return true;
            else if (values.Length == managedItems.Length)
            {
                res = managedItems[index];
                return true;
            }
            return false;
        }

        private void InnerSubscription_DataChanged(object subscriptionHandle, object requestHandle, ItemValueResult[] values)
        {
            Task.Factory.StartNew(() => ProcessDataChange(values));            
        }

        private void ProcessDataChange(ItemValueResult[] values)
        {
            bool updated = false;
            for (int k = 0; k < values.Length; k++)
            {
                ManagedItem managedItem;
                ItemValueResult value = values[k];
                if (GetItem(values, k, out managedItem))
                    lock (managedItem)
                    {
                        if (managedItem.updated)
                        {
                            ItemValueResult itemValue = managedItem.Value;
                            if (managedItem.buffer != null)
                            {
                                if (managedItem.buffer.Count == ServiceManager.SharedInstance.BufferLimit)
                                {
                                    managedItem.buffer.RemoveAt(0);
                                    DataBufferOverflow = true;
                                }
                                managedItem.buffer.Add(itemValue);
                                updated = true;
                            }
                        }
                        managedItem.Add(value);
                        if (!minuteFilter)
                        {
                            managedItem.Update();
                            updated = true;
                        }
                    }
            }
            if (updated)
                changed.Set();
        }

        public ItemResult[] AddItem(Item[] items, string[] queries)
        {

            managedItems = new ManagedItem[items.Length];
            itemIndex = new ConcurrentDictionary<string, ManagedItem>();
            for (int i = 0; i < items.Length; i++)
            {
                if (items[i].ClientHandle == null || itemIndex.ContainsKey(items[i].ClientHandle.ToString()))
                    items[i].ClientHandle = String.Format("0x{0:X3}", i + 1);
                ManagedItem managedItem = new ManagedItem(new Item(items[i]), queries[i], Buffered, minuteFilter);
                itemIndex.TryAdd(managedItem.item.ClientHandle.ToString(), managedItems[i] = managedItem);
            }
            ItemResult[] res = innerSubscription.AddItems(items);
            if (minuteFilter)
            {
                SetAckTimestamp();
                timer = new Timer(new TimerCallback(Acknowledgment), null, 250, 0);
            }
            return res;
        }

        private void SetAckTimestamp()
        {
            DateTime now = DateTime.Now;
            ackTimestamp = new DateTime(now.Year, now.Month,
                now.Day, now.Hour, now.Minute, 0).Ticks;
        }

        public void Refresh()
        {
            innerSubscription.Refresh();
        }

        public void GetValues(bool allItems, Action<Item, ItemValueResult> addItem)
        {
            foreach (ManagedItem managedItem in itemIndex.Values)
                lock (managedItem)
                {
                    if (managedItem.buffer != null)
                    {
                        for (int k = 0; k < managedItem.buffer.Count; k++)
                            addItem(managedItem.item, managedItem.buffer[k]);
                        managedItem.buffer.Clear();
                    }
                    if (allItems || managedItem.updated)
                        addItem(managedItem.item, managedItem.Value);
                }
            DataBufferOverflow = false;
        }
    }

}