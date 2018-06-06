//        Copyright (c) 2017-2018, Semyon A. Chertkov (semyonc@gmail.com)
//        All rights reserved.
//
//        This program is free software: you can redistribute it and/or modify
//        it under the terms of the GNU General Public License as published by
//        the Free Software Foundation, either version 3 of the License, or
//        any later version.

using System;
using System.Diagnostics;
using System.ServiceModel;
using System.ServiceModel.Dispatcher;
using System.Threading;
using System.Reflection;

namespace XMLDAServer
{
    // Based on https://github.com/Duikmeester/WF_WCF_Samples/blob/master/WCF/Extensibility/Instancing/Durable/CS/extensions/OperationInvoker.cs
    //   and https://stackoverflow.com/questions/6016568/wcf-wait-for-operations-to-finish-when-closing-servicehost
    public class OperationInvoker : IOperationInvoker
    {
        private IOperationInvoker innerOperationInvoker;

        public OperationInvoker(IOperationInvoker innerOperationInvoker)
        {
            this.innerOperationInvoker = innerOperationInvoker;
        }

        public bool IsSynchronous
        {
            get { return innerOperationInvoker.IsSynchronous; }
        }

        public object[] AllocateInputs()
        {
            return innerOperationInvoker.AllocateInputs();
        }

        public static volatile int InvokeBalancer = 0;

        public object Invoke(object instance, object[] inputs, out object[] outputs)
        {
            string invokeContext = "";
            Interlocked.Increment(ref InvokeBalancer);
            Stopwatch stopwatch = new Stopwatch();
            try
            {
                try
                {
                    OPCService service = instance as OPCService;
                    if (service != null && inputs.Length == 1)
                    {
                        object input = inputs[0];
                        Type inputType = input.GetType();
                        FieldInfo[] fields = inputType.GetFields();
                        if (fields != null && fields.Length > 0)
                            invokeContext = fields[0].Name;
                    }
                    stopwatch.Start();
                    object result = innerOperationInvoker.Invoke(instance,
                        inputs, out outputs);
                    stopwatch.Stop();
                    if (service != null && invokeContext != "")
                    {
                        string serverUrl = ServiceManager.SharedInstance.ServerUrlForChannel(OperationContext.Current.Channel);
                        ServiceManager.LogMessage("[OperationInvoker] {0}() {1} processed - {2} item(s), {3} ms.",
                            invokeContext, serverUrl, service.NumProcessed, stopwatch.ElapsedMilliseconds);
                    }
                    return result;
                }    
                catch (FaultException fault)
                {
                    ServiceManager.LogMessage("[OperationInvoker] Fault was thrown during {0}: ({1}) {2}", 
                        invokeContext, fault.Code.Name, fault.Reason.ToString());
                    throw;
                }
                catch (Exception ex)
                {
                    ServiceManager.LogMessage("[OperationInvoker] {0} was thrown during {1}:\n{2}", ex.GetType().Name, invokeContext, ex.Message);
                    throw new FaultException(ex.Message, new FaultCode("E_FAIL"));
                }
            }
            finally
            {
                Interlocked.Decrement(ref InvokeBalancer);
            }
        }

        public IAsyncResult InvokeBegin(object instance, object[] inputs, AsyncCallback callback, object state)
        {
            return innerOperationInvoker.InvokeBegin(instance, inputs, callback, state);
        }

        public object InvokeEnd(object instance, out object[] outputs, IAsyncResult asyncResult)
        {
            // Finish invoking the operation using the inner operation 
            // invoker.
            object result = innerOperationInvoker.InvokeEnd(instance, out outputs, asyncResult);
            return result;
        }
    }
}
