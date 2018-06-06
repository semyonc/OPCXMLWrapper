//        Copyright (c) 2017-2018, Semyon A. Chertkov (semyonc@gmail.com)
//        All rights reserved.
//
//        This program is free software: you can redistribute it and/or modify
//        it under the terms of the GNU General Public License as published by
//        the Free Software Foundation, either version 3 of the License, or
//        any later version.

using System;
using System.ServiceModel.Channels;
using System.ServiceModel.Description;
using System.ServiceModel.Dispatcher;

namespace XMLDAServer
{
    // Based on https://github.com/Duikmeester/WF_WCF_Samples/blob/master/WCF/Extensibility/Instancing/Durable/CS/extensions/SaveStateAttribute.cs,
    // https://blogs.msdn.microsoft.com/carlosfigueira/2011/04/11/wcf-extensibility-ioperationbehavior/
    public class CustomInvokerBehavior : IOperationBehavior
    {
        public CustomInvokerBehavior()
        {

        }

        public void AddBindingParameters(OperationDescription operationDescription, BindingParameterCollection bindingParameters)
        {
        }

        public void ApplyClientBehavior(OperationDescription operationDescription, ClientOperation clientOperation)
        {
        }

        public void ApplyDispatchBehavior(OperationDescription operationDescription, DispatchOperation dispatchOperation)
        {
            if (dispatchOperation == null)
                throw new ArgumentNullException("dispatchOperation");

            // Wrap the operation invoker inside our custom operation 
            // invoker which does the persisting work.
            dispatchOperation.Invoker = new OperationInvoker(dispatchOperation.Invoker);
        }

        public void Validate(OperationDescription operationDescription)
        {
        }
    }
}
