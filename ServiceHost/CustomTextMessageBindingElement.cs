//        Copyright (c) 2017-2018, Semyon A. Chertkov (semyonc@gmail.com)
//        All rights reserved.
//
//        This program is free software: you can redistribute it and/or modify
//        it under the terms of the GNU General Public License as published by
//        the Free Software Foundation, either version 3 of the License, or
//        any later version.

using System;
using System.ServiceModel.Channels;
using System.Xml;

namespace OpcServiceHost
{
    public class CustomTextMessageBindingElement : MessageEncodingBindingElement
    {
        private MessageVersion msgVersion;
        private string mediaType;
        private string encoding;
        private XmlDictionaryReaderQuotas readerQuotas;

        public CustomTextMessageBindingElement()
        {
            MessageVersion = MessageVersion.Soap11;
            mediaType = "text/xml";
            readerQuotas = new XmlDictionaryReaderQuotas();
        }

        public override MessageVersion MessageVersion
        {
            get
            {
                return this.msgVersion;
            }

            set
            {
                if (value == null)
                    throw new ArgumentNullException("value");
                this.msgVersion = value;
            }
        }

        public string MediaType
        {
            get
            {
                return this.mediaType;
            }

            set
            {
                if (value == null)
                    throw new ArgumentNullException("value");
                this.mediaType = value;
            }
        }

        public string Encoding
        {
            get
            {
                return this.encoding;
            }

            set
            {
                if (value == null)
                    throw new ArgumentNullException("value");
                this.encoding = value;
            }
        }

        // This encoder does not enforces any quotas for the unsecure messages. The  
        // quotas are enforced for the secure portions of messages when this encoder 
        // is used in a binding that is configured with security.  
        public XmlDictionaryReaderQuotas ReaderQuotas
        {
            get
            {
                return this.readerQuotas;
            }
        }

        #region IMessageEncodingBindingElement Members
        public override MessageEncoderFactory CreateMessageEncoderFactory()
        {
            return new CustomTextMessageEncoderFactory(MediaType, this.encoding, MessageVersion);
        }
        #endregion


        public override BindingElement Clone()
        {
            CustomTextMessageBindingElement res = new CustomTextMessageBindingElement();
            res.MediaType = MediaType;
            res.Encoding = Encoding;
            res.MessageVersion = MessageVersion;
            res.readerQuotas = readerQuotas;
            return res;
        }

        public override IChannelFactory<TChannel> BuildChannelFactory<TChannel>(BindingContext context)
        {
            if (context == null)
                throw new ArgumentNullException("context");

            context.BindingParameters.Add(this);
            return context.BuildInnerChannelFactory<TChannel>();
        }

        public override bool CanBuildChannelFactory<TChannel>(BindingContext context)
        {
            if (context == null)
                throw new ArgumentNullException("context");

            return context.CanBuildInnerChannelFactory<TChannel>();
        }

        public override IChannelListener<TChannel> BuildChannelListener<TChannel>(BindingContext context)
        {
            if (context == null)
                throw new ArgumentNullException("context");

            context.BindingParameters.Add(this);
            return context.BuildInnerChannelListener<TChannel>();
        }

        public override bool CanBuildChannelListener<TChannel>(BindingContext context)
        {
            if (context == null)
                throw new ArgumentNullException("context");

            context.BindingParameters.Add(this);
            return context.CanBuildInnerChannelListener<TChannel>();
        }

        public override T GetProperty<T>(BindingContext context)
        {
            if (typeof(T) == typeof(XmlDictionaryReaderQuotas))
            {
                return (T)(object)this.readerQuotas;
            }
            else
            {
                return base.GetProperty<T>(context);
            }
        }
    }
}

