//        Copyright (c) 2017-2018, Semyon A. Chertkov (semyonc@gmail.com)
//        All rights reserved.
//
//        This program is free software: you can redistribute it and/or modify
//        it under the terms of the GNU General Public License as published by
//        the Free Software Foundation, either version 3 of the License, or
//        any later version.

using System;
using System.Collections.Generic;
using System.ServiceModel;
using System.Diagnostics;
using System.Xml;
using System.Threading;
using System.Reflection;
using System.IO;
using System.Text;

using Opc;
using Opc.Da;
using System.Web;
using System.Security;

namespace XMLDAServer
{    
    public class OPCService : Service
    {
        /// <summary>
        /// Defines identifiers for well-known properties.
        /// </summary>
        public class Property
        {
            /// <remarks/>
            public static readonly XmlQualifiedName DATATYPE = new XmlQualifiedName("dataType", Opc.Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName VALUE = new XmlQualifiedName("value", Opc.Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>    
            public static readonly XmlQualifiedName QUALITY = new XmlQualifiedName("quality", Opc.Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName TIMESTAMP = new XmlQualifiedName("timestamp", Opc.Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName ACCESSRIGHTS = new XmlQualifiedName("accessRights", Opc.Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName SCANRATE = new XmlQualifiedName("scanRate", Opc.Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName EUTYPE = new XmlQualifiedName("euType", Opc.Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName EUINFO = new XmlQualifiedName("euInfo", Opc.Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName ENGINEERINGUINTS = new XmlQualifiedName("engineeringUnits", Opc.Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName DESCRIPTION = new XmlQualifiedName("description", Opc.Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName HIGHEU = new XmlQualifiedName("highEU", Opc.Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName LOWEU = new XmlQualifiedName("lowEU", Opc.Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName HIGHIR = new XmlQualifiedName("highIR", Opc.Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName LOWIR = new XmlQualifiedName("lowIR", Opc.Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName CLOSELABEL = new XmlQualifiedName("closeLabel", Opc.Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>     
            public static readonly XmlQualifiedName OPENLABEL = new XmlQualifiedName("openLabel", Opc.Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName TIMEZONE = new XmlQualifiedName("timeZone", Opc.Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName CONDITION_STATUS = new XmlQualifiedName("conditionStatus", Opc.Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName ALARM_QUICK_HELP = new XmlQualifiedName("alarmQuickHelp", Opc.Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName ALARM_AREA_LIST = new XmlQualifiedName("alarmAreaList", Opc.Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName PRIMARY_ALARM_AREA = new XmlQualifiedName("primaryAlarmArea", Opc.Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName CONDITION_LOGIC = new XmlQualifiedName("conditionLogic", Opc.Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName LIMIT_EXCEEDED = new XmlQualifiedName("limitExceeded", Opc.Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName DEADBAND = new XmlQualifiedName("deadband", Opc.Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName HIHI_LIMIT = new XmlQualifiedName("hihiLimit", Opc.Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName HI_LIMIT = new XmlQualifiedName("hiLimit", Opc.Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName LO_LIMIT = new XmlQualifiedName("loLimit", Opc.Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName LOLO_LIMIT = new XmlQualifiedName("loloLimit", Opc.Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName RATE_CHANGE_LIMIT = new XmlQualifiedName("rangeOfChangeLimit", Opc.Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName DEVIATION_LIMIT = new XmlQualifiedName("deviationLimit", Opc.Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName SOUNDFILE = new XmlQualifiedName("soundFile", Opc.Namespace.OPC_DATA_ACCESS_XML10);

            //======================================================================
            // Complex Data Properties

            /// <remarks/>
            public static readonly XmlQualifiedName TYPE_SYSTEM_ID = new XmlQualifiedName("typeSystemID", Opc.Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName DICTIONARY_ID = new XmlQualifiedName("dictionaryID", Opc.Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName TYPE_ID = new XmlQualifiedName("typeID", Opc.Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName DICTIONARY = new XmlQualifiedName("dictionary", Opc.Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName TYPE_DESCRIPTION = new XmlQualifiedName("typeDescription", Opc.Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName CONSISTENCY_WINDOW = new XmlQualifiedName("consistencyWindow", Opc.Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName WRITE_BEHAVIOR = new XmlQualifiedName("writeBehavior", Opc.Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName UNCONVERTED_ITEM_ID = new XmlQualifiedName("unconvertedItemID", Opc.Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName UNFILTERED_ITEM_ID = new XmlQualifiedName("unfilteredItemID", Opc.Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName DATA_FILTER_VALUE = new XmlQualifiedName("dataFilterValue", Opc.Namespace.OPC_DATA_ACCESS_XML10);

            //======================================================================
            // XML Data Access Properties

            /// <remarks/>
            public static readonly XmlQualifiedName MINIMUM_VALUE = new XmlQualifiedName("minimumValue", Opc.Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName MAXIMUM_VALUE = new XmlQualifiedName("maximumValue", Opc.Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName VALUE_PRECISION = new XmlQualifiedName("valuePrecision", Opc.Namespace.OPC_DATA_ACCESS_XML10);
        }

        public class Error
        {
            /// <summary>
            /// All errors that are defined in the XML-DA specification.
            /// </summary>summary>
            public static readonly XmlQualifiedName E_FAIL = new XmlQualifiedName("E_FAIL", Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName E_OUTOFMEMORY = new XmlQualifiedName("E_OUTOFMEMORY", Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName E_SERVERSTATE = new XmlQualifiedName("E_SERVERSTATE", Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName E_TIMEDOUT = new XmlQualifiedName("E_TIMEDOUT", Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName E_BUSY = new XmlQualifiedName("E_BUSY", Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName E_INVALIDCONTINUATIONPOINT = new XmlQualifiedName("E_INVALIDCONTINUATIONPOINT", Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName E_INVALIDFILTER = new XmlQualifiedName("E_INVALIDFILTER", Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName E_NOSUBSCRIPTION = new XmlQualifiedName("E_NOSUBSCRIPTION", Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName E_INVALIDHOLDTIME = new XmlQualifiedName("E_INVALIDHOLDTIME", Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName E_UNKNOWNITEMNAME = new XmlQualifiedName("E_UNKNOWNITEMNAME", Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName E_INVALIDITEMNAME = new XmlQualifiedName("E_INVALIDITEMNAME", Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName E_UNKNOWNITEMPATH = new XmlQualifiedName("E_UNKNOWNITEMPATH", Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName E_INVALIDITEMPATH = new XmlQualifiedName("E_INVALIDITEMPATH", Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName E_NOTSUPPORTED = new XmlQualifiedName("E_NOTSUPPORTED", Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName E_BADTYPE = new XmlQualifiedName("E_BADTYPE", Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName E_RANGE = new XmlQualifiedName("E_RANGE", Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName E_READONLY = new XmlQualifiedName("E_READONLY", Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName E_WRITEONLY = new XmlQualifiedName("E_WRITEONLY", Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName E_INVALIDPID = new XmlQualifiedName("E_INVALIDPID", Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName S_FALSE = new XmlQualifiedName("S_FALSE", Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName S_CLAMP = new XmlQualifiedName("S_CLAMP", Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName S_UNSUPPORTEDRATE = new XmlQualifiedName("S_UNSUPPORTEDRATE", Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName S_DATAQUEUEOVERFLOW = new XmlQualifiedName("S_DATAQUEUEOVERFLOW", Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName E_TYPE_CHANGED = new XmlQualifiedName("E_TYPE_CHANGED", Namespace.OPC_COMPLEX_DATA);
            /// <remarks/>
            public static readonly XmlQualifiedName E_FILTER_DUPLICATE = new XmlQualifiedName("E_FILTER_DUPLICATE", Namespace.OPC_COMPLEX_DATA);
            /// <remarks/>
            public static readonly XmlQualifiedName E_FILTER_INVALID = new XmlQualifiedName("E_FILTER_INVALID", Namespace.OPC_COMPLEX_DATA);
            /// <remarks/>
            public static readonly XmlQualifiedName E_FILTER_ERROR = new XmlQualifiedName("E_FILTER_ERROR", Namespace.OPC_COMPLEX_DATA);
            /// <remarks/>
            public static readonly XmlQualifiedName S_FILTER_NO_DATA = new XmlQualifiedName("S_FILTER_NO_DATA", Namespace.OPC_COMPLEX_DATA);
        }

        public class Type
        {
            /// <remarks/>
            public static readonly XmlQualifiedName ANY_TYPE = new XmlQualifiedName("anyType", Namespace.XML_SCHEMA);
            /// <remarks/>
            public static readonly XmlQualifiedName SBYTE = new XmlQualifiedName("byte", Namespace.XML_SCHEMA);
            /// <remarks/>
            public static readonly XmlQualifiedName BYTE = new XmlQualifiedName("unsignedByte", Namespace.XML_SCHEMA);
            /// <remarks/>
            public static readonly XmlQualifiedName SHORT = new XmlQualifiedName("short", Namespace.XML_SCHEMA);
            /// <remarks/>
            public static readonly XmlQualifiedName USHORT = new XmlQualifiedName("unsignedShort", Namespace.XML_SCHEMA);
            /// <remarks/>
            public static readonly XmlQualifiedName INT = new XmlQualifiedName("int", Namespace.XML_SCHEMA);
            /// <remarks/>
            public static readonly XmlQualifiedName UINT = new XmlQualifiedName("unsignedInt", Namespace.XML_SCHEMA);
            /// <remarks/>
            public static readonly XmlQualifiedName LONG = new XmlQualifiedName("long", Namespace.XML_SCHEMA);
            /// <remarks/>
            public static readonly XmlQualifiedName ULONG = new XmlQualifiedName("unsignedLong", Namespace.XML_SCHEMA);
            /// <remarks/>
            public static readonly XmlQualifiedName FLOAT = new XmlQualifiedName("float", Namespace.XML_SCHEMA);
            /// <remarks/>
            public static readonly XmlQualifiedName DOUBLE = new XmlQualifiedName("double", Namespace.XML_SCHEMA);
            /// <remarks/>
            public static readonly XmlQualifiedName DECIMAL = new XmlQualifiedName("decimal", Namespace.XML_SCHEMA);
            /// <remarks/>
            public static readonly XmlQualifiedName DATETIME = new XmlQualifiedName("dateTime", Namespace.XML_SCHEMA);
            /// <remarks/>
            public static readonly XmlQualifiedName TIME = new XmlQualifiedName("time", Namespace.XML_SCHEMA);
            /// <remarks/>
            public static readonly XmlQualifiedName DATE = new XmlQualifiedName("date", Namespace.XML_SCHEMA);
            /// <remarks/>
            public static readonly XmlQualifiedName DURATION = new XmlQualifiedName("duration", Namespace.XML_SCHEMA);
            /// <remarks/>
            public static readonly XmlQualifiedName BOOLEAN = new XmlQualifiedName("boolean", Namespace.XML_SCHEMA);
            /// <remarks/>
            public static readonly XmlQualifiedName STRING = new XmlQualifiedName("string", Namespace.XML_SCHEMA);
            /// <remarks/>
            public static readonly XmlQualifiedName QNAME = new XmlQualifiedName("QName", Namespace.XML_SCHEMA);
            /// <remarks/>
            public static readonly XmlQualifiedName BINARY = new XmlQualifiedName("base64Binary", Namespace.XML_SCHEMA);
            /// <remarks/>
            public static readonly XmlQualifiedName ARRAY_SBYTE = new XmlQualifiedName("ArrayOfByte", Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName ARRAY_BYTE = new XmlQualifiedName("ArrayOfUnsignedByte", Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName ARRAY_SHORT = new XmlQualifiedName("ArrayOfShort", Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName ARRAY_USHORT = new XmlQualifiedName("ArrayOfUnsignedShort", Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName ARRAY_INT = new XmlQualifiedName("ArrayOfInt", Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName ARRAY_UINT = new XmlQualifiedName("ArrayOfUnsignedInt", Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName ARRAY_LONG = new XmlQualifiedName("ArrayOfLong", Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName ARRAY_ULONG = new XmlQualifiedName("ArrayOfUnsignedLong", Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName ARRAY_FLOAT = new XmlQualifiedName("ArrayOfFloat", Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName ARRAY_DOUBLE = new XmlQualifiedName("ArrayOfDouble", Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName ARRAY_DECIMAL = new XmlQualifiedName("ArrayOfDecimal", Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName ARRAY_DATETIME = new XmlQualifiedName("ArrayOfDateTime", Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName ARRAY_BOOLEAN = new XmlQualifiedName("ArrayOfBoolean", Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName ARRAY_STRING = new XmlQualifiedName("ArrayOfString", Namespace.OPC_DATA_ACCESS_XML10);
            /// <remarks/>
            public static readonly XmlQualifiedName ARRAY_ANY_TYPE = new XmlQualifiedName("ArrayOfAnyType", Namespace.OPC_DATA_ACCESS_XML10);
        }

        private serverState GetServerState(Opc.Da.ServerStatus status)
        {
            switch (status.ServerState)
            {
                case Opc.Da.serverState.commFault:
                    return serverState.commFault;
                case Opc.Da.serverState.failed:
                    return serverState.failed;
                case Opc.Da.serverState.noConfig:
                    return serverState.noConfig;
                case Opc.Da.serverState.running:
                    return serverState.running;
                case Opc.Da.serverState.suspended:
                    return serverState.suspended;
                case Opc.Da.serverState.test:
                default: // !!!!! 
                    return serverState.test;
            }
        }  
        
        private qualityBits GetQualityBits(Opc.Da.qualityBits qa)
        {
            switch (qa)
            {
                case Opc.Da.qualityBits.badCommFailure:
                    return qualityBits.badCommFailure;
                case Opc.Da.qualityBits.badConfigurationError:
                    return qualityBits.badConfigurationError;
                case Opc.Da.qualityBits.badDeviceFailure:
                    return qualityBits.badDeviceFailure;
                case Opc.Da.qualityBits.badLastKnownValue:
                    return qualityBits.badLastKnownValue;
                case Opc.Da.qualityBits.badNotConnected:
                    return qualityBits.badNotConnected;
                case Opc.Da.qualityBits.badOutOfService:
                    return qualityBits.badOutOfService;
                case Opc.Da.qualityBits.badSensorFailure:
                    return qualityBits.badSensorFailure;
                case Opc.Da.qualityBits.badWaitingForInitialData:
                    return qualityBits.badWaitingForInitialData;
                case Opc.Da.qualityBits.good:
                    return qualityBits.good;
                case Opc.Da.qualityBits.goodLocalOverride:
                    return qualityBits.goodLocalOverride;
                case Opc.Da.qualityBits.uncertain:
                    return qualityBits.uncertain;
                case Opc.Da.qualityBits.uncertainEUExceeded:
                    return qualityBits.uncertainEUExceeded;
                case Opc.Da.qualityBits.uncertainLastUsableValue:
                    return qualityBits.uncertainLastUsableValue;
                case Opc.Da.qualityBits.uncertainSensorNotAccurate:
                    return qualityBits.uncertainSensorNotAccurate;                
                case Opc.Da.qualityBits.uncertainSubNormal:
                    return qualityBits.uncertainSubNormal;
                case Opc.Da.qualityBits.bad:
                default:
                    return qualityBits.bad;
            }
        }

        private limitBits GetLimitBits(Opc.Da.limitBits lm)
        {
            switch (lm)
            {
                case Opc.Da.limitBits.constant:
                    return limitBits.constant;
                case Opc.Da.limitBits.high:
                    return limitBits.high;
                case Opc.Da.limitBits.low:
                    return limitBits.low;
                case Opc.Da.limitBits.none:
                default:
                    return limitBits.none;
            }
        }

        private Opc.Da.browseFilter GetBrowseFilter(browseFilter f)
        {
            switch (f)
            {
                case browseFilter.all:
                    return Opc.Da.browseFilter.all;
                case browseFilter.branch:
                    return Opc.Da.browseFilter.branch;
                case browseFilter.item:
                default:
                    return Opc.Da.browseFilter.item;
            }
        }

        private int SafeToInteger(string value)
        {
            int res;
            if (!Int32.TryParse(value, out res))
                return 0;
            return res;
        }

        private string[] FilterStrings(string[] strs)
        {
            List<string> res = new List<string>(strs.Length);
            foreach (string s in strs)
            {
                if (!String.IsNullOrEmpty(s))
                    res.Add(s);
            }
            return res.ToArray();
        }

        private string ParseItemName(object itemName, out string query)
        {
            string str = itemName.ToString();
            query = "";
            int k = str.LastIndexOf("/");
            if (k != -1)
            {
                string key = str.Substring(k);
                if (key == "/LAST" || key == "/FIRST" || key == "/AVG" || key == "/SUM" || 
                    key == "/MIN" || key == "/MAX" || key == "/COUNT" || key == "/S3600")
                {
                    query = str.Substring(k);
                    return str.Substring(0, k);
                }
            }
            return str;
        }

        // these values can show up as return codes from COM-DA servers.
        private const int DISP_E_TYPEMISMATCH = -0x7FFDFFFB; // 0x80020005
        private const int DISP_E_OVERFLOW = -0x7FFDFFF6; // 0x8002000A

        private XmlQualifiedName GetResultID(Opc.ResultID input)
        {
            if (input == Opc.ResultID.S_OK) return null;
            if (input == Opc.ResultID.S_FALSE) return null;
            if (input == Opc.ResultID.E_FAIL) return Error.E_FAIL;
            if (input == Opc.ResultID.E_INVALIDARG) return Error.E_FAIL;
            if (input == Opc.ResultID.E_OUTOFMEMORY) return Error.E_OUTOFMEMORY;
            if (input == Opc.ResultID.E_TIMEDOUT) return Error.E_TIMEDOUT;
            if (input == Opc.ResultID.Da.S_DATAQUEUEOVERFLOW) return Error.S_DATAQUEUEOVERFLOW;
            if (input == Opc.ResultID.Da.S_UNSUPPORTEDRATE) return Error.S_UNSUPPORTEDRATE;
            if (input == Opc.ResultID.Da.S_CLAMP) return Error.S_CLAMP;
            if (input == Opc.ResultID.Da.E_INVALIDHANDLE) return Opc.ResultID.Da.E_INVALIDHANDLE.Name;
            if (input == Opc.ResultID.Da.E_UNKNOWN_ITEM_NAME) return Error.E_UNKNOWNITEMNAME;
            if (input == Opc.ResultID.Da.E_INVALID_ITEM_NAME) return Error.E_INVALIDITEMNAME;
            if (input == Opc.ResultID.Da.E_UNKNOWN_ITEM_PATH) return Error.E_UNKNOWNITEMPATH;
            if (input == Opc.ResultID.Da.E_INVALID_ITEM_PATH) return Error.E_INVALIDITEMPATH;
            if (input == Opc.ResultID.Da.E_INVALID_PID) return Error.E_INVALIDPID;
            if (input == Opc.ResultID.Da.E_READONLY) return Error.E_READONLY;
            if (input == Opc.ResultID.Da.E_WRITEONLY) return Error.E_WRITEONLY;
            if (input == Opc.ResultID.Da.E_BADTYPE) return Error.E_BADTYPE;
            if (input == Opc.ResultID.Da.E_RANGE) return Error.E_RANGE;
            if (input == Opc.ResultID.Da.E_NO_WRITEQT) return Error.E_NOTSUPPORTED;
            if (input == Opc.ResultID.Da.E_NO_ITEM_DEADBAND) return Opc.ResultID.Da.E_NO_ITEM_DEADBAND.Name;
            if (input == Opc.ResultID.Da.E_NO_ITEM_SAMPLING) return Opc.ResultID.Da.E_NO_ITEM_SAMPLING.Name;
            if (input == Opc.ResultID.Da.E_NO_ITEM_BUFFERING) return Opc.ResultID.Da.E_NO_ITEM_BUFFERING.Name;
            if (input == Opc.ResultID.Cpx.E_TYPE_CHANGED) return Error.E_TYPE_CHANGED;
            if (input == Opc.ResultID.Cpx.E_FILTER_DUPLICATE) return Error.E_FILTER_DUPLICATE;
            if (input == Opc.ResultID.Cpx.E_FILTER_INVALID) return Error.E_FILTER_INVALID;
            if (input == Opc.ResultID.Cpx.E_FILTER_ERROR) return Error.E_FILTER_ERROR;
            if (input == Opc.ResultID.Cpx.S_FILTER_NO_DATA) return Error.S_FILTER_NO_DATA;

            // return a generic error code if no name exists for the result id.
            if (input.Name == null)
            {
                if (input.Code == DISP_E_TYPEMISMATCH)
                {
                    return Error.E_BADTYPE;
                }

                if (input.Code == DISP_E_OVERFLOW)
                {
                    return Error.E_RANGE;
                }

                return (input.Succeeded()) ? Error.S_FALSE : Error.E_FAIL;
            }

            // no conversion for unrecognized errors.
            return input.Name;
        }

        private XmlQualifiedName GetType(System.Type input)
        {
            if (input == null) return null;
            if (input == typeof(sbyte)) return Type.SBYTE;
            if (input == typeof(byte)) return Type.BYTE;
            if (input == typeof(short)) return Type.SHORT;
            if (input == typeof(ushort)) return Type.USHORT;
            if (input == typeof(int)) return Type.INT;
            if (input == typeof(uint)) return Type.UINT;
            if (input == typeof(long)) return Type.LONG;
            if (input == typeof(ulong)) return Type.ULONG;
            if (input == typeof(float)) return Type.FLOAT;
            if (input == typeof(double)) return Type.DOUBLE;
            if (input == typeof(decimal)) return Type.DECIMAL;
            if (input == typeof(bool)) return Type.BOOLEAN;
            if (input == typeof(DateTime)) return Type.DATETIME;
            if (input == typeof(string)) return Type.STRING;
            if (input == typeof(sbyte[])) return Type.ARRAY_SBYTE;
            if (input == typeof(byte[])) return Type.BINARY;
            if (input == typeof(short[])) return Type.ARRAY_SHORT;
            if (input == typeof(ushort[])) return Type.ARRAY_USHORT;
            if (input == typeof(int[])) return Type.ARRAY_INT;
            if (input == typeof(uint[])) return Type.ARRAY_UINT;
            if (input == typeof(long[])) return Type.ARRAY_LONG;
            if (input == typeof(ulong[])) return Type.ARRAY_ULONG;
            if (input == typeof(float[])) return Type.ARRAY_FLOAT;
            if (input == typeof(double[])) return Type.ARRAY_DOUBLE;
            if (input == typeof(decimal[])) return Type.ARRAY_DECIMAL;
            if (input == typeof(bool[])) return Type.ARRAY_BOOLEAN;
            if (input == typeof(DateTime[])) return Type.ARRAY_DATETIME;
            if (input == typeof(string[])) return Type.ARRAY_STRING;
            if (input == typeof(object[])) return Type.ARRAY_ANY_TYPE;

            return Type.ANY_TYPE;
        }

        private System.Type GetType(XmlQualifiedName input)
        {
            if (input == null) return null;
            if (input == Type.SBYTE) return typeof(sbyte);
            if (input == Type.BYTE) return typeof(byte);
            if (input == Type.SHORT) return typeof(short);
            if (input == Type.USHORT) return typeof(ushort);
            if (input == Type.INT) return typeof(int);
            if (input == Type.UINT) return typeof(uint);
            if (input == Type.LONG) return typeof(long);
            if (input == Type.ULONG) return typeof(ulong);
            if (input == Type.FLOAT) return typeof(float);
            if (input == Type.DOUBLE) return typeof(double);
            if (input == Type.DECIMAL) return typeof(decimal);
            if (input == Type.BOOLEAN) return typeof(bool);
            if (input == Type.DATETIME) return typeof(DateTime);
            if (input == Type.STRING) return typeof(string);
            if (input == Type.ANY_TYPE) return typeof(object);
            if (input == Type.BINARY) return typeof(byte[]);
            if (input == Type.ARRAY_SBYTE) return typeof(sbyte[]);
            if (input == Type.ARRAY_BYTE) return typeof(byte[]);
            if (input == Type.ARRAY_SHORT) return typeof(short[]);
            if (input == Type.ARRAY_USHORT) return typeof(ushort[]);
            if (input == Type.ARRAY_INT) return typeof(int[]);
            if (input == Type.ARRAY_UINT) return typeof(uint[]);
            if (input == Type.ARRAY_LONG) return typeof(long[]);
            if (input == Type.ARRAY_ULONG) return typeof(ulong[]);
            if (input == Type.ARRAY_FLOAT) return typeof(float[]);
            if (input == Type.ARRAY_DOUBLE) return typeof(double[]);
            if (input == Type.ARRAY_DECIMAL) return typeof(decimal[]);
            if (input == Type.ARRAY_BOOLEAN) return typeof(bool[]);
            if (input == Type.ARRAY_DATETIME) return typeof(DateTime[]);
            if (input == Type.ARRAY_STRING) return typeof(string[]);
            if (input == Type.ARRAY_ANY_TYPE) return typeof(object[]);

            return Opc.Type.ILLEGAL_TYPE;
        }

        private OPCQuality GetQuality(Opc.Da.Quality input)
        {
            OPCQuality output = new OPCQuality();

            switch (input.QualityBits)
            {
                case Opc.Da.qualityBits.bad: { output.QualityField = qualityBits.bad; break; }
                case Opc.Da.qualityBits.badConfigurationError: { output.QualityField = qualityBits.badConfigurationError; break; }
                case Opc.Da.qualityBits.badNotConnected: { output.QualityField = qualityBits.badNotConnected; break; }
                case Opc.Da.qualityBits.badDeviceFailure: { output.QualityField = qualityBits.badDeviceFailure; break; }
                case Opc.Da.qualityBits.badSensorFailure: { output.QualityField = qualityBits.badSensorFailure; break; }
                case Opc.Da.qualityBits.badLastKnownValue: { output.QualityField = qualityBits.badLastKnownValue; break; }
                case Opc.Da.qualityBits.badCommFailure: { output.QualityField = qualityBits.badCommFailure; break; }
                case Opc.Da.qualityBits.badOutOfService: { output.QualityField = qualityBits.badOutOfService; break; }
                case Opc.Da.qualityBits.badWaitingForInitialData: { output.QualityField = qualityBits.badWaitingForInitialData; break; }
                case Opc.Da.qualityBits.uncertain: { output.QualityField = qualityBits.uncertain; break; }
                case Opc.Da.qualityBits.uncertainLastUsableValue: { output.QualityField = qualityBits.uncertainLastUsableValue; break; }
                case Opc.Da.qualityBits.uncertainSensorNotAccurate: { output.QualityField = qualityBits.uncertainSensorNotAccurate; break; }
                case Opc.Da.qualityBits.uncertainEUExceeded: { output.QualityField = qualityBits.uncertainEUExceeded; break; }
                case Opc.Da.qualityBits.uncertainSubNormal: { output.QualityField = qualityBits.uncertainSubNormal; break; }
                case Opc.Da.qualityBits.good: { output.QualityField = qualityBits.good; break; }
                case Opc.Da.qualityBits.goodLocalOverride: { output.QualityField = qualityBits.goodLocalOverride; break; }
            }

            switch (input.LimitBits)
            {
                case Opc.Da.limitBits.none: { output.LimitField = limitBits.none; break; }
                case Opc.Da.limitBits.high: { output.LimitField = limitBits.high; break; }
                case Opc.Da.limitBits.low: { output.LimitField = limitBits.low; break; }
                case Opc.Da.limitBits.constant: { output.LimitField = limitBits.constant; break; }
            }

            output.VendorField = input.VendorBits;

            return output;
        }

        private object MarshalPropertyValue(PropertyID propertyID, object input)
        {
            try
            {
                if (input == null) return null;

                if (propertyID == Opc.Da.Property.QUALITY)
                {
                    return GetQuality((Opc.Da.Quality)input);
                }

                if (propertyID == Opc.Da.Property.ACCESSRIGHTS)
                {
                    return input.ToString();
                }

                if (propertyID == Opc.Da.Property.EUTYPE)
                {
                    return input.ToString();
                }

                if (input is System.Type) /* if (propertyID == Opc.Da.Property.DATATYPE) */
                {
                    return GetType((System.Type)input);
                }

                if (input is String)
                {
                    return SanitizeText((string)input);
                }
            }
            catch (Exception ex)
            {
                ServiceManager.LogMessage("[MarshalPropertyValue] Exception = {0}", ex.ToString());
            }

            return input;
        }

        private PropertyID GetPropertyID(XmlQualifiedName input)
        {
            // convert standard properties from xml to unified da.
            FieldInfo[] fields = typeof(Opc.Da.Property).GetFields(BindingFlags.Static | BindingFlags.Public);

            foreach (FieldInfo field in fields)
            {
                PropertyID property = (PropertyID)field.GetValue(typeof(PropertyID));

                if (input.Name == property.Name.Name)
                {
                    return property;
                }
            }

            // attempt to convert property name to a integer property id for unknown properties.
            return new PropertyID(input.Name, -1, input.Namespace);
        }

        private bool needSanitizeItemName;
        private string codePage;
        private Encoding ae = null;

        private string SanitizeText(string text)
        {
            if (!needSanitizeItemName || text == null)
                return text;
            // Filter control characters
            StringBuilder sb = new StringBuilder(text.Length);
            for (int i = 0; i < text.Length; i++)
            {
                char c = text[i];
                if (!Char.IsControl(c))
                    sb.Append(c);
            }
            // Filter special characters
            if (ae == null)
                ae = Encoding.GetEncoding(
                            codePage,
                            new EncoderExceptionFallback(),
                            new DecoderExceptionFallback());
            byte[] oemChars = ae.GetBytes(sb.ToString());
            text = ae.GetString(oemChars);
            return text;
        }

        private XmlQualifiedName GetResultID(Opc.Da.Server server, string local, Opc.ResultID input, Dictionary<XmlQualifiedName, string> errors)
        {
            XmlQualifiedName res = GetResultID(input);
            if (errors != null && !input.Succeeded())
            {
                if (!errors.ContainsKey(res))
                    errors.Add(res, server.GetErrorText(local, input));
            }
            return res;
        }

        public OPCError[] GetErrors(Dictionary<XmlQualifiedName, string> errors)
        {
            OPCError[] res = new OPCError[errors.Count];
            int k = 0;
            foreach (var pair in errors)
            {
                res[k] = new OPCError();
                res[k].ID = pair.Key;
                res[k].Text = pair.Value;
            }
            return res;
        }

        private XmlQualifiedName GetPropertyName(PropertyID input)
        {
            // check for a vendor defined property with no name.
            if (input.Name == null)
            {
                return new XmlQualifiedName(input.ToString(), "http://default.vendor.com/namespace");
            }

            // convert standard properties from unified da to xml. 
            FieldInfo[] fields = typeof(Property).GetFields(BindingFlags.Static | BindingFlags.Public);

            foreach (FieldInfo field in fields)
            {
                XmlQualifiedName property = (XmlQualifiedName)field.GetValue(typeof(PropertyID));

                if (input.Name.Name == property.Name)
                {
                    return property;
                }
            }

            return input.Name;
        }        

        public int NumProcessed { get; set; }

        public BrowseResponse Browse(BrowseRequest request)
        {
            NumProcessed = 0;
            BrowseResponse resp = new BrowseResponse();
            resp.BrowseResult = new ReplyBase();
            resp.BrowseResult.ClientRequestHandle = request.Browse.ClientRequestHandle;
            resp.BrowseResult.RcvTime = DateTime.Now;
            Dictionary<XmlQualifiedName, string> errors = null;
            if (request.Browse.ReturnErrorText)
                errors = new Dictionary<XmlQualifiedName, string>();
            string serverUrl = ServiceManager.SharedInstance.ServerUrlForChannel(
                OperationContext.Current.Channel, out needSanitizeItemName, out codePage);
            BrowseFilters filters = new BrowseFilters();
            filters.BrowseFilter = GetBrowseFilter(request.Browse.BrowseFilter);
            filters.ElementNameFilter = request.Browse.ElementNameFilter;
            filters.MaxElementsReturned = request.Browse.MaxElementsReturned;
            filters.ReturnAllProperties = request.Browse.ReturnAllProperties;
            if (!filters.ReturnAllProperties)
            {
                var propertyNames = request.Browse.PropertyNames;
                if (propertyNames != null)
                {
                    filters.PropertyIDs = new PropertyID[propertyNames.Length];
                    for (int i = 0; i < propertyNames.Length; i++)
                        filters.PropertyIDs[i] = GetPropertyID(propertyNames[i]);
                }
            }
            filters.ReturnPropertyValues = request.Browse.ReturnPropertyValues;
            filters.VendorFilter = request.Browse.VendorFilter;
            using (ConnectionHandle handle = ConnectionManager.SharedInstance.Open(serverUrl))
            {
                Opc.Da.Server server = handle.Server;
                resp.BrowseResult.RevisedLocaleID = server.SetLocale(request.Browse.LocaleID);
                resp.BrowseResult.ServerState = GetServerState(handle.Status);
                Opc.ItemIdentifier itemID = new Opc.ItemIdentifier();
                itemID.ItemName = request.Browse.ItemName;
                itemID.ItemPath = request.Browse.ItemPath;
                Opc.Da.BrowseElement[] elements;
                BrowsePosition position = null;
                if (request.Browse.ContinuationPoint == null)
                    elements = server.Browse(itemID, filters, out position);
                else
                {
                    ServiceManager.LogMessage("[OPCService] BROWSE {0} continue from /{1}/",
                        serverUrl, request.Browse.ContinuationPoint);
                    string[] parts = request.Browse.ContinuationPoint.Split(',');
                    if (parts.Length != 2 || SafeToInteger(parts[1]) != filters.MaxElementsReturned)
                        throw new ArgumentException("Bad ContinuationPoint format");
                    int offset = SafeToInteger(parts[0]);
                    elements = server.Browse(itemID, filters, out position);
                    if (elements == null)
                        throw new FaultException("[OPCService] OPC Browse return NULL !", new FaultCode("E_FAIL"));
                    offset -= elements.Length;
                    while (offset > 0 && position != null)
                    {
                        elements = server.BrowseNext(ref position);
                        offset -= elements.Length;
                    }
                }
                if (position != null)
                {
                    resp.MoreElements = true;
                    resp.ContinuationPoint = string.Format("{0},{1}",
                        elements.Length, filters.MaxElementsReturned);
                }
                if (elements != null)
                {
                    resp.Elements = new BrowseElement[elements.Length];
                    for (int i = 0; i < elements.Length; i++)
                    {
                        Opc.Da.BrowseElement resElement = elements[i];
                        BrowseElement browseElem = new BrowseElement();
                        browseElem.HasChildren = resElement.HasChildren;
                        browseElem.IsItem = resElement.IsItem;
                        browseElem.ItemName = SanitizeText(resElement.ItemName);
                        browseElem.ItemPath = SanitizeText(resElement.ItemPath);
                        browseElem.Name = SanitizeText(resElement.Name);
                        if (resElement.Properties != null)
                        {
                            browseElem.Properties = new ItemProperty[resElement.Properties.Length];
                            for (int j = 0; j < resElement.Properties.Length; j++)
                            {
                                browseElem.Properties[j] = new ItemProperty();
                                browseElem.Properties[j].Description = SanitizeText(resElement.Properties[j].Description);
                                browseElem.Properties[j].ItemName = SanitizeText(resElement.Properties[j].ItemName);
                                browseElem.Properties[j].ItemPath = SanitizeText(resElement.Properties[j].ItemPath);
                                if (request.Browse.ReturnPropertyValues)
                                {
                                    browseElem.Properties[j].ResultID = GetResultID(server, request.Browse.LocaleID, resElement.Properties[j].ResultID, errors);
                                    browseElem.Properties[j].Value = MarshalPropertyValue(resElement.Properties[j].ID,
                                        resElement.Properties[j].Value);
                                }
                            }
                        }
                        resp.Elements[i] = browseElem;
                        NumProcessed++;
                    }
                }
            }
            if (request.Browse.ReturnErrorText)
                resp.Errors = GetErrors(errors);
            resp.BrowseResult.ReplyTime = DateTime.Now;
            return resp;
        }

        public GetPropertiesResponse GetProperties(GetPropertiesRequest request)
        {
            GetPropertiesResponse resp = new GetPropertiesResponse();
            resp.GetPropertiesResult = new ReplyBase();
            resp.GetPropertiesResult.ClientRequestHandle = request.GetProperties.ClientRequestHandle;
            resp.GetPropertiesResult.RcvTime = DateTime.Now;
            Dictionary<XmlQualifiedName, string> errors = null;
            if (request.GetProperties.ReturnErrorText)
                errors = new Dictionary<XmlQualifiedName, string>();
            NumProcessed = 0;
            string serverUrl = ServiceManager.SharedInstance.ServerUrlForChannel(
                OperationContext.Current.Channel, out needSanitizeItemName, out codePage);
            Opc.ItemIdentifier[] itemIds = null;
            if (request.GetProperties.ItemIDs != null)
            {
                itemIds = new Opc.ItemIdentifier[request.GetProperties.ItemIDs.Length];
                for (int i = 0; i < request.GetProperties.ItemIDs.Length; i++)
                {
                    itemIds[i] = new Opc.ItemIdentifier();
                    itemIds[i].ItemName = request.GetProperties.ItemIDs[i].ItemName;
                    string itemPath = request.GetProperties.ItemIDs[i].ItemPath;
                    itemIds[i].ItemPath = !String.IsNullOrEmpty(itemPath) ?
                        itemPath : request.GetProperties.ItemPath;
                }
            }
            PropertyID[] propertyIds = null;
            if (!request.GetProperties.ReturnAllProperties && request.GetProperties.PropertyNames != null)
            {
                propertyIds = new PropertyID[request.GetProperties.PropertyNames.Length];
                for (int i = 0; i < request.GetProperties.PropertyNames.Length; i++)
                    propertyIds[i] = GetPropertyID(request.GetProperties.PropertyNames[i]);
            }
            using (ConnectionHandle handle = ConnectionManager.SharedInstance.Open(serverUrl))
            {
                Opc.Da.Server server = handle.Server;
                resp.GetPropertiesResult.RevisedLocaleID = server.SetLocale(request.GetProperties.LocaleID);
                resp.GetPropertiesResult.ServerState = GetServerState(handle.Status);
                ItemPropertyCollection[] itemPropertyCollection = server.GetProperties(itemIds, propertyIds,
                    request.GetProperties.ReturnPropertyValues);
                resp.PropertyLists = new PropertyReplyList[itemPropertyCollection.Length];
                for (int i = 0; i < itemPropertyCollection.Length; i++)
                {
                    PropertyReplyList propList = new PropertyReplyList();
                    propList.ItemName = SanitizeText(itemPropertyCollection[i].ItemName);
                    propList.ItemPath = SanitizeText(itemPropertyCollection[i].ItemPath);
                    propList.ResultID = GetResultID(itemPropertyCollection[i].ResultID);
                    propList.Properties = new ItemProperty[itemPropertyCollection[i].Count];
                    for (int j = 0; j < itemPropertyCollection[i].Count; j++)
                    {
                        ItemProperty prop = new ItemProperty();
                        prop.Name = itemPropertyCollection[i][j].ID.Code.ToString();
                        prop.Description = SanitizeText(itemPropertyCollection[i][j].Description);
                        prop.ItemName = SanitizeText(itemPropertyCollection[i][j].ItemName);
                        prop.ItemPath = SanitizeText(itemPropertyCollection[i][j].ItemPath);
                        prop.ResultID = GetResultID(server, request.GetProperties.LocaleID, itemPropertyCollection[i][j].ResultID, errors);
                        if (request.GetProperties.ReturnPropertyValues)
                        {
                            prop.Value = MarshalPropertyValue(itemPropertyCollection[i][j].ID,
                                itemPropertyCollection[i][j].Value);
                        }
                        propList.Properties[j] = prop;
                    }
                    resp.PropertyLists[i] = propList;
                    NumProcessed++;
                }
            }
            if (request.GetProperties.ReturnErrorText)
                resp.Errors = GetErrors(errors);
            resp.GetPropertiesResult.ReplyTime = DateTime.Now;
            return resp;
        }

        public GetStatusResponse GetStatus(GetStatusRequest request)
        {
            GetStatusResponse resp = new GetStatusResponse();
            resp.GetStatusResult = new ReplyBase();
            resp.GetStatusResult.ClientRequestHandle = request.GetStatus.ClientRequestHandle;
            resp.GetStatusResult.RcvTime = DateTime.Now;
            string serverUrl = ServiceManager.SharedInstance.ServerUrlForChannel(
                OperationContext.Current.Channel, out needSanitizeItemName, out codePage);
            using (ConnectionHandle handle = ConnectionManager.SharedInstance.Open(serverUrl))
            {
                resp.GetStatusResult.RevisedLocaleID = handle.LocalID;
                resp.GetStatusResult.ServerState = GetServerState(handle.Status);
                resp.Status = new ServerStatus();
                resp.Status.SupportedInterfaceVersions =
                    new interfaceVersion[] { interfaceVersion.XML_DA_Version_1_0 };
                resp.Status.ProductVersion = handle.Status.ProductVersion;
                resp.Status.StartTime = handle.Status.StartTime;
                resp.Status.StatusInfo = handle.Status.StatusInfo;
                resp.Status.SupportedLocaleIDs = FilterStrings(handle.SupportedLocales);
                resp.Status.VendorInfo = SanitizeText(handle.Status.VendorInfo);
                resp.GetStatusResult.ReplyTime = DateTime.Now;
            }
            return resp;
        }

        public ReadResponse Read(ReadRequest request)
        {
            ReadResponse resp = new ReadResponse();
            resp.ReadResult = new ReplyBase();
            resp.ReadResult.ClientRequestHandle = request.Read.Options.ClientRequestHandle;
            resp.ReadResult.RcvTime = DateTime.Now;
            Dictionary<XmlQualifiedName, string> errors = null;
            if (request.Read.Options.ReturnErrorText)
                errors = new Dictionary<XmlQualifiedName, string>();
            resp.RItemList = new ReplyItemList();
            NumProcessed = 0;
            string serverUrl = ServiceManager.SharedInstance.ServerUrlForChannel(
                OperationContext.Current.Channel, out needSanitizeItemName, out codePage);
            using (ConnectionHandle handle = ConnectionManager.SharedInstance.Open(serverUrl))
            {
                Opc.Da.Server server = handle.Server;
                resp.ReadResult.RevisedLocaleID = server.SetLocale(request.Read.Options.LocaleID);
                resp.ReadResult.ServerState = GetServerState(handle.Status);
                ReadRequestItem[] requestItems = request.Read.ItemList.Items;
                Item[] opcItems = new Item[requestItems.Length];
                for (int i = 0; i < requestItems.Length; i++)
                {
                    string query;
                    ReadRequestItem requestItem = requestItems[i];
                    Item item = new Item();
                    item.ItemName = ParseItemName(requestItem.ItemName, out query);
                    item.ItemPath = requestItem.ItemPath;
                    if (requestItem.MaxAgeSpecified)
                    {
                        item.MaxAge = requestItem.MaxAge;
                        item.MaxAgeSpecified = true;
                    }
                    if (requestItem.ReqType != null)
                        item.ReqType = GetType(requestItem.ReqType);
                    opcItems[i] = item;
                }
                resp.RItemList.Items = new ItemValue[requestItems.Length];
                ItemValueResult[] resValues = server.Read(opcItems);
                for (int i = 0; i < resValues.Length; i++)
                {
                    ItemValueResult resValue = resValues[i];
                    ItemValue itemValue = new ItemValue();
                    itemValue.ClientItemHandle = requestItems[i].ClientItemHandle;
                    itemValue.ResultID = GetResultID(server, request.Read.Options.LocaleID, resValue.ResultID, errors);
                    if (request.Read.Options.ReturnItemName)
                        itemValue.ItemName = resValue.ItemName;
                    if (request.Read.Options.ReturnItemPath)
                        itemValue.ItemPath = resValue.ItemPath;
                    if (request.Read.Options.ReturnItemTime)
                    {
                        itemValue.Timestamp = resValue.Timestamp;
                        itemValue.TimestampSpecified = resValue.TimestampSpecified;
                    }
                    if (request.Read.Options.ReturnDiagnosticInfo)
                        itemValue.DiagnosticInfo = SanitizeText(resValue.DiagnosticInfo);
                    if (opcItems[i].ReqType != null)
                        itemValue.ValueTypeQualifier = GetType(opcItems[i].ReqType);
                    itemValue.Value = resValue.Value;
                    if (resValue.QualitySpecified)
                    {
                        itemValue.Quality = new OPCQuality();
                        itemValue.Quality.QualityField = GetQualityBits(resValue.Quality.QualityBits);
                        itemValue.Quality.LimitField = GetLimitBits(resValue.Quality.LimitBits);
                        itemValue.Quality.VendorField = resValue.Quality.VendorBits;
                    }
                    resp.RItemList.Items[i] = itemValue;
                    NumProcessed++;
                }
            }
            if (request.Read.Options.ReturnErrorText)
                resp.Errors = GetErrors(errors);
            resp.ReadResult.ReplyTime = DateTime.Now;
            return resp;
        }

        public SubscribeResponse Subscribe(SubscribeRequest request)
        {
            if (request.Subscribe.SubscriptionPingRate < 0)
                throw new FaultException("Bad value of SubscriptionPingRate", new FaultCode("E_FAIL"));
            SubscribeResponse resp = new SubscribeResponse();
            resp.SubscribeResult = new ReplyBase();
            resp.SubscribeResult.ClientRequestHandle = request.Subscribe.Options.ClientRequestHandle;
            resp.SubscribeResult.RcvTime = DateTime.Now;
            Dictionary<XmlQualifiedName, string> errors = null;
            if (request.Subscribe.Options.ReturnErrorText)
                errors = new Dictionary<XmlQualifiedName, string>();
            resp.RItemList = new SubscribeReplyItemList();
            NumProcessed = 0;
            string serverUrl = ServiceManager.SharedInstance.ServerUrlForChannel(
                OperationContext.Current.Channel, out needSanitizeItemName, out codePage);
            using (ConnectionHandle handle = ConnectionManager.SharedInstance.Open(serverUrl))
            {
                Opc.Da.Server server = handle.Server;
                resp.SubscribeResult.RevisedLocaleID = server.SetLocale(request.Subscribe.Options.LocaleID);
                resp.SubscribeResult.ServerState = GetServerState(handle.Status);
                Subscription subscription = handle.CreateSubscription();
                resp.ServerSubHandle = subscription.ServerSubHandle;
                //subscription.Locale = request.Subscribe.Options.LocaleID;
                subscription.ClientHandle = request.Subscribe.Options.ClientRequestHandle;
                subscription.PingRate = request.Subscribe.SubscriptionPingRate;
                if (request.Subscribe.ItemList.DeadbandSpecified)
                    subscription.Deadband = request.Subscribe.ItemList.Deadband;
                if (request.Subscribe.ItemList.RequestedSamplingRateSpecified)
                    subscription.UpdateRate = request.Subscribe.ItemList.RequestedSamplingRate;
                if (request.Subscribe.ItemList.EnableBufferingSpecified)
                    subscription.Buffered = request.Subscribe.ItemList.EnableBuffering;
                subscription.Start();
                subscription.GetState();
                resp.RItemList.RevisedSamplingRateSpecified = true;
                resp.RItemList.RevisedSamplingRate = subscription.UpdateRate;
                SubscribeRequestItem[] requestItems = request.Subscribe.ItemList.Items;
                Item[] items = new Item[requestItems.Length];
                string[] queries = new string[requestItems.Length];
                for (int i = 0; i < requestItems.Length; i++)
                {
                    SubscribeRequestItem requestItem = requestItems[i];
                    Item item = new Item();
                    item.ClientHandle = requestItem.ClientItemHandle;
                    item.Deadband = requestItem.Deadband;
                    item.DeadbandSpecified = requestItem.DeadbandSpecified;
                    item.EnableBuffering = requestItem.EnableBuffering;
                    item.EnableBufferingSpecified = requestItem.EnableBufferingSpecified;
                    item.ItemName = ParseItemName(requestItem.ItemName, out queries[i]);
                    item.ItemPath = requestItem.ItemPath;
                    item.SamplingRate = requestItem.RequestedSamplingRate;
                    item.SamplingRateSpecified = requestItem.RequestedSamplingRateSpecified;
                    item.ServerHandle = subscription.ServerSubHandle;
                    if (requestItem.ReqType != null)
                        item.ReqType = GetType(requestItem.ReqType);                    
                    items[i] = item;
                }
                ItemResult[] itemResult = subscription.AddItem(items, queries);
                resp.RItemList.Items = new SubscribeItemValue[itemResult.Length];
                for (int i = 0; i < itemResult.Length; i++)
                {
                    SubscribeItemValue itemValue = new SubscribeItemValue();
                    if (items[i].SamplingRateSpecified)
                    {
                        itemValue.RevisedSamplingRateSpecified = itemResult[i].SamplingRateSpecified;
                        itemValue.RevisedSamplingRate = itemResult[i].SamplingRate;
                    }
                    itemValue.ItemValue = new ItemValue();
                    if (request.Subscribe.Options.ReturnItemName)
                        itemValue.ItemValue.ItemName = itemResult[i].ItemName;
                    if (request.Subscribe.Options.ReturnItemPath)
                        itemValue.ItemValue.ItemPath = itemResult[i].ItemPath;
                    itemValue.ItemValue.ClientItemHandle = (string)items[i].ClientHandle;
                    itemValue.ItemValue.ResultID = GetResultID(server, request.Subscribe.Options.LocaleID, itemResult[i].ResultID, errors);
                    if (request.Subscribe.Options.ReturnDiagnosticInfo)
                        itemValue.ItemValue.DiagnosticInfo = SanitizeText(itemResult[i].DiagnosticInfo);
                    resp.RItemList.Items[i] = itemValue;
                    NumProcessed++;
                }
                if (request.Subscribe.ReturnValuesOnReply)
                {
                    subscription.Refresh();
                    int i = 0;
                    subscription.GetValues(true, (item, value) => {
                        ItemValue resValue = resp.RItemList.Items[i++].ItemValue;
                        resValue.ClientItemHandle = item.ClientHandle.ToString();
                        if (request.Subscribe.Options.ReturnItemName)
                            resValue.ItemName = item.ItemName;
                        if (request.Subscribe.Options.ReturnItemPath)
                            resValue.ItemPath = item.ItemPath;
                        if (value != null)
                        {
                            if (value.QualitySpecified)
                            {
                                resValue.Quality = new OPCQuality();
                                resValue.Quality.QualityField = GetQualityBits(value.Quality.QualityBits);
                                resValue.Quality.LimitField = GetLimitBits(value.Quality.LimitBits);
                                resValue.Quality.VendorField = value.Quality.VendorBits;
                            }
                            resValue.ResultID = GetResultID(server, request.Subscribe.Options.LocaleID, value.ResultID, errors);
                            if (request.Subscribe.Options.ReturnItemTime)
                            {
                                resValue.TimestampSpecified = value.TimestampSpecified;
                                resValue.Timestamp = value.Timestamp;
                            }
                            if (request.Subscribe.Options.ReturnDiagnosticInfo)
                                resValue.DiagnosticInfo = SanitizeText(value.DiagnosticInfo);
                            resValue.Value = value.Value;
                        }
                        if (item.ReqType != null)
                            resValue.ValueTypeQualifier = GetType(item.ReqType);
                    });
                }
            }
            if (request.Subscribe.Options.ReturnErrorText)
                resp.Errors = GetErrors(errors);
            resp.SubscribeResult.ReplyTime = DateTime.Now;
            return resp;
        }

        public SubscriptionCancelResponse SubscriptionCancel(SubscriptionCancelRequest request)
        {
            SubscriptionCancelResponse resp = new SubscriptionCancelResponse();
            resp.ClientRequestHandle = request.SubscriptionCancel.ClientRequestHandle;
            Subscription subscription = ConnectionManager.SharedInstance.OpenSubscription(request.SubscriptionCancel.ServerSubHandle);
            if (subscription == null)
                throw new FaultException(String.Format("Unknown ServerSubHandle = {0}", request.SubscriptionCancel.ServerSubHandle), new FaultCode("E_FAIL"));
            subscription.Stop();
            return resp;
        }

        public SubscriptionPolledRefreshResponse SubscriptionPolledRefresh(SubscriptionPolledRefreshRequest request)
        {
            SubscriptionPolledRefreshResponse resp = new SubscriptionPolledRefreshResponse();
            resp.SubscriptionPolledRefreshResult = new ReplyBase();
            resp.SubscriptionPolledRefreshResult.ClientRequestHandle = request.SubscriptionPolledRefresh.Options.ClientRequestHandle;
            resp.SubscriptionPolledRefreshResult.RcvTime = DateTime.Now;
            Dictionary<XmlQualifiedName, string> errors = null;
            if (request.SubscriptionPolledRefresh.Options.ReturnErrorText)
                errors = new Dictionary<XmlQualifiedName, string>();
            List<SubscribePolledRefreshReplyItemList> rItemList = new List<SubscribePolledRefreshReplyItemList>();
            List<string> invalidServerSubHandles = new List<string>();
            NumProcessed = 0;
            // 1. Get all subscriptions
            List<Subscription> workset = new List<Subscription>();
            foreach (string serverSubHandle in request.SubscriptionPolledRefresh.ServerSubHandles)
            {
                Subscription subscription = ConnectionManager.SharedInstance.OpenSubscription(serverSubHandle);
                if (subscription == null)
                {
                    invalidServerSubHandles.Add(serverSubHandle);
                    continue;
                }
                workset.Add(subscription);
            }
            // 2. Wait hold time
            if (request.SubscriptionPolledRefresh.HoldTimeSpecified && workset.Count > 0)
            {
                DateTime now = DateTime.Now;
                if (now < request.SubscriptionPolledRefresh.HoldTime)
                {
                    TimeSpan ts = request.SubscriptionPolledRefresh.HoldTime - now;
                    if (ts.Minutes > ServiceManager.SharedInstance.MaxHoldTime)
                        throw new FaultException(String.Format("Hold time is too long (more server limit {0} minutes)", ServiceManager.SharedInstance.MaxHoldTime), 
                            new FaultCode("E_INVALIDHOLDTIME"));
                    Thread.Sleep(ts);
                }
            }
            // 3. Wait new data if specified
            if (request.SubscriptionPolledRefresh.WaitTime > 0 && workset.Count > 0)
            {
                WaitHandle[] waitHandle = new WaitHandle[workset.Count];
                for (int i = 0; i < workset.Count; i++)
                    waitHandle[i] = workset[i].changed;
                WaitHandle.WaitAny(waitHandle, request.SubscriptionPolledRefresh.WaitTime);
            }
            // 4. Output results
            foreach (Subscription subscription in workset)
            {
                SubscribePolledRefreshReplyItemList rItems = new SubscribePolledRefreshReplyItemList();
                rItems.SubscriptionHandle = subscription.ServerSubHandle;
                resp.DataBufferOverflow |= subscription.DataBufferOverflow;
                List<ItemValue> itemValue = new List<ItemValue>();
                subscription.Connection.Enter();
                Opc.Da.Server server = subscription.Connection.server;
                try
                {
                    subscription.GetValues(request.SubscriptionPolledRefresh.ReturnAllItems, (item, value) => {
                        ItemValue resValue = new ItemValue();
                        resValue.ClientItemHandle = item.ClientHandle.ToString();
                        if (request.SubscriptionPolledRefresh.Options.ReturnItemName)
                            resValue.ItemName = item.ItemName;
                        if (request.SubscriptionPolledRefresh.Options.ReturnItemPath)
                            resValue.ItemPath = item.ItemPath;
                        if (value != null)
                        {
                            if (value.QualitySpecified)
                            {
                                resValue.Quality = new OPCQuality();
                                resValue.Quality.QualityField = GetQualityBits(value.Quality.QualityBits);
                                resValue.Quality.LimitField = GetLimitBits(value.Quality.LimitBits);
                                resValue.Quality.VendorField = value.Quality.VendorBits;
                            }
                            resValue.ResultID = GetResultID(server, request.SubscriptionPolledRefresh.Options.LocaleID, value.ResultID, errors);
                            if (request.SubscriptionPolledRefresh.Options.ReturnItemTime)
                            {
                                resValue.TimestampSpecified = value.TimestampSpecified;
                                resValue.Timestamp = value.Timestamp;
                            }
                            if (request.SubscriptionPolledRefresh.Options.ReturnDiagnosticInfo)
                                resValue.DiagnosticInfo = value.DiagnosticInfo;
                            resValue.Value = value.Value;
                        }
                        if (item.ReqType != null)
                            resValue.ValueTypeQualifier = GetType(item.ReqType);
                        itemValue.Add(resValue);
                    });
                    subscription.Pulse();
                }
                finally
                {
                    subscription.Connection.Exit();
                }
                rItems.Items = itemValue.ToArray();
                rItemList.Add(rItems);
            }
            if (request.SubscriptionPolledRefresh.Options.ReturnErrorText)
                resp.Errors = GetErrors(errors);
            resp.InvalidServerSubHandles = invalidServerSubHandles.ToArray();
            resp.RItemList = rItemList.ToArray();
            resp.SubscriptionPolledRefreshResult.ReplyTime = DateTime.Now;
            return resp;
        }

        public WriteResponse Write(WriteRequest request)
        {
            throw new NotImplementedException();
        }

        #region XML wrappers

        BrowseResponse1 Service.Browse(BrowseRequest request)
        {
            return new BrowseResponse1(Browse(request));
        }

        GetPropertiesResponse1 Service.GetProperties(GetPropertiesRequest request)
        {
            return new GetPropertiesResponse1(GetProperties(request));
        }

        GetStatusResponse1 Service.GetStatus(GetStatusRequest request)
        {
            return new GetStatusResponse1(GetStatus(request));
        }

        ReadResponse1 Service.Read(ReadRequest request)
        {
            return new ReadResponse1(Read(request));
        }

        SubscribeResponse1 Service.Subscribe(SubscribeRequest request)
        {
            return new SubscribeResponse1(Subscribe(request));
        }

        SubscriptionCancelResponse1 Service.SubscriptionCancel(SubscriptionCancelRequest request)
        {
            return new SubscriptionCancelResponse1(SubscriptionCancel(request));
        }

        SubscriptionPolledRefreshResponse1 Service.SubscriptionPolledRefresh(SubscriptionPolledRefreshRequest request)
        {
            return new SubscriptionPolledRefreshResponse1(SubscriptionPolledRefresh(request));
        }

        WriteResponse1 Service.Write(WriteRequest request)
        {
            return new WriteResponse1(Write(request));
        }

        #endregion
    }
}
