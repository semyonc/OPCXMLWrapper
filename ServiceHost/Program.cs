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
using System.IO;
using System.ServiceModel;
using System.ServiceModel.Description;
using System.Threading;
using System.Text;
using System.ServiceModel.Channels;
using System.Runtime.InteropServices;
using XMLDAServer;

namespace OpcServiceHost
{
    class Program
    {
        static ServiceHost host;
        static AutoResetEvent terminate = new AutoResetEvent(false);

        public static int Main(string[] args)
        {
            try
            {
                string iniFileName = ServiceManager.SharedInstance.IniFileName;
                if (!File.Exists(iniFileName))
                {
                    ServiceManager.LogMessage("[OpcServiceHost] No servers.ini file found '{0}'. Terminated.", iniFileName);
                    return 1;
                }
                PrepareConsole();
                ServiceManager.SharedInstance.Init();
                host = new ServiceHost(typeof(OPCService), new Uri(ServiceManager.SharedInstance.ServiceUrl));
                int numEndpoints = 0;
                ServiceManager.SharedInstance.EnumServers((address, serverUrl, parameters) => {
                    if (ConnectionManager.SharedInstance.RegisterServer(serverUrl))
                    {
                        ServiceEndpoint endpoint;
                        string codepage = parameters["codepage"];
                        if (codepage != null) 
                        {
                            ICollection<BindingElement> bindingElements = new List<BindingElement>();
                            HttpTransportBindingElement httpBindingElement = new HttpTransportBindingElement();
                            CustomTextMessageBindingElement textBindingElement = new CustomTextMessageBindingElement();
                            textBindingElement.Encoding = codepage;
                            bindingElements.Add(textBindingElement);
                            bindingElements.Add(httpBindingElement);
                            CustomBinding customBinding = new CustomBinding(bindingElements);
                            endpoint = host.AddServiceEndpoint(typeof(Service),
                                new BasicHttpBinding() { MaxReceivedMessageSize = ServiceManager.SharedInstance.MaxReceivedMessageSize }, address);
                            endpoint.Binding = customBinding;
                        }
                        else // Default UTF-8
                            endpoint = host.AddServiceEndpoint(typeof(Service), 
                                new BasicHttpBinding() { MaxReceivedMessageSize = ServiceManager.SharedInstance.MaxReceivedMessageSize }, address);
                        ServiceManager.SharedInstance.RegisterEndpoint(endpoint, serverUrl, parameters);
                        ServiceManager.LogMessage("[OpcServiceHost] Created SOAP service endpoint {0}", endpoint.Address);
                        numEndpoints++;
                    }
                });
                if (numEndpoints == 0) {
                    ServiceManager.LogMessage("[OpcServiceHost] No endpoints configured.");
                    return 1;
                }
                foreach (var operation in host.Description.Endpoints[0].Contract.Operations)
                    operation.Behaviors.Add(new CustomInvokerBehavior());
                ConnectionManager.SharedInstance.StartWatchDog();
                host.Open();
                Console.CancelKeyPress += Console_CancelKeyPress;
                ServiceManager.LogMessage("[OpcServiceHost] Starting service host. Press Ctrl+Break for terminate...");
                terminate.WaitOne();
                ConnectionManager.SharedInstance.Shutdown();
                host.Close();
                ServiceManager.LogMessage("[OpcServiceHost] Stopped service host.");
                return 0;
            }
            catch (Exception ex)
            {
                ServiceManager.LogMessage("[OpcServiceHost] Unhandled exception thrown {0}", ex.ToString());
                ServiceManager.LogMessage("[OpcServiceHost] Terminated.");
                return 2;
            }
        }

        private static void Console_CancelKeyPress(object sender, ConsoleCancelEventArgs e)
        {
            ServiceManager.LogMessage("[OpcServiceHost] Received cancel signal. Shutdown service...");
            e.Cancel = true;
            terminate.Set();
        }

        private static void PrepareConsole()
        {
            IntPtr hnd = GetStdHandle(STD_OUTPUT_HANDLE);
            if (hnd != INVALID_HANDLE_VALUE)
            {
                CONSOLE_FONT_INFOEX font_infoEx = new CONSOLE_FONT_INFOEX();
                font_infoEx.cbSize = (uint)Marshal.SizeOf(font_infoEx);
                if (GetCurrentConsoleFontEx(hnd, false, ref font_infoEx))
                {
                    // Set console font to Lucida Console.
                    font_infoEx.FontFamily = TMPF_TRUETYPE;
                    font_infoEx.FaceName = fontName;
                    SetCurrentConsoleFontEx(hnd, false, ref font_infoEx);
                }
            }

            Console.Title = "OPC XML DA 1.00 Server";
            Console.OutputEncoding = Encoding.Unicode;
            Console.BackgroundColor = ConsoleColor.DarkBlue;
            Console.ForegroundColor = ConsoleColor.White;

            Console.SetWindowSize(100, 30);
            Console.Clear();
        }

        #region PInvoke Win32

        private const int STD_OUTPUT_HANDLE = -11;
        private const int TMPF_TRUETYPE = 4;
        private const int FONT_FAMILY = 54;

        private static string fontName = "Lucida Console";
        private static IntPtr INVALID_HANDLE_VALUE = new IntPtr(-1);

        // Import GetStdHandle to get a handle on the terminal output.
        [DllImport("kernel32.dll", SetLastError = true)]
        static extern IntPtr GetStdHandle(int nStdHandle);

        // Import GetCurrentConsoleFontEx for current font info.
        [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        static extern bool SetCurrentConsoleFontEx(IntPtr consoleOutput, bool maximumWindow,
            [In,Out] ref CONSOLE_FONT_INFOEX lpConsoleCurrentFontEx);

        // Import SetCurrentConsoleFontEx to change the font.
        [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        extern static bool GetCurrentConsoleFontEx(IntPtr hConsoleOutput, bool bMaximumWindow, 
            [In,Out] ref CONSOLE_FONT_INFOEX lpConsoleCurrentFont);

        // Define the CONSOLE_FONT_INFOEX structure.
        [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Unicode)]
        internal struct CONSOLE_FONT_INFOEX
        {
            public uint cbSize;
            public int FontIndex;
            public short FontWidth;
            public short FontHeight;
            public int FontFamily;
            public int FontWeight;
            [MarshalAs(UnmanagedType.ByValTStr, SizeConst = 32)]
            public string FaceName;
        }

        #endregion
    }
}
