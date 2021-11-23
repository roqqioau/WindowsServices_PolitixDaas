using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;

namespace PolitixDaas
{
    static class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        static void Main(string[] args)
        {
            Console.WriteLine("Here 1");
            if (args != null && args.Length == 1 && args[0].Length > 1 && (args[0][0] == '-' || args[0][0] == '/'))
            {
                switch (args[0].Substring(1).ToLower())
                {
                    default:
                        break;
                    case "install":
                    case "i":
                        SelfInstaller.InstallMe();
                        break;
                    case "uninstall":
                    case "u":
                        SelfInstaller.UninstallMe();
                        break;
                    case "console":
                    case "c":
                        Console.WriteLine("Here");
                        Service1 service = new Service1();
                        Console.WriteLine("Here service");
                        service.Starty();
                        Console.WriteLine("Service Started...");
                        Console.WriteLine("<press any key to exit...>");
                        Console.Read();
                        service.Stoppy();
                        break;
                }
            }
            else
                System.ServiceProcess.ServiceBase.Run(new Service1());
        }
    }
}
