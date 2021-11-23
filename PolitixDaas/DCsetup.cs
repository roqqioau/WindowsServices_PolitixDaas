using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace PolitixDaas
{
    public class DCsetup
    {
        public int IntervalMins;

        public String ActiveMQUrl { get; private set; }
        public String LocationsQueueName { get; private set; }
        public String SMTPHost { get; private set; }
        public String SMTPUserName { get; private set; }
        public String SMTPPassword { get; private set; }
        public int SMTPPort { get; private set; }
        public bool SMTPUsesTls { get; private set; }
        public String EmailFrom { get; private set; }
        public String SMTPRecipients { get; private set; }
        public String EmailSubject { get; private set; }

        public String SqlServer { get; private set; }
        public String SqlDatabase { get; private set; }
        public String SqlErsDatabase { get; private set; }
        public String SqlUser { get; private set; }
        public String SqlPassword { get; private set; }
        public bool SqlOsAuthentication { get; private set; }
        public int Debug { get; private set; }
        public int MinSendDate { get; private set; }

 
        public int LocationUpdate
        {
            get => getLocationUpdate();
            set => setLocationUpdate(value);
        }

        private int getLocationUpdate()
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            return anIni.readInteger("System", "LocationUpdate", 0);
        }


        private void setLocationUpdate(int value)
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            anIni.WriteInteger("System", "LocationUpdate", value);
        }
        public DCsetup()
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);

 
            IntervalMins = anIni.readInteger("System", "IntervalMins", 3);

            SMTPHost = anIni.readString("SMTP", "host", "");
            SMTPUserName = anIni.readString("SMTP", "username", "");
            SMTPPassword = anIni.readString("SMTP", "password", "");
            SMTPPort = anIni.readInteger("SMTP", "port", 21);

            String usesTls = anIni.readString("SMTP", "tls", "0");
            SMTPUsesTls = usesTls.Equals("1") || usesTls.ToUpper().Equals("Y");

            EmailFrom = anIni.readString("SMTP", "emailfrom", "");
            String auxstr = anIni.readString("SMTP", "recipients", "");

            SMTPRecipients = auxstr.Replace(";", ",");

            EmailSubject = anIni.readString("SMTP", "emailsubject", "");

            SqlServer = anIni.readString("SQL", "server", "");
            SqlDatabase = anIni.readString("SQL", "database", "");
            SqlUser = anIni.readString("SQL", "userid", "");
            SqlPassword = anIni.readString("SQL", "password", "");
            SqlErsDatabase = anIni.readString("SQL", "ersdatabase", "");

            String osAuthentic = anIni.readString("SQL", "osauthentication", "0");
            SqlOsAuthentication = osAuthentic.Equals("1") || osAuthentic.ToUpper().Equals("Y");


            Debug = anIni.readInteger("SYSTEM", "Debug", 0);
 
            MinSendDate = anIni.readInteger("SYSTEM", "MinSendDate", 20210301);
            ActiveMQUrl = anIni.readString("SYSTEM", "ActiveMQUrl", "");
            LocationsQueueName = anIni.readString("Queues", "Locations", "");

        }



    }
}
