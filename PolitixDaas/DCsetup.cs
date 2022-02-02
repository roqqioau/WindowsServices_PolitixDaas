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
        public String PricesQueueName { get; private set; }
        public String ProductsQueueName { get; private set; }
        public String SalesQueueName { get; private set; }
        public String PermanentMarkdownsQueueName { get; private set; }
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
        public int PermanentMarkDownsInitialDate { get; private set; }
        public int ResultSet { get; private set; }
        public int LocationModule { get; private set; }
        public int DateFrom { get; set; }
        public int DateTo { get; set; }
        public int LookupIntervalDays { get; set; }

        public bool BlockLocations { get; set; }
        public bool BlockProducts { get; set; }
        public bool BlockPrices { get; set; }
        public bool BlockSales { get; set; }
        public bool BlockPermanentMarkdowns { get; set; }


        public String PermanentMarkdownUpdate
        {
            get => getPermanentMarkdownUpdate();
            set => setPermanentMarkdownUpdate(value);
        }

        private String getPermanentMarkdownUpdate()
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            return anIni.readString("System", "PermanentMarkdownUpdate", "");
        }

        private void setPermanentMarkdownUpdate(String value)
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            anIni.writeString("System", "PermanentMarkdownUpdate", value);
        }

        public String LocationUpdate
        {
            get => getLocationUpdate();
            set => setLocationUpdate(value);
        }

        private String getLocationUpdate()
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            return anIni.readString("System", "LocationUpdate", "");
        }


        private void setLocationUpdate(String value)
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            anIni.writeString("System", "LocationUpdate", value);
        }

        public String ProductUpdate
        {
            get => getProductUpdate();
            set => setProductUpdate(value);
        }

        private String getProductUpdate()
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            return anIni.readString("System", "ProductUpdate", "");

        }

        private void setProductUpdate(String value)
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            anIni.writeString("System", "ProductUpdate", value);
        }


        public String PriceUpdate
        {
            get => getPriceUpdate();
            set => setPriceUpdate(value);
        }

        private String getPriceUpdate()
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            return anIni.readString("System", "PriceUpdate", "");

        }

        private void setPriceUpdate(String value)
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            anIni.writeString("System", "PriceUpdate", value);
        }

        public String SaleUpdate
        {
            get => getSaleUpdate();
            set => setSaleUpdate(value);
        }

        private void setSaleUpdate(String value)
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            anIni.writeString("System", "SaleUpdate", value);
        }

        private String getSaleUpdate()
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            return anIni.readString("System", "SaleUpdate", "");
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

            // BlockLocations = 1
            // BlockProducts = 1
            // BlockPrices = 1
            //BlockSales = 0

            String sBlockLocations = anIni.readString("SYSTEM", "BlockLocations", "");
            BlockLocations = sBlockLocations.Equals("1") || sBlockLocations.ToUpper().Equals("Y");

            String sBlockProducts = anIni.readString("SYSTEM", "BlockProducts", "");
            BlockProducts = sBlockProducts.Equals("1") || sBlockProducts.ToUpper().Equals("Y");

            String sBlockPrices = anIni.readString("SYSTEM", "BlockPrices", "");
            BlockPrices = sBlockPrices.Equals("1") || sBlockPrices.ToUpper().Equals("Y");

            String sBlockSales = anIni.readString("SYSTEM", "BlockSales", "");
            BlockSales = sBlockSales.Equals("1") || sBlockSales.ToUpper().Equals("Y");

            String sBlockPermanentMarkdowns = anIni.readString("SYSTEM", "BlockPermanentMarkdowns", "");
            BlockPermanentMarkdowns = sBlockPermanentMarkdowns.Equals("1") || sBlockPermanentMarkdowns.ToUpper().Equals("Y");

            ResultSet = anIni.readInteger("SYSTEM", "ResultSet", 0);
            Debug = anIni.readInteger("SYSTEM", "Debug", 0);
            LocationModule = anIni.readInteger("SYSTEM", "LocationModule", 1);
            if(LocationModule < 1)
            {
                LocationModule = 1;
            }
            MinSendDate = anIni.readInteger("SYSTEM", "MinSendDate", 20210301);
            PermanentMarkDownsInitialDate = anIni.readInteger("SYSTEM", "PermanentMarkDownsInitialDate", 20210301);
            ActiveMQUrl = anIni.readString("SYSTEM", "ActiveMQUrl", "");
            LocationsQueueName = anIni.readString("Queues", "Locations", "");
            ProductsQueueName = anIni.readString("Queues", "Products", "");
            PricesQueueName = anIni.readString("Queues", "Prices", "");
            SalesQueueName = anIni.readString("Queues", "Sales", "");
            PermanentMarkdownsQueueName = anIni.readString("Queues", "PermanentMarkdowns", "");


            DateFrom = anIni.readInteger("SYSTEM", "SalesDateFrom", 0);
            DateTo = anIni.readInteger("SYSTEM", "SalesDateTo", 0);

            LookupIntervalDays = anIni.readInteger("SYSTEM", "LookupIntervalDays", 7);
        }



    }
}
