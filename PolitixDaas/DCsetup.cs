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
        public String SaleOnLineQueueName { get; private set; }
        public String ShipmentsQueueName { get; set; }
        public String OrdersQueueName { get; set; }
        public String LocationsQueueName { get; private set; }
        public String PricesQueueName { get; private set; }
        public String ProductsQueueName { get; private set; }
        public String SalesQueueName { get; private set; }
        public String PermanentMarkdownsQueueName { get; private set; }
        public String InventoryQueueName { get; set; }
        public String InventoryAdjustmentsQueueName { get; set; }
        public String TransfersFromHOQueueName { get; set; }
        public String TransfersFromBQueueName { get; set; }
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
        public String SqlErsNZDatabase { get; set; }
        public String SqlUser { get; private set; }
        public String SqlPassword { get; private set; }
        public bool SqlOsAuthentication { get; private set; }
        public int Debug { get; private set; }
        public int SalesOnLineNZInitialDate { get; private set; }
        public int SalesOnLineInitialDate { get;private set;}
        public int MinSendDate { get; private set; }
        public int MinSendDateNZ { get; private set; }
        public int PermanentMarkDownsInitialDate { get; private set; }
        public int PermanentMarkDownsNZInitialDate { get; private set; }
        public int InventoryAdjustmentsInitialDate { get; set; }
        public int InventoryAdjustmentsNZInitialDate { get; set; }
        public int TransfersFromHOInitialDate { get; set; }
        public int TransfersFromHONZInitialDate { get; set; }
        public int TransfersFromBranchesInitialDate { get; set; }
        public int TransfersFromBranchesNZInitialDate { get; set; }
        public int OrdersInitialDate { get; set; }
        public int ShipmentsInitialDate { get; set; }
        public int ShipmentsNZInitialDate { get; set; }
        public int ShipmentFromDate { get; set; }
        public int ShipmentToDate { get; set; }
        public int ShipmentNZFromDate { get; set; }
        public int ShipmentNZToDate { get; set; }
        public int MarkdownsFromDate { get; set; }
        public int MarkdownsToDate { get; set; }
        public int MarkdownsNZFromDate { get; set; }
        public int MarkdownsNZToDate { get; set; }
        public int OrdersFromDate { get; set; }
        public int OrdersToDate { get; set; }
        public int SalesOnLineFromDate { get; set; }
        public int SalesOnLineToDate{ get; set; }
        public int SalesOnLineNZFromDate { get; set; }
        public int SalesOnLineNZToDate { get; set; }
        public int TransfersFromHOFromDate { get; set; }
        public int TransfersFromHOToDate { get; set; }
        public int TransfersFromHONZFromDate { get; set; }
        public int TransfersFromHONZToDate { get; set; }
        public int TransfersFromBranchesFromDate { get; set; }
        public int TransfersFromBranchesToDate { get; set; }
        public int TransfersFromBranchesNZFromDate { get; set; }
        public int TransfersFromBranchesNZToDate { get; set; }

        public int InventoryAdjustmentFromDate { get; set; }
        public int InventoryAdjustmentToDate { get; set; }

        public int InventoryAdjustmentNZFromDate { get; set; }
        public int InventoryAdjustmentNZToDate { get; set; }
        public int InventoryBranch { get; set; }
        public int ResultSet { get; private set; }
        public int LocationModule { get; private set; }
        public int DateFrom { get; set; }
        public int DateTo { get; set; }
        public int DateFromNZ { get; set; }
        public int DateToNZ { get; set; }

        public int LookupIntervalDays { get; set; }
        public bool BlockShipments { get; set; }
        public bool BlockShipmentsNZ { get; set; }
        public bool BlockLocations { get; set; }
        public bool BlockProducts { get; set; }
        public bool BlockPrices { get; set; }
        public bool BlockPricesNZ { get; set; }
        public bool BlockSales { get; set; }
        public bool BlockSalesNZ { get; set; }
        public bool BlockSalesOnLine { get; set; }
        public bool BlockSalesOnLineNZ { get; set; }
        public bool BlockPermanentMarkdowns { get; set; }
        public bool BlockPermanentMarkdownsNZ { get; set; }
        public bool BlockInventory { get; set; }
        public bool BlockInventoryAdjustments { get; set; }
        public bool BlockInventoryAdjustmentsNZ { get; set; }
        public bool BlockTransfersFromHO { get; set; }
        public bool BlockTransfersFromHONZ { get; set; }
        public bool BlockTransfersFromBranches { get; set; }
        public bool BlockTransfersFromBranchesNZ { get; set; }
        public bool BlockOrders { get; set; }
        public bool DevMode { get; set; }
        public int WacIntervalDays { get; set; }

        public String ShipmentsNZUpdate
        {
            get => getShipmentsNZUpdate();
            set => setShipmentsNZUpdate(value);
        }

        private String getShipmentsNZUpdate()
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            return anIni.readString("System", "ShipmentsNZUpdate", "");

        }

        private void setShipmentsNZUpdate(String value)
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            anIni.writeString("System", "ShipmentsNZUpdate", value);
        }

        public String ShipmentsUpdate
        {
            get => getShipmentsUpdate();
            set => setShipmentsUpdate(value);
        }

        private String getShipmentsUpdate()
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            return anIni.readString("System", "ShipmentsUpdate", "");

        }

        private void setShipmentsUpdate(String value)
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            anIni.writeString("System", "ShipmentsUpdate", value);
        }

        public String InventoryAdjustmentsUpdate
        {
            get => getInventoryAdjustmentsUpdate();
            set => setInventoryAdjustmentsUpdate(value);
        }

        private String getInventoryAdjustmentsUpdate()
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            return anIni.readString("System", "InventoryAdjustmentsUpdate", "");
        }

        private void setInventoryAdjustmentsUpdate(String value)
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            anIni.writeString("System", "InventoryAdjustmentsUpdate", value);
        }

        public String InventoryAdjustmentsNZUpdate
        {
            get => getInventoryAdjustmentsNZUpdate();
            set => setInventoryAdjustmentsNZUpdate(value);
        }

        private String getInventoryAdjustmentsNZUpdate()
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            return anIni.readString("System", "InventoryAdjustmentsNZUpdate", "");
        }

        private void setInventoryAdjustmentsNZUpdate(String value)
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            anIni.writeString("System", "InventoryAdjustmentsNZUpdate", value);
        }


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

        //RetailTransfersUpdate;

        public String TransfersFromHOUpdate
        {
            get => getTransfersUpdateFromHOUpdate();
            set => setTransfersFromHOUpdate(value);
        }

        private String getTransfersUpdateFromHOUpdate()
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            return anIni.readString("System", "TransfersFromHOUpdate", "");
        }

        private void setTransfersFromHOUpdate(String value)
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            anIni.writeString("System", "TransfersFromHOUpdate", value);
        }


        public String TransfersFromHONZUpdate
        {
            get => getTransfersUpdateFromHONZUpdate();
            set => setTransfersFromHONZUpdate(value);
        }

        private String getTransfersUpdateFromHONZUpdate()
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            return anIni.readString("System", "TransfersFromHONZUpdate", "");
        }


        private void setTransfersFromHONZUpdate(String value)
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            anIni.writeString("System", "TransfersFromHONZUpdate", value);
        }


        public String PermanentMarkdownNZUpdate
        {
            get => getPermanentMarkdownNZUpdate();
            set => setPermanentMarkdownNZUpdate(value);
        }

        private String getPermanentMarkdownNZUpdate()
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            return anIni.readString("System", "PermanentMarkdownNZUpdate", "");
        }

        private void setPermanentMarkdownNZUpdate(String value)
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            anIni.writeString("System", "PermanentMarkdownNZUpdate", value);

        }

        public String TransfersFromBranchesUpdate
        {
            get => getTransfersFromBranchesUpdate();
            set => setTransfersFromBranchesUpdate(value);
        }


        private String getTransfersFromBranchesUpdate()
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            return anIni.readString("System", "TransfersFromBranchesUpdate", "");
        }

        private void setTransfersFromBranchesUpdate(String value)
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            anIni.writeString("System", "TransfersFromBranchesUpdate", value);
        }


        public String TransfersFromBranchesNZUpdate
        {
            get => getTransfersFromBranchesNZUpdate();
            set => setTransfersFromBranchesNZUpdate(value);
        }


        private String getTransfersFromBranchesNZUpdate()
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            return anIni.readString("System", "TransfersFromBranchesNZUpdate", "");
        }

        private void setTransfersFromBranchesNZUpdate(String value)
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            anIni.writeString("System", "TransfersFromBranchesNZUpdate", value);
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

        public String PriceNZUpdate
        {
            get => getPriceNZUpdate();
            set => setPriceNZUpdate(value);
        }

        private String getPriceNZUpdate()
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            return anIni.readString("System", "PriceNZUpdate", "");

        }

        private void setPriceNZUpdate(String value)
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            anIni.writeString("System", "PriceNZUpdate", value);
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


        public String SaleOnLineUpdate
        {
            get => getSaleOnLineUpdate();
            set => setSaleOnLineUpdate(value);
        }

        private void setSaleOnLineUpdate(String value)
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            anIni.writeString("System", "SaleOnLineUpdate", value);
        }

        private String getSaleOnLineUpdate()
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            return anIni.readString("System", "SaleOnLineUpdate", "");
        }

        public String SaleOnLineNZUpdate
        {
            get => getSaleOnLineNZUpdate();
            set => setSaleOnLineNZUpdate(value);
        }

        private void setSaleOnLineNZUpdate(String value)
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            anIni.writeString("System", "SaleOnLineNZUpdate", value);
        }

        private String getSaleOnLineNZUpdate()
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            return anIni.readString("System", "SaleOnLineNZUpdate", "");
        }



        public String SaleUpdateNZ
        {
            get => getSaleUpdateNZ();
            set => setSaleUpdateNZ(value);
        }

        private void setSaleUpdateNZ(String value)
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            anIni.writeString("System", "SaleUpdateNZ", value);
        }

        private String getSaleUpdateNZ()
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            return anIni.readString("System", "SaleUpdateNZ", "");
        }


        public String InventoryUpdate
        {
            get => getInventoryUpdate();
            set => setInventoryUpdate(value);
        }

        private String getInventoryUpdate()
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            return anIni.readString("System", "InventoryUpdate", "");
        }

        private void setInventoryUpdate(String value)
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            anIni.writeString("System", "InventoryUpdate", value);
        }

        public String OrdersUpdate
        {
            get => getOrdersUpdate();
            set => setOrdersUpdate(value);
        }

        private String getOrdersUpdate()
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            return anIni.readString("System", "OrdersUpdate", "");

        }

        private void setOrdersUpdate(String value)
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            anIni.writeString("System", "OrdersUpdate", value);

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
            SqlErsNZDatabase = anIni.readString("SQL", "ersnzdatabase", "");

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

            String sBlockPricesNZ = anIni.readString("SYSTEM", "BlockPricesNZ", "");
            BlockPricesNZ = sBlockPricesNZ.Equals("1") || sBlockPricesNZ.ToUpper().Equals("Y");

            String sBlockSales = anIni.readString("SYSTEM", "BlockSales", "");
            BlockSales = sBlockSales.Equals("1") || sBlockSales.ToUpper().Equals("Y");


            String sBlockSalesNZ = anIni.readString("SYSTEM", "BlockSalesNZ", "");
            BlockSalesNZ = sBlockSalesNZ.Equals("1") || sBlockSalesNZ.ToUpper().Equals("Y");


            String sBlockPermanentMarkdowns = anIni.readString("SYSTEM", "BlockPermanentMarkdowns", "");
            BlockPermanentMarkdowns = sBlockPermanentMarkdowns.Equals("1") || sBlockPermanentMarkdowns.ToUpper().Equals("Y");

            String sBlockPermanentMarkdownsNZ = anIni.readString("SYSTEM", "BlockPermanentMarkdownsNZ", "");
            BlockPermanentMarkdownsNZ = sBlockPermanentMarkdownsNZ.Equals("1") || sBlockPermanentMarkdownsNZ.ToUpper().Equals("Y");


            String sInventory = anIni.readString("SYSTEM", "BlockInventory", "");
            BlockInventory = sInventory.Equals("1") || sInventory.ToUpper().Equals("Y");

            String sInventoryAdjustments = anIni.readString("SYSTEM", "BlockInventoryAdjustments", "");
            BlockInventoryAdjustments = sInventoryAdjustments.Equals("1") || sInventoryAdjustments.ToUpper().Equals("Y");

            String sInventoryAdjustmentsNZ = anIni.readString("SYSTEM", "BlockInventoryAdjustmentsNZ", "");
            BlockInventoryAdjustmentsNZ = sInventoryAdjustmentsNZ.Equals("1") || sInventoryAdjustmentsNZ.ToUpper().Equals("Y");


            //BlockRetailTransfers
            String sBlockTransfersFromHO = anIni.readString("SYSTEM", "BlockTransfersFromHO", "");
            BlockTransfersFromHO = sBlockTransfersFromHO.Equals("1") || sBlockTransfersFromHO.ToUpper().Equals("Y");

            String sBlockTransfersFromHONZ = anIni.readString("SYSTEM", "BlockTransfersFromHONZ", "");
            BlockTransfersFromHONZ = sBlockTransfersFromHONZ.Equals("1") || sBlockTransfersFromHONZ.ToUpper().Equals("Y");


            //BlockTransfersFromBranches
            String sBlockTransfersFromBranches = anIni.readString("SYSTEM", "BlockTransfersFromBranches", "");
            BlockTransfersFromBranches = sBlockTransfersFromBranches.Equals("1") || sBlockTransfersFromBranches.ToUpper().Equals("Y");

            String sBlockTransfersFromBranchesNZ = anIni.readString("SYSTEM", "BlockTransfersFromBranchesNZ", "");
            BlockTransfersFromBranchesNZ = sBlockTransfersFromBranchesNZ.Equals("1") || sBlockTransfersFromBranchesNZ.ToUpper().Equals("Y");
            //BlockOrders
            String sBlockOrders = anIni.readString("SYSTEM", "BlockOrders", "");
            BlockOrders = sBlockOrders.Equals("1") || sBlockOrders.ToUpper().Equals("Y");

            String sBlockShipments = anIni.readString("SYSTEM", "BlockShipments", "");
            BlockShipments = sBlockShipments.Equals("1") || sBlockShipments.ToUpper().Equals("Y");

            String sBlockShipmentsNZ = anIni.readString("SYSTEM", "BlockShipmentsNZ", "");
            BlockShipmentsNZ = sBlockShipmentsNZ.Equals("1") || sBlockShipmentsNZ.ToUpper().Equals("Y");

            String sDevMode = anIni.readString("SYSTEM", "DevMode", "");
            DevMode = sDevMode.Equals("1") || sDevMode.ToUpper().Equals("Y");

            ResultSet = anIni.readInteger("SYSTEM", "ResultSet", 0);
            Debug = anIni.readInteger("SYSTEM", "Debug", 0);
            LocationModule = anIni.readInteger("SYSTEM", "LocationModule", 1);
            if(LocationModule < 1)
            {
                LocationModule = 1;
            }
            MinSendDate = anIni.readInteger("SYSTEM", "MinSendDate", 20210301);
            MinSendDateNZ = anIni.readInteger("SYSTEM", "MinSendDateNZ", 20210301);
            PermanentMarkDownsInitialDate = anIni.readInteger("SYSTEM", "PermanentMarkDownsInitialDate", 20210301);
            PermanentMarkDownsNZInitialDate = anIni.readInteger("SYSTEM", "PermanentMarkDownsNZInitialDate", 20210301);

            InventoryAdjustmentsInitialDate = anIni.readInteger("SYSTEM", "InventoryAdjustmentsInitialDate", 20210301);
            InventoryAdjustmentsNZInitialDate = anIni.readInteger("SYSTEM", "InventoryAdjustmentsNZInitialDate", 20210301);

            TransfersFromHOInitialDate = anIni.readInteger("SYSTEM", "TransfersFromHOInitialDate", 20210301);
            TransfersFromHONZInitialDate = anIni.readInteger("SYSTEM", "TransfersFromHONZInitialDate", 20210301);

            TransfersFromBranchesInitialDate = anIni.readInteger("SYSTEM", "TransfersFromBranchesInitialDate", 20210301);
            TransfersFromBranchesNZInitialDate =anIni.readInteger("SYSTEM", "TransfersFromBranchesNZInitialDate", 20210301);

            OrdersInitialDate = anIni.readInteger("SYSTEM", "OrdersInitialDate", 20210301);
            ShipmentsInitialDate = anIni.readInteger("SYSTEM", "ShipmentsInitialDate", 20210301);
            ShipmentsNZInitialDate = anIni.readInteger("SYSTEM", "ShipmentsNZInitialDate", 20210301);


            ActiveMQUrl = anIni.readString("SYSTEM", "ActiveMQUrl", "");
            LocationsQueueName = anIni.readString("Queues", "Locations", "");
            OrdersQueueName = anIni.readString("Queues", "Orders", "");
            ProductsQueueName = anIni.readString("Queues", "Products", "");
            PricesQueueName = anIni.readString("Queues", "Prices", "");
            SalesQueueName = anIni.readString("Queues", "Sales", "");
            PermanentMarkdownsQueueName = anIni.readString("Queues", "PermanentMarkdowns", "");
            InventoryQueueName = anIni.readString("Queues", "Inventory", "");
            InventoryAdjustmentsQueueName = anIni.readString("Queues", "InventoryAdjustments", "");
            TransfersFromHOQueueName = anIni.readString("Queues", "TransfersFromHO", "");
            TransfersFromBQueueName = anIni.readString("Queues", "TransfersFromBranch", "");
            ShipmentsQueueName = anIni.readString("Queues", "Shipments", "");
            SaleOnLineQueueName = anIni.readString("Queues", "SaleOnLine", "");
            DateFrom = anIni.readInteger("SYSTEM", "SalesDateFrom", 0);
            DateTo = anIni.readInteger("SYSTEM", "SalesDateTo", 0);
            DateFromNZ = anIni.readInteger("SYSTEM", "SalesNZDateFrom", 0);
            DateToNZ = anIni.readInteger("SYSTEM", "SalesNZDateTo", 0);

            ShipmentFromDate = anIni.readInteger("SYSTEM", "ShipmentsFromDate", 0);
            ShipmentToDate = anIni.readInteger("SYSTEM", "ShipmentsToDate", 0);
            ShipmentNZFromDate = anIni.readInteger("SYSTEM", "ShipmentsNZFromDate", 0);
            ShipmentNZToDate = anIni.readInteger("SYSTEM", "ShipmentsNZToDate", 0);

            LookupIntervalDays = anIni.readInteger("SYSTEM", "LookupIntervalDays", 7);
            WacIntervalDays = anIni.readInteger("SYSTEM", "WacIntervalDays", 7);
            MarkdownsFromDate = anIni.readInteger("SYSTEM", "MarkdownsFromDate", 0);
            MarkdownsToDate = anIni.readInteger("SYSTEM", "MarkdownsToDate", 0);
            MarkdownsNZFromDate = anIni.readInteger("SYSTEM", "MarkdownsNZFromDate", 0);
            MarkdownsNZToDate = anIni.readInteger("SYSTEM", "MarkdownsNZToDate", 0);

            InventoryAdjustmentFromDate = anIni.readInteger("SYSTEM", "InventoryAdjustmentFromDate", 0);
            InventoryAdjustmentToDate = anIni.readInteger("SYSTEM", "InventoryAdjustmentToDate", 0);

            InventoryAdjustmentNZFromDate = anIni.readInteger("SYSTEM", "InventoryAdjustmentNZFromDate", 0);
            InventoryBranch = anIni.readInteger("SYSTEM", "InventoryBranch", 0);
            InventoryAdjustmentNZToDate = anIni.readInteger("SYSTEM", "InventoryAdjustmentNZToDate", 0);
            OrdersFromDate = anIni.readInteger("SYSTEM", "OrdersFromDate", 0);
            OrdersToDate = anIni.readInteger("SYSTEM", "OrdersToDate", 0);
            TransfersFromHOFromDate = anIni.readInteger("SYSTEM", "TransfersFromHOFromDate", 0);
            TransfersFromHOToDate = anIni.readInteger("SYSTEM", "TransfersFromHOToDate", 0);
            TransfersFromHONZFromDate = anIni.readInteger("SYSTEM", "TransfersFromHONZFromDate", 0);
            TransfersFromHONZToDate = anIni.readInteger("SYSTEM", "TransfersFromHONZToDate", 0);

            TransfersFromBranchesFromDate = anIni.readInteger("SYSTEM", "TransfersFromBranchesFromDate", 0);
            TransfersFromBranchesToDate = anIni.readInteger("SYSTEM", "TransfersFromBranchesToDate", 0);

            TransfersFromBranchesNZFromDate = anIni.readInteger("SYSTEM", "TransfersFromBranchesNZFromDate", 0);
            TransfersFromBranchesNZToDate = anIni.readInteger("SYSTEM", "TransfersFromBranchesNZToDate", 0);

            SalesOnLineFromDate = anIni.readInteger("SYSTEM", "SalesOnLineFromDate", 0);
            SalesOnLineToDate = anIni.readInteger("SYSTEM", "SalesOnLineToDate", 0);
            SalesOnLineNZFromDate = anIni.readInteger("SYSTEM", "SalesOnLineNZFromDate", 0);
            SalesOnLineNZToDate = anIni.readInteger("SYSTEM", "SalesOnLineNZToDate", 0);
        }

        public void resetOnLineSalesDateRange()
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            anIni.writeString("SYSTEM", "SalesOnLineFromDate", "0");
            anIni.writeString("SYSTEM", "SalesOnLineToDate", "0");

        }


        public void resetOnLineSalesNZDateRange()
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            anIni.writeString("SYSTEM", "SalesOnLineNZFromDate", "0");
            anIni.writeString("SYSTEM", "SalesOnLineNZToDate", "0");

        }


        public void resetInventoryAdjustmentDateRange()
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            anIni.writeString("SYSTEM", "InventoryAdjustmentFromDate", "0");
            anIni.writeString("SYSTEM", "InventoryAdjustmentToDate", "0");
        }

        public void resetInventoryAdjustmentNZDateRange()
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            anIni.writeString("SYSTEM", "InventoryAdjustmentNZFromDate", "0");
            anIni.writeString("SYSTEM", "InventoryAdjustmentNZToDate", "0");
        }


        public void resetSalesDateRange()
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            anIni.writeString("SYSTEM", "SalesDateFrom", "0");
            anIni.writeString("SYSTEM", "SalesDateTo", "0");

        }

        public void resetSalesNZDateRange()
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            anIni.writeString("SYSTEM", "SalesNZDateFrom", "0");
            anIni.writeString("SYSTEM", "SalesNZDateTo", "0");

        }


        public void resetShipmentsDateRange()
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            anIni.writeString("SYSTEM", "ShipmentsFromDate", "0");
            anIni.writeString("SYSTEM", "ShipmentsToDate", "0");
        }

        public void resetShipmentsNZDateRange()
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            anIni.writeString("SYSTEM", "ShipmentsNZFromDate", "0");
            anIni.writeString("SYSTEM", "ShipmentsNZToDate", "0");
        }


        public void resetMarkdownsDateRange()
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            anIni.writeString("SYSTEM", "MarkdownsFromDate", "0");
            anIni.writeString("SYSTEM", "MarkdownsToDate", "0");
        }
        public void resetMarkdownsNZDateRange()
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            anIni.writeString("SYSTEM", "MarkdownsNZFromDate", "0");
            anIni.writeString("SYSTEM", "MarkdownsNZToDate", "0");
        }

        public void resetOrdersDateRange()
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            anIni.writeString("SYSTEM", "OrdersFromDate", "0");
            anIni.writeString("SYSTEM", "OrdersToDate", "0");
        }

        public void resetTransfersFromHoRange()
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            anIni.writeString("SYSTEM", "TransfersFromHOFromDate", "0");
            anIni.writeString("SYSTEM", "TransfersFromHOToDate", "0");
        }

        public void resetTransfersFromHoNZRange()
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            anIni.writeString("SYSTEM", "TransfersFromHONZFromDate", "0");
            anIni.writeString("SYSTEM", "TransfersFromHONZToDate", "0");
        }

        public void resetTransfersFromBranchesRange()
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            anIni.writeString("SYSTEM", "TransfersFromBranchesFromDate", "0");
            anIni.writeString("SYSTEM", "TransfersFromBranchesToDate", "0");
        }

        public void resetTransfersFromBranchesNZRange()
        {
            String apath = Path.ChangeExtension(Assembly.GetExecutingAssembly().Location, ".ini");
            RdeIniFile.Rde_IniFile anIni = new RdeIniFile.Rde_IniFile(apath);
            anIni.writeString("SYSTEM", "TransfersFromBranchesNZFromDate", "0");
            anIni.writeString("SYSTEM", "TransfersFromBranchesNZToDate", "0");
        }


    }
}
