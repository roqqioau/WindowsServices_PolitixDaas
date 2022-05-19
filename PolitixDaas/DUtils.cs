using Apache.NMS;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Mail;
using System.Text;
using System.Threading.Tasks;

namespace PolitixDaas
{
    public class DUtils
    {
        public DCsetup dcSetup;

        private Dictionary<String, String> lstDiscount = new Dictionary<string, string>();
        private Dictionary<String, String> lstDiscountNZ = new Dictionary<string, string>();

        private String DaasExportTable { get;  set; }

        private int ACounter { get; set; } = 0;

        private String localCurrency;

        public DUtils()
        {
            dcSetup = new DCsetup();
            DaasExportTable = "DAAS_EXPORT";
            if(dcSetup.DevMode)
            {
                DaasExportTable = "DAAS_EXPORT_DEV";
            }
        }

        private SqlConnection openSQLConnection()
        {
            SqlConnection sqlConnection = null;

            String cnString = "Data Source=" + dcSetup.SqlServer + ";" +
                 "Initial Catalog=" + dcSetup.SqlDatabase + ";" +
                 "User id=" + dcSetup.SqlUser + ";" +
                 "Password=" + dcSetup.SqlPassword + ";Connect Timeout=30;MultipleActiveResultSets = true;";

            if (dcSetup.SqlOsAuthentication)
            {
                cnString = cnString + "Integrated Security=SSPI;";

            }

            try
            {
                sqlConnection = new SqlConnection(cnString);
                sqlConnection.Open();
                return sqlConnection;
            }
            catch (Exception e)
            {
                Logging.WriteErrorLog("Could not open database connection - " + e.Message);
                return null;
            }

        }

        private String getdiscountReason(int amandant, String kasInfo)
        {
            if (kasInfo == null || kasInfo.Trim().Equals(""))
            {
                return "";
            }
            String areason = "";

            String afield = "";

            String[] fields = kasInfo.Trim().Split(' ');
            for (int i = 0; i < fields.Length; i++)
            {
                afield = fields[i].Trim();
                if (afield.Equals(""))
                {
                    continue;
                }

                String akey = amandant + "~" + afield;
                try
                {
                    areason = lstDiscount[akey];
                }
                catch { }
                break;
            }

            return afield + "-" + areason;
        }

        private double getDiscountAmount(String kasInfo)
        {
            double ares = 0;
            String sAmount = kasInfo.Substring(10).Trim();
            try
            {
                ares = -1 * Convert.ToDouble(sAmount) / 100;
            }
            catch { }


            return ares;
        }

        public void populateDiscount(int amandant, String kasInfo, DiscountLine discountLine)
        {
            if (kasInfo == null || kasInfo.Trim().Equals(""))
            {
                return;
            }
            String areason = "";

            String refNo = "";

            String[] fields = kasInfo.Trim().Split(' ');
            int acounter = 0;
            double amount = 0;
            for (int i = 0; i < fields.Length; i++)
            {
                String afield = fields[i].Trim();
                if (afield.Equals(""))
                {
                    continue;
                }
                if (acounter == 0)
                {
                    String akey = amandant + "~" + afield;
                    refNo = afield;
                    try
                    {
                        areason = lstDiscount[akey];
                    }
                    catch { }
                    acounter++;
                    continue;
                }
                if (acounter == 1)
                {
                    amount = Logging.strToDoubleDef(afield, 0) / 100;
                    break;
                }


            }

            discountLine.DiscountReasonId = refNo;
            discountLine.DiscountReason = areason;
            discountLine.Amount = amount;
        }


        public void populateDiscountNZ(String kasInfo, DiscountLine discountLine)
        {
            if (kasInfo == null || kasInfo.Trim().Equals(""))
            {
                return;
            }
            String areason = "";

            String refNo = "";

            String[] fields = kasInfo.Trim().Split(' ');
            int acounter = 0;
            double amount = 0;
            for (int i = 0; i < fields.Length; i++)
            {
                String afield = fields[i].Trim();
                if (afield.Equals(""))
                {
                    continue;
                }
                if (acounter == 0)
                {
                    String akey = afield;
                    refNo = afield;
                    try
                    {
                        areason = lstDiscountNZ[akey];
                    }
                    catch { }
                    acounter++;
                    continue;
                }
                if (acounter == 1)
                {
                    amount = Logging.strToDoubleDef(afield, 0) / 100;
                    break;
                }


            }

            discountLine.DiscountReasonId = refNo;
            discountLine.DiscountReason = areason;
            discountLine.Amount = amount;
        }




        private void SendNewMessageQueue(string text, string queueName)
        {
            Console.WriteLine($"Adding message to queue topic: {queueName}");

            string brokerUri = $"activemq:tcp://" + dcSetup.ActiveMQUrl + "?useInactivityMonitor=false&wireFormat.maxInactivityDuration=0";
            NMSConnectionFactory factory = new NMSConnectionFactory(brokerUri);

            using (IConnection connection = factory.CreateConnection())
            {
                connection.Start();

                using (ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge))
                using (IDestination dest = session.GetQueue(queueName))
                using (IMessageProducer producer = session.CreateProducer(dest))
                {
                    producer.DeliveryMode = MsgDeliveryMode.NonPersistent;

                    producer.Send(session.CreateTextMessage(text));
                }
            }
        }

        private void updateDaasExport(String daasK1, String daasK2, String daasK3, String daasK4, String daasK5, String setName, String amd5,
            SqlConnection ersConnection)
        {
            String anSql = " select DAAS_MD5 from " + DaasExportTable + "  where DAAS_KEY1 = @DAAS_KEY1 and DAAS_KEY1 = @DAAS_KEY1 and DAAS_KEY2 = @DAAS_KEY2 and DAAS_KEY3 = @DAAS_KEY3 " +
                "  and DAAS_KEY4 = @DAAS_KEY4  and DAAS_KEY5 = @DAAS_KEY5 and DAAS_SET_NAME = @DAAS_SET_NAME";
            bool found = false;

            using (SqlCommand cmd = new SqlCommand(anSql, ersConnection))
            {
                cmd.Parameters.AddWithValue("@DAAS_KEY1", daasK1);
                cmd.Parameters.AddWithValue("@DAAS_KEY2", daasK2);
                cmd.Parameters.AddWithValue("@DAAS_KEY3", daasK3);
                cmd.Parameters.AddWithValue("@DAAS_KEY4", daasK4);
                cmd.Parameters.AddWithValue("@DAAS_KEY5", daasK5);
                cmd.Parameters.AddWithValue("@DAAS_SET_NAME", setName);
                using (SqlDataReader areader = cmd.ExecuteReader())
                {
                    found = areader.Read();
                }
            }

            DateTime anow = DateTime.Now;
            String snow = anow.ToString("yyyyMMddhhmmss");
            Int64 inow = Logging.strToInt64Def(snow, 0);


            String updateSql = "insert into " + DaasExportTable + "  (DAAS_KEY1, DAAS_KEY2, DAAS_KEY3, DAAS_KEY4, DAAS_KEY5, DAAS_SET_NAME, DAAS_MD5, DAAS_UPDATE_TIME) values " +
                "(@DAAS_KEY1, @DAAS_KEY2, @DAAS_KEY3, @DAAS_KEY4, @DAAS_KEY5, @DAAS_SET_NAME, @DAAS_MD5, @DAAS_UPDATE_TIME)";

            if (found)
            {
                updateSql = "update " + DaasExportTable + "  set DAAS_MD5 = @DAAS_MD5, DAAS_UPDATE_TIME = @DAAS_UPDATE_TIME where " +
                    " DAAS_KEY1 = @DAAS_KEY1 and DAAS_KEY1 = @DAAS_KEY1 and DAAS_KEY2 = @DAAS_KEY2 and DAAS_KEY3 = @DAAS_KEY3 and DAAS_KEY4 = @DAAS_KEY4  " +
                    " and DAAS_KEY5 = @DAAS_KEY5 and DAAS_SET_NAME = @DAAS_SET_NAME";
            }

            using (SqlCommand cmd = new SqlCommand(updateSql, ersConnection))
            {
                cmd.Parameters.AddWithValue("@DAAS_KEY1", daasK1);
                cmd.Parameters.AddWithValue("@DAAS_KEY2", daasK2);
                cmd.Parameters.AddWithValue("@DAAS_KEY3", daasK3);
                cmd.Parameters.AddWithValue("@DAAS_KEY4", daasK4);
                cmd.Parameters.AddWithValue("@DAAS_KEY5", daasK5);
                cmd.Parameters.AddWithValue("@DAAS_SET_NAME", setName);
                cmd.Parameters.AddWithValue("@DAAS_MD5", amd5);
                cmd.Parameters.AddWithValue("@DAAS_UPDATE_TIME", inow);
                cmd.ExecuteNonQuery();

            }

        }

        private String getMd5(String daasK1, String daasK2, String daasK3, String daasK4, String daasK5, String setName, Int64 aDate,
            SqlConnection ersConnection)
        {
            String res = "";
            String anSql = " select DAAS_MD5 from " + DaasExportTable + "  WITH(NOLOCK)  where DAAS_KEY1 = @DAAS_KEY1 and DAAS_KEY1 = @DAAS_KEY1 and DAAS_KEY2 = @DAAS_KEY2 and DAAS_KEY3 = @DAAS_KEY3 " +
                 "  and DAAS_KEY4 = @DAAS_KEY4  and DAAS_KEY5 = @DAAS_KEY5 and DAAS_SET_NAME = @DAAS_SET_NAME and DAAS_UPDATE_TIME <= @DAAS_UPDATE_TIME";
            using (SqlCommand cmd = new SqlCommand(anSql, ersConnection))
            {
                cmd.Parameters.AddWithValue("@DAAS_KEY1", daasK1);
                cmd.Parameters.AddWithValue("@DAAS_KEY2", daasK2);
                cmd.Parameters.AddWithValue("@DAAS_KEY3", daasK3);
                cmd.Parameters.AddWithValue("@DAAS_KEY4", daasK4);
                cmd.Parameters.AddWithValue("@DAAS_KEY5", daasK5);
                cmd.Parameters.AddWithValue("@DAAS_SET_NAME", setName);
                cmd.Parameters.AddWithValue("@DAAS_UPDATE_TIME", aDate);
                using (SqlDataReader areader = cmd.ExecuteReader())
                {
                    if (areader.Read())
                    {
                        res = areader.GetString(0);
                    }
                }

            }


            return res;

        }


        private SqlConnection openERSSQLConnection()
        {
            SqlConnection sqlConnection = null;

            String cnString = "Data Source=" + dcSetup.SqlServer + ";" +
                 "Initial Catalog=" + dcSetup.SqlErsDatabase + ";" +
                 "User id=" + dcSetup.SqlUser + ";" +
                 "Password=" + dcSetup.SqlPassword + ";Connect Timeout=30;MultipleActiveResultSets = true;";

            if (dcSetup.SqlOsAuthentication)
            {
                cnString = cnString + "Integrated Security=SSPI;";

            }

            try
            {
                sqlConnection = new SqlConnection(cnString);
                sqlConnection.Open();
                return sqlConnection;
            }
            catch (Exception e)
            {
                Logging.WriteErrorLog("Could not open database connection - " + e.Message);
                return null;
            }

        }

        private SqlConnection openERSNZSQLConnection()
        {
            SqlConnection sqlConnection = null;

            String cnString = "Data Source=" + dcSetup.SqlServer + ";" +
                 "Initial Catalog=" + dcSetup.SqlErsNZDatabase + ";" +
                 "User id=" + dcSetup.SqlUser + ";" +
                 "Password=" + dcSetup.SqlPassword + ";Connect Timeout=30;MultipleActiveResultSets = true;";

            if (dcSetup.SqlOsAuthentication)
            {
                cnString = cnString + "Integrated Security=SSPI;";

            }

            try
            {
                sqlConnection = new SqlConnection(cnString);
                sqlConnection.Open();
                return sqlConnection;
            }
            catch (Exception e)
            {
                Logging.WriteErrorLog("Could not open database connection - " + e.Message);
                return null;
            }

        }


        private String getCustNo(String kasInfo)
        {
            String custNo = "0";
            bool is01 = false;

            int apos = kasInfo.IndexOf(" 03");
            if(apos < 0)
            {
                apos = kasInfo.IndexOf(" 13");
            }


            if(apos < 0)
            {
                apos = kasInfo.IndexOf(" 01 ");
                is01 = true;
            }
            if (apos < 0)
            {
                apos = kasInfo.IndexOf(" 11 ");
                is01 = true;
            }
            if (apos < 0)
            {
                apos = kasInfo.IndexOf(" 21 ");
                is01 = true;
            }
            if (apos < 0)
            {
                apos = kasInfo.IndexOf(" 31 ");
                is01 = true;
            }
            if (apos < 0)
                return custNo;

            String aline;
            try
            {
                aline = kasInfo.Substring(apos).Trim();
                aline = aline.Substring(2).Trim();
                int blankPos = aline.IndexOf(" ");
                if (blankPos > 0)
                {
                    aline = aline.Substring(0, blankPos);
                }
                if (aline.Equals(""))
                    return "0";
                Int64 myInt;
                bool isNumerical = Int64.TryParse(aline, out myInt);
                if (isNumerical)
                {
                    if(is01)
                    {
                        String aress = "000000000" + myInt.ToString();
                        aress = "1" + aress.Substring(aress.Length - 9); 
                        myInt = Convert.ToInt64(aress);
                    }
                    return myInt.ToString();
                }

                char[] alch = aline.ToCharArray();

                int icustNo = 0;

                for (int i = 0; i < alch.Length; i++)
                {
                    char ac = alch[i];
                    if (ac >= 'A' && ac <= 'Z')
                    {
                        if (i == 0)
                        {
                            icustNo = icustNo * 36 + ((int)ac) - 65;
                        }
                        else
                        {
                            icustNo = icustNo * 36 + ((int)ac) - 55;
                        }
                    }
                    else if (ac >= '0' && ac <= '9')
                    {
                        icustNo = icustNo * 36 + ((int)ac) - 48;
                    }
                }

                return icustNo.ToString();

            }
            catch (Exception e)
            {
                return "0";
            }


        }

        private String getCustNoFromTransactionNZ(SqlConnection ersConnection, int kasDatum, int kasFiliale, int kasKasse, int kasBonnr)
        {

            //           String asql = "select  KAS_INFO from KASSTRNS  WITH (NOLOCK) where  KAS_INFO like '% 03 %' and KAS_SATZART = 16 " +
            String asql = "select  KAS_INFO from V_KASSTRNS  WITH (NOLOCK) where     KAS_SATZART = 16 " +
                " and KAS_DATUM = " + kasDatum.ToString() + " and KAS_FILIALE = " + kasFiliale.ToString() +
                " and KAS_KASSE = " + kasKasse.ToString() + " and KAS_BONNR = " + kasBonnr.ToString() ;

            String kasInfo = "";
            using (SqlCommand acommand = new SqlCommand(asql, ersConnection))
            {
                Object anObj = acommand.ExecuteScalar();
                if (anObj != null)
                {
                    kasInfo = anObj.ToString();
                }
            }
            if (!kasInfo.Equals(""))
            {
                return getCustNo(kasInfo);
            }
            asql = "select  KAS_INFO from V_KASIDLTA  WITH (NOLOCK) where  KAS_SATZART = 16 " +
                " and KAS_DATUM = " + kasDatum.ToString() + " and KAS_FILIALE = " + kasFiliale.ToString() +
                " and KAS_KASSE = " + kasKasse.ToString() + " and KAS_BONNR = " + kasBonnr.ToString() ;

            using (SqlCommand acommand = new SqlCommand(asql, ersConnection))
            {
                Object anObj = acommand.ExecuteScalar();
                if (anObj != null)
                {
                    kasInfo = anObj.ToString();
                }
            }
            if (!kasInfo.Equals(""))
            {
                return getCustNo(kasInfo);
            }

            asql = "select  KAS_INFO from V_KASSE  WITH (NOLOCK) where  KAS_SATZART = 16 " +
                " and KAS_DATUM = " + kasDatum.ToString() + " and KAS_FILIALE = " + kasFiliale.ToString() +
                " and KAS_KASSE = " + kasKasse.ToString() + " and KAS_BONNR = " + kasBonnr.ToString() ;

            using (SqlCommand acommand = new SqlCommand(asql, ersConnection))
            {
                Object anObj = acommand.ExecuteScalar();
                if (anObj != null)
                {
                    kasInfo = anObj.ToString();
                }
            }
            if (!kasInfo.Equals(""))
            {
                return getCustNo(kasInfo);
            }


            return "0";
        }


        private String getCustNoFromTransaction(SqlConnection ersConnection, int kasDatum, int kasFiliale, int kasKasse, int kasBonnr, int kasMandant)
        {

 //           String asql = "select  KAS_INFO from KASSTRNS  WITH (NOLOCK) where  KAS_INFO like '% 03 %' and KAS_SATZART = 16 " +
            String asql = "select  KAS_INFO from V_KASSTRNS  WITH (NOLOCK) where     KAS_SATZART = 16 " +
                " and KAS_DATUM = " + kasDatum.ToString() + " and KAS_FILIALE = " + kasFiliale.ToString() +
                " and KAS_KASSE = " + kasKasse.ToString() + " and KAS_BONNR = " + kasBonnr.ToString() + " and KAS_MANDANT = " + kasMandant.ToString();

            String kasInfo = "";
            using (SqlCommand acommand = new SqlCommand(asql, ersConnection))
            {
                Object anObj = acommand.ExecuteScalar();
                if (anObj != null)
                {
                    kasInfo = anObj.ToString();
                }
            }
            if (!kasInfo.Equals(""))
            {
                return getCustNo(kasInfo);
            }
            asql = "select  KAS_INFO from V_KASIDLTA  WITH (NOLOCK) where  KAS_SATZART = 16 " +
                " and KAS_DATUM = " + kasDatum.ToString() + " and KAS_FILIALE = " + kasFiliale.ToString() +
                " and KAS_KASSE = " + kasKasse.ToString() + " and KAS_BONNR = " + kasBonnr.ToString() + " and KAS_MANDANT = " + kasMandant.ToString();

            using (SqlCommand acommand = new SqlCommand(asql, ersConnection))
            {
                Object anObj = acommand.ExecuteScalar();
                if (anObj != null)
                {
                    kasInfo = anObj.ToString();
                }
            }
            if (!kasInfo.Equals(""))
            {
                return getCustNo(kasInfo);
            }

            asql = "select  KAS_INFO from V_KASSE  WITH (NOLOCK) where  KAS_SATZART = 16 " +
                " and KAS_DATUM = " + kasDatum.ToString() + " and KAS_FILIALE = " + kasFiliale.ToString() +
                " and KAS_KASSE = " + kasKasse.ToString() + " and KAS_BONNR = " + kasBonnr.ToString() + " and KAS_MANDANT = " + kasMandant.ToString();

            using (SqlCommand acommand = new SqlCommand(asql, ersConnection))
            {
                Object anObj = acommand.ExecuteScalar();
                if (anObj != null)
                {
                    kasInfo = anObj.ToString();
                }
            }
            if (!kasInfo.Equals(""))
            {
                return getCustNo(kasInfo);
            }


            return "0";
        }

        public void getShipments(SqlConnection ersConnection)
        {
            Logging.WriteLog("Starting getShipments");

            String lastUpdate = dcSetup.ShipmentsUpdate;
            if (lastUpdate.Equals("0") || lastUpdate.Equals(""))
            {
                String iiSql = "delete from " + DaasExportTable + " where DAAS_SET_NAME = 'SHIPMENT' ";
                using (SqlCommand cmd = new SqlCommand(iiSql, ersConnection))
                {
                    cmd.ExecuteNonQuery();
                }
            }

            String dateFrom = dcSetup.ShipmentFromDate.ToString();
            String dateTo = dcSetup.ShipmentToDate.ToString();

            if (dcSetup.ShipmentFromDate > 0 || dcSetup.ShipmentToDate > 0)
            {
                String anSql = "delete from " + DaasExportTable + " where DAAS_SET_NAME = 'SHIPMENT' and DAAS_KEY1 >= @mindate and DAAS_KEY1 <= @maxDate  ";
                using (SqlCommand cmd = new SqlCommand(anSql, ersConnection))
                {
                    cmd.Parameters.AddWithValue("@mindate", dcSetup.ShipmentFromDate.ToString());
                    cmd.Parameters.AddWithValue("@maxDate", dcSetup.ShipmentToDate.ToString());
                    cmd.ExecuteNonQuery();
                }

            }

            if (lastUpdate.Length >= 8)
            {
                lastUpdate = lastUpdate.Substring(0, 8);
            }
            int iLastUpdate = Logging.strToIntDef(lastUpdate, 0);
            int initialdate = dcSetup.ShipmentsInitialDate;

            String top10 = " ";
            if (dcSetup.ResultSet > 0)
            {
                top10 = " top " + dcSetup.ResultSet.ToString();
            }

            String headSql = "select " + top10 + " WEH_ORIG, WEH_EINGANG, WEH_DATUM, WEH_TEXT, WEH_WAEHRUNG from WE_HEADR " +
                " where ((WEH_DATUM >=  " + initialdate + " and WEH_DATUM >= " + iLastUpdate + ") or (WEH_DATUM >= " + dcSetup.ShipmentFromDate + " and WEH_DATUM <= " + dcSetup.ShipmentToDate + ")) " +
                " and WEH_MANDANT = 1 " + 
                " order by WEH_DATUM ";

            Logging.WriteDebug(headSql, dcSetup.Debug);

            using (SqlCommand cmd = new SqlCommand(headSql, ersConnection))
            {
                String detSql = "SELECT WEZ_ZEILE, WEZ_REFNUMMER, WEZ_BESTELLNUMMER [ORDER_NUMNER], WEZ_BESTELLZEILE[ORDER_POSITION], WEZ_ANZBESTELLT [ORDERED_QTY], " +
                    " WEZ_ANZGELIEFERT[DELIVERY_NOTE_QTY], WEZ_ANZBERECHNET[INVOICED_QTY],  WEZ_EKWPREIS[PP_PRICE], WEZ_ANZGEZAEHLT[QTY_DELIVERED] FROM WE_ZEILE " +
                    " WHERE WEZ_MANDANT = 1 AND WEZ_ORIG = @WEZ_ORIG AND WEZ_EINGANG = @WEZ_EINGANG ";

                using(SqlDataReader areader = cmd.ExecuteReader())
                {
                    while(areader.Read())
                    {
                        ShipmentJson shipmentJson = new ShipmentJson();
                        shipmentJson.Shipment = new GoodsIn();
                        shipmentJson.Shipment.Id = areader["WEH_ORIG"].ToString() + "-" + areader["WEH_EINGANG"].ToString();
                        shipmentJson.Shipment.Lines = new List<GoodsInLine>();
                        shipmentJson.Shipment.ShipmentDate = Logging.strToIntDef(areader["WEH_DATUM"].ToString(), 0);
                        shipmentJson.Shipment.Text = areader["WEH_TEXT"].ToString();
                        shipmentJson.Shipment.Currency = areader["WEH_WAEHRUNG"].ToString();
                        using (SqlCommand cmdDet = new SqlCommand(detSql, ersConnection))
                        {
                            cmdDet.Parameters.AddWithValue("@WEZ_ORIG", Logging.strToIntDef(areader["WEH_ORIG"].ToString(), 0));
                            cmdDet.Parameters.AddWithValue("@WEZ_EINGANG", Logging.strToIntDef(areader["WEH_EINGANG"].ToString(), 0));
                            using(SqlDataReader detReader = cmdDet.ExecuteReader())
                            {
                                while (detReader.Read())
                                {
                                    GoodsInLine anItem = new GoodsInLine();
                                    shipmentJson.Shipment.Lines.Add(anItem);
                                    anItem.LineNo = Logging.strToIntDef(detReader["WEZ_ZEILE"].ToString(), 0);
                                    anItem.DeliveryNoteQty = Logging.strToDoubleDef(detReader["DELIVERY_NOTE_QTY"].ToString(), 0);
                                    anItem.OrderNo = Convert.ToInt32(detReader["ORDER_NUMNER"]);
                                    anItem.OrderPosition = Logging.strToIntDef(detReader["ORDER_POSITION"].ToString(), 0);
                                    anItem.PP_Price = Logging.strToDoubleDef(detReader["PP_PRICE"].ToString(), 0);
                                    anItem.QtyDelivered = Logging.strToDoubleDef(detReader["QTY_DELIVERED"].ToString(), 0);
                                    anItem.QtyInvoiced = Logging.strToDoubleDef(detReader["INVOICED_QTY"].ToString(), 0);
                                    anItem.QtyOrderd = Logging.strToDoubleDef(detReader["ORDERED_QTY"].ToString(), 0);
                                    anItem.SkuId = Logging.strToIntDef(detReader["WEZ_REFNUMMER"].ToString(), 0);

                                }
                            }
                        }

                        String ajsonStr = SimpleJson.SerializeObject(shipmentJson).ToString();
                        String md5Contents = Logging.CreateMD5(ajsonStr);


                        String storedMd5 = getMd5(areader["WEH_ORIG"].ToString(), areader["WEH_EINGANG"].ToString(),
                            "1", "1", "1", "SHIPMENT", Logging.strToInt64Def(dcSetup.ShipmentsUpdate, 0), ersConnection);

                        if (!md5Contents.Equals(storedMd5))
                        {
                            Logging.WriteDebug("JSON " + SimpleJson.SerializeObject(shipmentJson).ToString(), dcSetup.Debug);
                            SendNewMessageQueue(SimpleJson.SerializeObject(shipmentJson).ToString(), dcSetup.ShipmentsQueueName);
                            updateDaasExport(areader["WEH_ORIG"].ToString(), areader["WEH_EINGANG"].ToString(), "1", "1", "1", "SHIPMENT", md5Contents, ersConnection);
                        }


                    }
                }

            }

            DateTime anow = DateTime.Now;
            String snow = anow.ToString("yyyyMMddhhmmss");
            dcSetup.ShipmentsUpdate = snow;
            dcSetup.resetShipmentsDateRange();


        }

        public void getShipmentsNZ(SqlConnection ersConnection)
        {
            Logging.WriteLog("Starting getShipmentsNZ");

            String lastUpdate = dcSetup.ShipmentsNZUpdate;
            if (lastUpdate.Equals("0") || lastUpdate.Equals(""))
            {
                String iiSql = "delete from " + DaasExportTable + " where DAAS_SET_NAME = 'SHIPMENTNZ' ";
                using (SqlCommand cmd = new SqlCommand(iiSql, ersConnection))
                {
                    cmd.ExecuteNonQuery();
                }
            }

            String dateFrom = dcSetup.ShipmentNZFromDate.ToString();
            String dateTo = dcSetup.ShipmentNZToDate.ToString();

            if (dcSetup.ShipmentNZFromDate > 0 || dcSetup.ShipmentNZToDate > 0)
            {
                String anSql = "delete from " + DaasExportTable + " where DAAS_SET_NAME = 'SHIPMENTNZ' and DAAS_KEY1 >= @mindate and DAAS_KEY1 <= @maxDate  ";
                using (SqlCommand cmd = new SqlCommand(anSql, ersConnection))
                {
                    cmd.Parameters.AddWithValue("@mindate", dcSetup.ShipmentNZFromDate.ToString());
                    cmd.Parameters.AddWithValue("@maxDate", dcSetup.ShipmentNZToDate.ToString());
                    cmd.ExecuteNonQuery();
                }

            }

            if (lastUpdate.Length >= 8)
            {
                lastUpdate = lastUpdate.Substring(0, 8);
            }
            int iLastUpdate = Logging.strToIntDef(lastUpdate, 0);
            int initialdate = dcSetup.ShipmentsNZInitialDate;

            String top10 = " ";
            if (dcSetup.ResultSet > 0)
            {
                top10 = " top " + dcSetup.ResultSet.ToString();
            }

            String headSql = "select " + top10 + " WEH_ORIG, WEH_EINGANG, WEH_DATUM, WEH_TEXT, WEH_WAEHRUNG from WE_HEADR " +
                " where ((WEH_DATUM >=  " + initialdate + " and WEH_DATUM >= " + iLastUpdate + ") or (WEH_DATUM >= " + dcSetup.ShipmentNZFromDate + " and WEH_DATUM <= " + dcSetup.ShipmentNZToDate + ")) " +
                " order by WEH_DATUM ";

            Logging.WriteDebug(headSql, dcSetup.Debug);

            using (SqlCommand cmd = new SqlCommand(headSql, ersConnection))
            {
                String detSql = "SELECT WEZ_ZEILE, WEZ_REFNUMMER, WEZ_BESTELLNUMMER [ORDER_NUMNER], WEZ_BESTELLZEILE[ORDER_POSITION], WEZ_ANZBESTELLT [ORDERED_QTY], " +
                    " WEZ_ANZGELIEFERT[DELIVERY_NOTE_QTY], WEZ_ANZBERECHNET[INVOICED_QTY],  WEZ_EKWPREIS[PP_PRICE], WEZ_ANZGEZAEHLT[QTY_DELIVERED] FROM WE_ZEILE " +
                    " WHERE WEZ_ORIG = @WEZ_ORIG AND WEZ_EINGANG = @WEZ_EINGANG ";

                using (SqlDataReader areader = cmd.ExecuteReader())
                {
                    while (areader.Read())
                    {
                        ShipmentJson shipmentJson = new ShipmentJson();
                        shipmentJson.Shipment = new GoodsIn();
                        shipmentJson.Shipment.Id = areader["WEH_ORIG"].ToString() + "-" + areader["WEH_EINGANG"].ToString();
                        shipmentJson.Shipment.Lines = new List<GoodsInLine>();
                        shipmentJson.Shipment.ShipmentDate = Logging.strToIntDef(areader["WEH_DATUM"].ToString(), 0);
                        shipmentJson.Shipment.Text = areader["WEH_TEXT"].ToString();
                        shipmentJson.Shipment.Currency = areader["WEH_WAEHRUNG"].ToString();

                        using (SqlCommand cmdDet = new SqlCommand(detSql, ersConnection))
                        {
                            cmdDet.Parameters.AddWithValue("@WEZ_ORIG", Logging.strToIntDef(areader["WEH_ORIG"].ToString(), 0));
                            cmdDet.Parameters.AddWithValue("@WEZ_EINGANG", Logging.strToIntDef(areader["WEH_EINGANG"].ToString(), 0));
                            using (SqlDataReader detReader = cmdDet.ExecuteReader())
                            {
                                while (detReader.Read())
                                {
                                    GoodsInLine anItem = new GoodsInLine();
                                    shipmentJson.Shipment.Lines.Add(anItem);
                                    anItem.LineNo = Logging.strToIntDef(detReader["WEZ_ZEILE"].ToString(), 0);
                                    anItem.DeliveryNoteQty = Logging.strToDoubleDef(detReader["DELIVERY_NOTE_QTY"].ToString(), 0);
                                    anItem.OrderNo = Convert.ToInt32(detReader["ORDER_NUMNER"]);
                                    anItem.OrderPosition = Logging.strToIntDef(detReader["ORDER_POSITION"].ToString(), 0);
                                    anItem.PP_Price = Logging.strToDoubleDef(detReader["PP_PRICE"].ToString(), 0);
                                    anItem.QtyDelivered = Logging.strToDoubleDef(detReader["QTY_DELIVERED"].ToString(), 0);
                                    anItem.QtyInvoiced = Logging.strToDoubleDef(detReader["INVOICED_QTY"].ToString(), 0);
                                    anItem.QtyOrderd = Logging.strToDoubleDef(detReader["ORDERED_QTY"].ToString(), 0);
                                    anItem.SkuId = Logging.strToIntDef(detReader["WEZ_REFNUMMER"].ToString(), 0);

                                }
                            }
                        }

                        String ajsonStr = SimpleJson.SerializeObject(shipmentJson).ToString();
                        String md5Contents = Logging.CreateMD5(ajsonStr);


                        String storedMd5 = getMd5(areader["WEH_ORIG"].ToString(), areader["WEH_EINGANG"].ToString(),
                            "1", "1", "1", "SHIPMENTNZ", Logging.strToInt64Def(dcSetup.ShipmentsNZUpdate, 0), ersConnection);

                        if (!md5Contents.Equals(storedMd5))
                        {
                            Logging.WriteDebug("JSON " + SimpleJson.SerializeObject(shipmentJson).ToString(), dcSetup.Debug);
                            SendNewMessageQueue(SimpleJson.SerializeObject(shipmentJson).ToString(), dcSetup.ShipmentsQueueName);
                            updateDaasExport(areader["WEH_ORIG"].ToString(), areader["WEH_EINGANG"].ToString(), "1", "1", "1", "SHIPMENTNZ", md5Contents, ersConnection);
                        }


                    }
                }

            }

            DateTime anow = DateTime.Now;
            String snow = anow.ToString("yyyyMMddhhmmss");
            dcSetup.ShipmentsNZUpdate = snow;
            dcSetup.resetShipmentsNZDateRange();


        }


        public void getPOs(SqlConnection ersConnection)
        {

            Logging.WriteLog("Starting getPOs");

            String lastUpdate = dcSetup.OrdersUpdate;

            if (lastUpdate.Equals("0") || lastUpdate.Equals(""))
            {
                String iiSql = "delete from " + DaasExportTable + " where DAAS_SET_NAME = 'ORDER' ";
                using (SqlCommand cmd = new SqlCommand(iiSql, ersConnection))
                {
                    cmd.ExecuteNonQuery();
                }
            }

            if (lastUpdate.Length >= 8)
            {
                lastUpdate = lastUpdate.Substring(0, 8);
            }
            int iLastUpdate = Logging.strToIntDef(lastUpdate, 0);
            int initialdate = dcSetup.OrdersInitialDate;

            String top10 = " ";
            if (dcSetup.ResultSet > 0)
            {
                top10 = " top " + dcSetup.ResultSet.ToString();
            }

            int dateFrom = dcSetup.OrdersFromDate;
            int dateTo = dcSetup.OrdersToDate;

            if(dateFrom > 0 || dateTo > 0)
            {
                String rSql = "delete from " + DaasExportTable + " where DAAS_SET_NAME = 'ORDER' and DAAS_KEY3 >= @mindate and DAAS_KEY3 <= @maxDate  ";
                using (SqlCommand cmd = new SqlCommand(rSql, ersConnection))
                {
                    cmd.Parameters.AddWithValue("@mindate", dateFrom.ToString());
                    cmd.Parameters.AddWithValue("@maxDate", dateTo.ToString());
                    cmd.ExecuteNonQuery();
                }

            }


            String anSql = "select " + top10 + "  BST_ORIGNR, BST_BESTELLUNG, BST_STATUS, BST_BESTELLDATUM [ORDER_DATE],  BST_LIEFER_AB [DELIVERY_DATE_FROM], BST_LIEFER_BIS [DELIVEY_DATE_TO], " +
                " BST_EINGANGDATUM[ARRIVAL_DATE], BST_LIEFERANT[SUPPLIER], BST_TOT_WARE_EK[TOTAL_PURCHASE_VALUE], BST_TOT_WARE_VK[TOTAL_SALE_VALUE], BST_TOT_ANZAHL[TOTAL_QTY], BST_ULOG_DATE, " +
                " RTRIM(BST_TEXT_1 + ' ' + BST_TEXT_2)[OTEXT],  " +
                " CASE " +
                "   WHEN BST_STATUS = 0 THEN 'PLANNED' " +
                "   WHEN BST_STATUS = 1 THEN 'ORDERED' " +
                "   WHEN BST_STATUS = 2 THEN 'DELIVERED' " +
                "   WHEN BST_STATUS = 3 THEN 'BILLED' " +
                "   WHEN BST_STATUS = 4 THEN 'CANCELLED' " +
                "   ELSE '' " +
                " END[STATUS] " +
                " from V_BESTHEAD WHERE BST_MANDANT = 1 AND ((BST_ULOG_DATE >= " + iLastUpdate + " AND BST_ULOG_DATE >= " + initialdate + ") or (BST_ULOG_DATE >= " + dateFrom + " AND BST_ULOG_DATE <= " + dateTo + " ))";

            Logging.WriteDebug(anSql, dcSetup.Debug);

            String detsql = "SELECT BDT_ORIGNR, BDT_BESTELLUNG, BDT_REFNUMMER, BDT_TEXT, BDT_BESTELL_MENGE[ORDER_QTY], BDT_ANZ_LIEFERUNG[DELIVERI_QTY], BDT_RECHNUNG_MENGE[INVOICED_QTY], " +
                " BDT_EK_CALC[PURCHASE_PRICE_TOTAL], BDT_VK_CALC[SALES_PRICE_TOTAL], BDT_VK_SOLL, BDT_EKP_BESTELLT[PURCHASE_PRICE], BDT_POSITION, " +
                " case  " +
                "   when ART_SET_EKGEW_MODE <> 0 then ART_EK_GEWICHTET " +
                "   else ART_EK_DM " +
                " end[WAC] " +

                " FROM V_BESTZEIL " +
                " JOIN V_ARTIKEL ON ART_MANDANT = 1 AND ART_REFNUMMER = BDT_REFNUMMER " + 
                " WHERE BDT_BESTELLUNG = @BDT_BESTELLUNG AND BDT_ORIGNR = @BDT_ORIGNR and BDT_MANDANT = 1 ";

            using(SqlCommand cmd = new SqlCommand(anSql, ersConnection))
            {
                cmd.CommandTimeout = 1200;
                using(SqlDataReader areader = cmd.ExecuteReader())
                {
                    while (areader.Read())
                    {
                        POJson apo = new POJson();
                        apo.Order = new POOrder();
                        apo.Order.OrderId = areader["BST_ORIGNR"].ToString() + "-" + areader["BST_BESTELLUNG"].ToString();
                        apo.Order.ArrivalDate = Logging.strToIntDef(areader["ARRIVAL_DATE"].ToString(), 0);
                        apo.Order.OrderDate = Logging.strToIntDef(areader["ORDER_DATE"].ToString(), 0);
                        apo.Order.POLines = new List<PODetails>();
                        apo.Order.Supplier = areader["SUPPLIER"].ToString();
                        apo.Order.Text = areader["OTEXT"].ToString();
                        apo.Order.TotalOrderQty = Logging.strToDoubleDef(areader["TOTAL_QTY"].ToString(), 0);
                        apo.Order.TotalPurchaseValue = Logging.strToDoubleDef(areader["TOTAL_PURCHASE_VALUE"].ToString(), 0);
                        apo.Order.TotalRetailValue = Logging.strToDoubleDef(areader["TOTAL_SALE_VALUE"].ToString(), 0);

                        using(SqlCommand cmdDet = new SqlCommand(detsql, ersConnection))
                        {
                            cmdDet.Parameters.AddWithValue("@BDT_BESTELLUNG", areader["BST_BESTELLUNG"].ToString());
                            cmdDet.Parameters.AddWithValue("@BDT_ORIGNR", areader["BST_ORIGNR"].ToString());
                            using (SqlDataReader detReader = cmdDet.ExecuteReader())
                            {
                                while (detReader.Read())
                                {
                                    PODetails adetail = new PODetails();
                                    apo.Order.POLines.Add(adetail);
                                    adetail.OrderId = areader["BST_ORIGNR"].ToString() + "-" + areader["BST_BESTELLUNG"].ToString();
                                    adetail.DeliveredQty = Logging.strToDoubleDef(detReader["DELIVERI_QTY"].ToString(), 0);
                                    adetail.Description = detReader["BDT_TEXT"].ToString();
                                    adetail.OrderQty = Logging.strToDoubleDef(detReader["ORDER_QTY"].ToString(), 0);
                                    adetail.Line = Logging.strToIntDef(detReader["BDT_POSITION"].ToString(), 1);
                                    adetail.PurchaseValue = Logging.strToDoubleDef(detReader["PURCHASE_PRICE_TOTAL"].ToString(), 0);
                                    adetail.RetailValue = Logging.strToDoubleDef(detReader["SALES_PRICE_TOTAL"].ToString(), 0);
                                    adetail.SkuId = Logging.strToIntDef(detReader["BDT_REFNUMMER"].ToString(), 1);
                                    adetail.WAC = Logging.strToDoubleDef(detReader["WAC"].ToString(), 0);
                                }

                            }



                        }

                        String ajsonStr = SimpleJson.SerializeObject(apo).ToString();
                        String md5Contents = Logging.CreateMD5(ajsonStr);


                        String storedMd5 = getMd5(areader["BST_ORIGNR"].ToString(), areader["BST_BESTELLUNG"].ToString(),
                            areader["BST_ULOG_DATE"].ToString(), "1", "1", "ORDER", Logging.strToInt64Def(dcSetup.OrdersUpdate, 0), ersConnection);

                        if (!md5Contents.Equals(storedMd5) )
                        {
                            Logging.WriteDebug("JSON " + SimpleJson.SerializeObject(apo).ToString(), dcSetup.Debug);
                            SendNewMessageQueue(SimpleJson.SerializeObject(apo).ToString(), dcSetup.OrdersQueueName);
                            updateDaasExport(areader["BST_ORIGNR"].ToString(), areader["BST_BESTELLUNG"].ToString(), areader["BST_ULOG_DATE"].ToString(), "1", "1", "ORDER", md5Contents, ersConnection);
                        }



                    }
                }
            }

            DateTime anow = DateTime.Now;
            String snow = anow.ToString("yyyyMMddhhmmss");
            dcSetup.OrdersUpdate = snow;
            dcSetup.resetOrdersDateRange();

        }

        public void getTransfersFromBranchesNZ(SqlConnection ersConnection)
        {
            Logging.WriteLog("Starting getTransfersFromBranchesNZ");

            String lastUpdate = dcSetup.TransfersFromBranchesNZUpdate;
            if (lastUpdate.Equals("0") || lastUpdate.Equals(""))
            {
                String iiSql = "delete from " + DaasExportTable + " where DAAS_SET_NAME = 'TRANSFERS_BNZ' ";
                using (SqlCommand cmd = new SqlCommand(iiSql, ersConnection))
                {
                    cmd.ExecuteNonQuery();
                }
            }

            if (lastUpdate.Length >= 8)
            {
                lastUpdate = lastUpdate.Substring(0, 8);
            }
            int iLastUpdate = Logging.strToIntDef(lastUpdate, 0);
            int initialdate = dcSetup.TransfersFromBranchesNZInitialDate;

            String top10 = " ";
            if (dcSetup.ResultSet > 0)
            {
                top10 = " top " + dcSetup.ResultSet.ToString();
            }

            int dateFrom = dcSetup.TransfersFromBranchesNZFromDate;
            int dateTo = dcSetup.TransfersFromBranchesNZToDate;
            if (dateFrom > 0 || dateTo > 0)
            {
                String rSql = "delete from " + DaasExportTable + " where DAAS_SET_NAME = 'TRANSFERS_BNZ' and DAAS_KEY4 >= @mindate and DAAS_KEY4 <= @maxDate  ";
                using (SqlCommand cmd = new SqlCommand(rSql, ersConnection))
                {
                    cmd.Parameters.AddWithValue("@mindate", dateFrom.ToString());
                    cmd.Parameters.AddWithValue("@maxDate", dateTo.ToString());
                    cmd.ExecuteNonQuery();
                }

            }


            String anSql = "select " + top10 + " FTK_FILIALE, FTK_KASSE, FTK_NUMMER, FTK_VON_NUMMER [FROM_BRANCH], FTK_AN_NUMMER [TO_BRANCH], FTK_CLOG_DATE [CREATION_DATE], FTK_ACK_DATE, " +
                " FTK_LIEFERDATUM[DELIVERY_DATE], FTK_CLOG_USER,FTK_STATUS, FTK_TEXT, FTK_REF_FILIALE, FTK_REF_KASSE, FTK_REF_NUMMER, " +
                " CASE " +
                "   WHEN FTK_TYP = 2 THEN 'OPEN TRANSFERS' " +
                "   WHEN FTK_TYP = 4 THEN 'GOODS IN (FROM BRANCH / STOCKROOM IN TRANSIT) IN THIS BRANCH' " +
                "   WHEN FTK_TYP = 6 THEN 'INTER BRANCH TRANSFERS' " +
                "   else '' " +
                " end[FTK_TYP], " +
                " (select isnull(max(KAS_VK_DATUM), 0) from KASSTRNS where KAS_FILIALE = FTK_REF_FILIALE AND KAS_KASSE = FTK_REF_KASSE AND KAS_BONNR= FTK_REF_NUMMER AND FTK_LIEFERDATUM >= KAS_VK_DATUM) [DN_DATE], " +
                " FTK_TYP [FTK_TYP_NO], FTK_LIEFERSCHEIN [DELIVERY_NOTE] " +
                " from V_FTR_KOPF " +
                " where FTK_TYP in (4, 2, 6) and FTK_AN_TYP = 2 AND FTK_FILIALE <> 1 " +
                " and(FTK_VON_NUMMER > 200 or FTK_AN_NUMMER > 200) AND((FTK_LIEFERDATUM >= " + initialdate + " and FTK_LIEFERDATUM >= " + iLastUpdate +
                " ) or(FTK_CLOG_DATE >=  " + initialdate + " and FTK_CLOG_DATE >= " + iLastUpdate + " ) or(FTK_CLOG_DATE > 0 and  FTK_CLOG_DATE >=  " + dateFrom + " and FTK_CLOG_DATE <= " + dateTo + " )) ";

            Logging.WriteDebug(anSql, dcSetup.Debug);

            String dSql = "select FTR_ZEILE [LINE_NO], FTR_REFNUMMER [SKU_ID], FTR_ANZAHL [QTY], FTR_EINZELPREIS [UNIT_PRICE], ART_VKPREIS from V_FTR_DATA " +
                " JOIN ARTIKEL ON ART_REFNUMMER = FTR_REFNUMMER " +
                " where FTR_FILIALE = @FTR_FILIALE AND FTR_KASSE = @FTR_KASSE AND FTR_NUMMER = @FTR_NUMMER ";

            String dkSql = "select KAS_POSNR, KAS_REFNUMMER, KAS_ANZAHL, KAS_BETRAG " +
                " from KASSTRNS " +
                " where KAS_MANDANT = 1 AND KAS_SATZART = 20 AND KAS_REFNUMMER > 20 AND " +
                " KAS_DATUM = @KAS_DATUM AND KAS_FILIALE = @FTR_FILIALE AND KAS_KASSE = @FTR_KASSE AND KAS_BONNR =  @FTR_NUMMER ORDER BY 1";

            using (SqlCommand cmd = new SqlCommand(anSql, ersConnection))
            {
                cmd.CommandTimeout = 1200;
                using (SqlDataReader areader = cmd.ExecuteReader())
                {
                    while (areader.Read())
                    {
                        String delNo = areader["FTK_REF_FILIALE"].ToString() + "/" + areader["FTK_REF_KASSE"].ToString() + "/" + areader["FTK_REF_NUMMER"].ToString();

                        BTransfersJson btransfersJson = new BTransfersJson();
                        TransferBHeader aheader = new TransferBHeader();
                        btransfersJson.TransferFromBranch = aheader;
                        String dndate = areader["DN_DATE"].ToString();
                        if (dndate.Equals("0"))
                        {
                            dndate = "";
                        }

                        int afiliale = Convert.ToInt32(areader["FTK_FILIALE"]);
                        int akasse = Convert.ToInt32(areader["FTK_KASSE"]);
                        int tnumber = Convert.ToInt32(areader["FTK_NUMMER"]);
                        aheader.TrasferNo = afiliale.ToString() + "-" + akasse.ToString() + "-" + tnumber.ToString();
                        aheader.FromStore = Convert.ToInt32(areader["FROM_BRANCH"]);
                        aheader.RequestUser = Convert.ToInt32(areader["FTK_CLOG_USER"]);
                        aheader.ToStore = Convert.ToInt32(areader["TO_BRANCH"]);
                        aheader.Type = areader["FTK_TYP"].ToString();
                        aheader.Details = new List<TransferBDetails>();
                        aheader.CreationDate = Convert.ToInt32(areader["CREATION_DATE"]);
                        aheader.DespatchDate = Convert.ToInt32(areader["DELIVERY_DATE"]);
                        aheader.AcknowledgeDate = Convert.ToInt32(areader["FTK_ACK_DATE"]);
                        aheader.DeliveryNoteNumber = delNo;
                        aheader.DeliveryNote = areader["DELIVERY_NOTE"].ToString();
                        aheader.Text = areader["FTK_TEXT"].ToString();
                        aheader.DNDate = dndate;
                        int ftkTyp = Convert.ToInt32(areader["FTK_TYP_NO"]);
                        using (SqlCommand dcmd = new SqlCommand(dSql, ersConnection))
                        {
                            dcmd.Parameters.AddWithValue("@FTR_FILIALE", afiliale);
                            dcmd.Parameters.AddWithValue("@FTR_KASSE", akasse);
                            dcmd.Parameters.AddWithValue("@FTR_NUMMER", tnumber);
                            using (SqlDataReader dreader = dcmd.ExecuteReader())
                            {
                                while (dreader.Read())
                                {
                                    TransferBDetails adetails = new TransferBDetails();
                                    aheader.Details.Add(adetails);

                                    adetails.LineNo = Convert.ToInt32(dreader["LINE_NO"].ToString());
                                    adetails.Qty = Logging.strToDoubleDef(dreader["QTY"].ToString(), 0);
                                    adetails.SkuId = Convert.ToInt32(dreader["SKU_ID"]);
                                    adetails.UnitPrice = Logging.strToDoubleDef(dreader["UNIT_PRICE"].ToString(), 0);
                                    if (adetails.UnitPrice == 0)
                                    {
                                        adetails.UnitPrice = Logging.strToDoubleDef(dreader["ART_VKPREIS"].ToString(), 0);
                                    }
                                }
                            }

                        }

                        String ajsonStr = SimpleJson.SerializeObject(btransfersJson).ToString();
                        String md5Contents = Logging.CreateMD5(ajsonStr);


                        String storedMd5 = getMd5(afiliale.ToString(), akasse.ToString(),
                            tnumber.ToString(), areader["CREATION_DATE"].ToString(), "1", "TRANSFERS_BNZ", Logging.strToInt64Def(dcSetup.TransfersFromHONZUpdate, 0), ersConnection);

                        if (!md5Contents.Equals(storedMd5) && ((ftkTyp != 2) || (ftkTyp == 2 && aheader.Details.Count > 0)))
                        {
                            Logging.WriteDebug("JSON " + SimpleJson.SerializeObject(btransfersJson).ToString(), dcSetup.Debug);
                            SendNewMessageQueue(SimpleJson.SerializeObject(btransfersJson).ToString(), dcSetup.TransfersFromBQueueName);
                            updateDaasExport(afiliale.ToString(), akasse.ToString(), tnumber.ToString(), areader["CREATION_DATE"].ToString(), "1", "TRANSFERS_BNZ", md5Contents, ersConnection);
                        }



                    }
                }
            }
            DateTime anow = DateTime.Now;
            String snow = anow.ToString("yyyyMMddhhmmss");
            dcSetup.TransfersFromBranchesNZUpdate = snow;
            dcSetup.resetTransfersFromBranchesNZRange();


        }


        public void getTransfersFromBranches(SqlConnection ersConnection)
        {
            Logging.WriteLog("Starting getTransfersFromBranches");

            String lastUpdate = dcSetup.TransfersFromBranchesUpdate;
            if (lastUpdate.Equals("0") || lastUpdate.Equals(""))
            {
                String iiSql = "delete from " + DaasExportTable + " where DAAS_SET_NAME = 'TRANSFERS_B' ";
                using (SqlCommand cmd = new SqlCommand(iiSql, ersConnection))
                {
                    cmd.ExecuteNonQuery();
                }
            }

            if (lastUpdate.Length >= 8)
            {
                lastUpdate = lastUpdate.Substring(0, 8);
            }
            int iLastUpdate = Logging.strToIntDef(lastUpdate, 0);
            int initialdate = dcSetup.TransfersFromBranchesInitialDate;

            String top10 = " ";
            if (dcSetup.ResultSet > 0)
            {
                top10 = " top " + dcSetup.ResultSet.ToString();
            }

            int dateFrom = dcSetup.TransfersFromBranchesFromDate;
            int dateTo = dcSetup.TransfersFromBranchesToDate;
            if (dateFrom > 0 || dateTo > 0)
            {
                String rSql = "delete from " + DaasExportTable + " where DAAS_SET_NAME = 'TRANSFERS_B' and DAAS_KEY4 >= @mindate and DAAS_KEY4 <= @maxDate  ";
                using (SqlCommand cmd = new SqlCommand(rSql, ersConnection))
                {
                    cmd.Parameters.AddWithValue("@mindate", dateFrom.ToString());
                    cmd.Parameters.AddWithValue("@maxDate", dateTo.ToString());
                    cmd.ExecuteNonQuery();
                }

            }


            String anSql = "select " + top10 + " FTK_FILIALE, FTK_KASSE, FTK_NUMMER, FTK_VON_NUMMER [FROM_BRANCH], FTK_AN_NUMMER [TO_BRANCH], FTK_CLOG_DATE [CREATION_DATE], FTK_ACK_DATE, " +
                " FTK_LIEFERDATUM[DELIVERY_DATE], FTK_CLOG_USER,FTK_STATUS, FTK_TEXT, FTK_REF_FILIALE, FTK_REF_KASSE, FTK_REF_NUMMER, " +
                " CASE " +
                "   WHEN FTK_TYP = 2 THEN 'OPEN TRANSFERS' " +
                "   WHEN FTK_TYP = 4 THEN 'GOODS IN (FROM BRANCH / STOCKROOM IN TRANSIT) IN THIS BRANCH' " +
                "   WHEN FTK_TYP = 6 THEN 'INTER BRANCH TRANSFERS' " +
                "   else '' " +
                " end[FTK_TYP], " +
                " (select isnull(max(KAS_VK_DATUM), 0) from KASSTRNS where KAS_FILIALE = FTK_REF_FILIALE AND KAS_KASSE = FTK_REF_KASSE AND KAS_BONNR= FTK_REF_NUMMER AND FTK_LIEFERDATUM >= KAS_VK_DATUM) [DN_DATE], " +
                " FTK_TYP [FTK_TYP_NO], FTK_LIEFERSCHEIN [DELIVERY_NOTE] " +
                " from V_FTR_KOPF " +
                " where FTK_MANDANT = 1 AND FTK_TYP in (4, 2, 6) and FTK_AN_TYP = 2 AND FTK_FILIALE <> 1 " +
                " and(FTK_VON_NUMMER > 200 or FTK_AN_NUMMER > 200) AND((FTK_LIEFERDATUM >= " + initialdate + " and FTK_LIEFERDATUM >= " + iLastUpdate +
                " ) or(FTK_CLOG_DATE >=  " + initialdate + " and FTK_CLOG_DATE >= " + iLastUpdate + " ) or(FTK_CLOG_DATE > 0 and  FTK_CLOG_DATE >=  " + dateFrom + " and FTK_CLOG_DATE <= " + dateTo + " )) ";

            Logging.WriteDebug(anSql, dcSetup.Debug);

            String dSql = "select FTR_ZEILE [LINE_NO], FTR_REFNUMMER [SKU_ID], FTR_ANZAHL [QTY], FTR_EINZELPREIS [UNIT_PRICE], ART_VKPREIS from V_FTR_DATA " +
                " JOIN ARTIKEL ON ART_MANDANT = 1 AND ART_REFNUMMER = FTR_REFNUMMER " +
                " where FTR_MANDANT = 1 AND FTR_FILIALE = @FTR_FILIALE AND FTR_KASSE = @FTR_KASSE AND FTR_NUMMER = @FTR_NUMMER ";

            String dkSql = "select KAS_POSNR, KAS_REFNUMMER, KAS_ANZAHL, KAS_BETRAG " +
                " from KASSTRNS " +
                " where KAS_MANDANT = 1 AND KAS_SATZART = 20 AND KAS_REFNUMMER > 20 AND " +
                " KAS_DATUM = @KAS_DATUM AND KAS_FILIALE = @FTR_FILIALE AND KAS_KASSE = @FTR_KASSE AND KAS_BONNR =  @FTR_NUMMER ORDER BY 1";

            using (SqlCommand cmd = new SqlCommand(anSql, ersConnection))
            {
                cmd.CommandTimeout = 1200;
                using (SqlDataReader areader = cmd.ExecuteReader())
                {
                    while (areader.Read())
                    {
                        String delNo = areader["FTK_REF_FILIALE"].ToString() + "/" + areader["FTK_REF_KASSE"].ToString() + "/" + areader["FTK_REF_NUMMER"].ToString();

                        BTransfersJson btransfersJson = new BTransfersJson();
                        TransferBHeader aheader = new TransferBHeader();
                        btransfersJson.TransferFromBranch = aheader;
                        String dndate = areader["DN_DATE"].ToString();
                        if (dndate.Equals("0") )
                        {
                            dndate = "";
                        }

                        int afiliale = Convert.ToInt32(areader["FTK_FILIALE"]);
                        int akasse = Convert.ToInt32(areader["FTK_KASSE"]);
                        int tnumber = Convert.ToInt32(areader["FTK_NUMMER"]);
                        aheader.TrasferNo = afiliale.ToString() + "-" + akasse.ToString() + "-" + tnumber.ToString();
                        aheader.FromStore = Convert.ToInt32(areader["FROM_BRANCH"]);
                        aheader.RequestUser = Convert.ToInt32(areader["FTK_CLOG_USER"]);
                        aheader.ToStore = Convert.ToInt32(areader["TO_BRANCH"]);
                        aheader.Type = areader["FTK_TYP"].ToString();
                        aheader.Details = new List<TransferBDetails>();
                        aheader.CreationDate = Convert.ToInt32(areader["CREATION_DATE"]);
                        aheader.DespatchDate = Convert.ToInt32(areader["DELIVERY_DATE"]);
                        aheader.AcknowledgeDate = Convert.ToInt32(areader["FTK_ACK_DATE"]);
                        aheader.DeliveryNoteNumber = delNo;
                        aheader.DeliveryNote = areader["DELIVERY_NOTE"].ToString();
                        aheader.Text = areader["FTK_TEXT"].ToString();
                        aheader.DNDate = dndate;
                        int ftkTyp = Convert.ToInt32(areader["FTK_TYP_NO"]);
                        using (SqlCommand dcmd = new SqlCommand(dSql, ersConnection))
                        {
                            dcmd.Parameters.AddWithValue("@FTR_FILIALE", afiliale);
                            dcmd.Parameters.AddWithValue("@FTR_KASSE", akasse);
                            dcmd.Parameters.AddWithValue("@FTR_NUMMER", tnumber);
                            using (SqlDataReader dreader = dcmd.ExecuteReader())
                            {
                                while(dreader.Read())
                                {
                                    TransferBDetails adetails = new TransferBDetails();
                                    aheader.Details.Add(adetails);

                                    adetails.LineNo = Convert.ToInt32(dreader["LINE_NO"].ToString());
                                    adetails.Qty = Logging.strToDoubleDef(dreader["QTY"].ToString(), 0);
                                    adetails.SkuId = Convert.ToInt32(dreader["SKU_ID"]);
                                    adetails.UnitPrice = Logging.strToDoubleDef(dreader["UNIT_PRICE"].ToString(), 0);
                                    if(adetails.UnitPrice == 0)
                                    {
                                        adetails.UnitPrice = Logging.strToDoubleDef(dreader["ART_VKPREIS"].ToString(), 0);
                                    }
                                }
                            }

                        }

                        String ajsonStr = SimpleJson.SerializeObject(btransfersJson).ToString();
                        String md5Contents = Logging.CreateMD5(ajsonStr);


                        String storedMd5 = getMd5(afiliale.ToString(), akasse.ToString(),
                            tnumber.ToString(), areader["CREATION_DATE"].ToString(), "1", "TRANSFERS_B", Logging.strToInt64Def(dcSetup.TransfersFromHOUpdate, 0), ersConnection);

                        if (!md5Contents.Equals(storedMd5) && ((ftkTyp != 2) || (ftkTyp == 2 && aheader.Details.Count > 0)))
                        {
                            Logging.WriteDebug("JSON " + SimpleJson.SerializeObject(btransfersJson).ToString(), dcSetup.Debug);
                            SendNewMessageQueue(SimpleJson.SerializeObject(btransfersJson).ToString(), dcSetup.TransfersFromBQueueName);
                            updateDaasExport(afiliale.ToString(), akasse.ToString(), tnumber.ToString(), areader["CREATION_DATE"].ToString(), "1", "TRANSFERS_B", md5Contents, ersConnection);
                        }



                    }
                }
            }
            DateTime anow = DateTime.Now;
            String snow = anow.ToString("yyyyMMddhhmmss");
            dcSetup.TransfersFromBranchesUpdate = snow;
            dcSetup.resetTransfersFromBranchesRange();


        }

        public void getRetailTransfers(SqlConnection ersConnection)
        {
            Logging.WriteLog("Starting getRetailTransfers");

            String lastUpdate = dcSetup.TransfersFromHOUpdate;
            if (lastUpdate.Equals("0") || lastUpdate.Equals(""))
            {
                String iiSql = "delete from " + DaasExportTable + " where DAAS_SET_NAME = 'TRANSFERS_HO' ";
                using (SqlCommand cmd = new SqlCommand(iiSql, ersConnection))
                {
                    cmd.ExecuteNonQuery();
                }
            }

            if (lastUpdate.Length >= 8)
            {
                lastUpdate = lastUpdate.Substring(0, 8);
            }
            int iLastUpdate = Logging.strToIntDef(lastUpdate, 0);
            int initialdate = dcSetup.TransfersFromHOInitialDate;

            int dateFrom = dcSetup.TransfersFromHOFromDate;
            int dateTo = dcSetup.TransfersFromHOToDate;


            String top10 = " ";
            if (dcSetup.ResultSet > 0)
            {
                top10 = " top " + dcSetup.ResultSet.ToString();
            }

            if (dateFrom > 0 || dateTo > 0)
            {
                String rSql = "delete from " + DaasExportTable + " where DAAS_SET_NAME = 'TRANSFERS_HO' and DAAS_KEY4 >= @mindate and DAAS_KEY4 <= @maxDate  ";
                using (SqlCommand cmd = new SqlCommand(rSql, ersConnection))
                {
                    cmd.Parameters.AddWithValue("@mindate", dateFrom.ToString());
                    cmd.Parameters.AddWithValue("@maxDate", dateTo.ToString());
                    cmd.ExecuteNonQuery();
                }

            }


            String anSql = "select " + top10 + " LFS_ORIGNR, LFS_ANG_ANR, LFS_LFS, cast(LFS_ORIGNR as varchar) + '-' +  cast(LFS_ANG_ANR as varchar) + '-' + cast(LFS_LFS as varchar) [TRANSFER_NO], " +
                " LFS_VONNR[FROM_STORE], LFS_KNR[TO_STORE], LFS_CLOG_DATE, LFS_DATLFS, LFS_ULOG_DATE, " +
                " case  " +
                "   when LFS_STATUS = 0 then 'Standard delivery note' " +
                "   when LFS_STATUS = 1 then 'Delivery note being delivered' " +
                "   when LFS_STATUS = 2 then 'Delivery note was delivered' " +
                "   when LFS_STATUS = 3 then 'Delivery note back from delivery (branch)' " +
                "   else '' " +
                " end[LFS_STATUS], LFS_CLOG_USER " +
                " FROM V_LIEFHEAD " +
                " where " +
                //" LFS_KTYP = 6 AND LFS_STATUS = 2 and " + 
                " LFS_MANDANT = 1 AND LFS_KTYP = 2 and ((LFS_ULOG_DATE >= " + iLastUpdate + " and LFS_ULOG_DATE >= " + initialdate + ") or (LFS_CLOG_DATE >= " + iLastUpdate + " and LFS_CLOG_DATE >= " + initialdate + ") or (LFS_ULOG_DATE > 0 and LFS_ULOG_DATE >= " + dateFrom + " and LFS_ULOG_DATE <= " + dateTo + "  )  )";
            Logging.WriteDebug(anSql, dcSetup.Debug);
            using (SqlCommand cmd = new SqlCommand(anSql, ersConnection))
            {
                cmd.CommandTimeout = 1200;
                using (SqlDataReader areader = cmd.ExecuteReader())
                {
                    while (areader.Read())
                    {
                        RTransfersJson transferJson = new RTransfersJson();
                        TransferHeader transfer = new TransferHeader();
                        transferJson.TransferFromHO = transfer;

                        transfer.CreationDate = Convert.ToInt32(areader["LFS_CLOG_DATE"]);
                        transfer.DespatchDate = Convert.ToInt32(areader["LFS_DATLFS"]);
                        transfer.FromStore = Convert.ToInt32(areader["FROM_STORE"]);
                        transfer.RequestUser = Convert.ToInt32(areader["LFS_CLOG_USER"]);
                        transfer.ToStore = Convert.ToInt32(areader["TO_STORE"]);
                        transfer.TrasferNo = areader["TRANSFER_NO"].ToString();
                        transfer.Details = new List<TransferDetails>();

                        int origin = Convert.ToInt32(areader["LFS_ORIGNR"]);
                        int andAnr = Convert.ToInt32(areader["LFS_ANG_ANR"]);
                        int lfs = Convert.ToInt32(areader["LFS_LFS"]);

                        String trSql = "select  LZL_ZNR, LZL_REFNR[SKU_ID], LZL_MENGE [QTY] , " +
                            " CASE " +
                            "   WHEN LZL_STAT_EKDM = 0 THEN LZL_EEK " +
                            "   ELSE LZL_STAT_EKDM " +
                            " END[COST], " +
                            " CASE " +
                            "   WHEN LZL_STAT_VKDM = 0 THEN LZL_EVKB " +
                            "   ELSE LZL_STAT_VKDM " +
                            " END[RT_PRICE] " +
                            " from LIEFZEIL " +
                            " where LZL_MANDANT = 1 AND LZL_REFNR <> 0  AND LZL_ORIGNR = @LZL_ORIGNR AND LZL_ANG_ANR = @LZL_ANG_ANR AND LZL_LFS = @LZL_LFS ";
                        using (SqlCommand trCmd = new SqlCommand(trSql, ersConnection))
                        {
                            trCmd.Parameters.AddWithValue("@LZL_ORIGNR", origin);
                            trCmd.Parameters.AddWithValue("@LZL_ANG_ANR", andAnr);
                            trCmd.Parameters.AddWithValue("@LZL_LFS", lfs);
                            using (SqlDataReader trReader = trCmd.ExecuteReader())
                            {
                                while (trReader.Read())
                                {
                                    TransferDetails adetail = new TransferDetails();
                                    transfer.Details.Add(adetail);
                                    adetail.Cost = Logging.strToDoubleDef(trReader["COST"].ToString(), 0);
                                    adetail.LineNo = Convert.ToInt32(trReader["LZL_ZNR"]);
                                    adetail.Qty = Logging.strToDoubleDef(trReader["QTY"].ToString(), 0);
                                    adetail.RetailPrice = Logging.strToDoubleDef(trReader["RT_PRICE"].ToString(), 0);
                                    adetail.SkuId = Convert.ToInt32(trReader["SKU_ID"]);
                                    adetail.TransferNo = transfer.TrasferNo;
                                }
                            }
                        }

                        String ajsonStr = SimpleJson.SerializeObject(transferJson).ToString();
                        String md5Contents = Logging.CreateMD5(ajsonStr);


                        String storedMd5 = getMd5(origin.ToString(), andAnr.ToString(),
                            lfs.ToString(), areader["LFS_CLOG_DATE"].ToString(), "1", "TRANSFERS_HO", Logging.strToInt64Def(dcSetup.TransfersFromHOUpdate, 0), ersConnection);

                        if (!md5Contents.Equals(storedMd5))
                        {
                            Logging.WriteDebug("JSON " + SimpleJson.SerializeObject(transferJson).ToString(), dcSetup.Debug);
                            SendNewMessageQueue(SimpleJson.SerializeObject(transferJson).ToString(), dcSetup.TransfersFromHOQueueName);
                            updateDaasExport(origin.ToString(), andAnr.ToString(), lfs.ToString(), areader["LFS_CLOG_DATE"].ToString(), "1", "TRANSFERS_HO", md5Contents, ersConnection);
                        }


                    }

                }
            }


            DateTime anow = DateTime.Now;
            String snow = anow.ToString("yyyyMMddhhmmss");
            dcSetup.TransfersFromHOUpdate = snow;
            dcSetup.resetTransfersFromHoRange();

        }


        public void getRetailTransfersNZ(SqlConnection ersConnection)
        {
            Logging.WriteLog("Starting getRetailTransfersNZ");

            String lastUpdate = dcSetup.TransfersFromHONZUpdate;
            if (lastUpdate.Equals("0") || lastUpdate.Equals(""))
            {
                String iiSql = "delete from " + DaasExportTable + " where DAAS_SET_NAME = 'TRANSFERS_HONZ' ";
                using (SqlCommand cmd = new SqlCommand(iiSql, ersConnection))
                {
                    cmd.ExecuteNonQuery();
                }
            }

            if (lastUpdate.Length >= 8)
            {
                lastUpdate = lastUpdate.Substring(0, 8);
            }
            int iLastUpdate = Logging.strToIntDef(lastUpdate, 0);
            int initialdate = dcSetup.TransfersFromHONZInitialDate;

            int dateFrom = dcSetup.TransfersFromHONZFromDate;
            int dateTo = dcSetup.TransfersFromHONZToDate;


            String top10 = " ";
            if (dcSetup.ResultSet > 0)
            {
                top10 = " top " + dcSetup.ResultSet.ToString();
            }

            if (dateFrom > 0 || dateTo > 0)
            {
                String rSql = "delete from " + DaasExportTable + " where DAAS_SET_NAME = 'TRANSFERS_HONZ' and DAAS_KEY4 >= @mindate and DAAS_KEY4 <= @maxDate  ";
                using (SqlCommand cmd = new SqlCommand(rSql, ersConnection))
                {
                    cmd.Parameters.AddWithValue("@mindate", dateFrom.ToString());
                    cmd.Parameters.AddWithValue("@maxDate", dateTo.ToString());
                    cmd.ExecuteNonQuery();
                }

            }


            String anSql = "select " + top10 + " LFS_ORIGNR, LFS_ANG_ANR, LFS_LFS, cast(LFS_ORIGNR as varchar) + '-' +  cast(LFS_ANG_ANR as varchar) + '-' + cast(LFS_LFS as varchar) [TRANSFER_NO], " +
                " LFS_VONNR[FROM_STORE], LFS_KNR[TO_STORE], LFS_CLOG_DATE, LFS_DATLFS, LFS_ULOG_DATE, " +
                " case  " +
                "   when LFS_STATUS = 0 then 'Standard delivery note' " +
                "   when LFS_STATUS = 1 then 'Delivery note being delivered' " +
                "   when LFS_STATUS = 2 then 'Delivery note was delivered' " +
                "   when LFS_STATUS = 3 then 'Delivery note back from delivery (branch)' " +
                "   else '' " +
                " end[LFS_STATUS], LFS_CLOG_USER " +
                " FROM V_LIEFHEAD " +
                " where " +
                //" LFS_KTYP = 6 AND LFS_STATUS = 2 and " + 
                " LFS_KTYP = 2 and ((LFS_ULOG_DATE >= " + iLastUpdate + " and LFS_ULOG_DATE >= " + initialdate + ") or (LFS_CLOG_DATE >= " + iLastUpdate + " and LFS_CLOG_DATE >= " + initialdate + ") or (LFS_ULOG_DATE > 0 and LFS_ULOG_DATE >= " + dateFrom + " and LFS_ULOG_DATE <= " + dateTo + "  )  )";
            Logging.WriteDebug(anSql, dcSetup.Debug);
            using (SqlCommand cmd = new SqlCommand(anSql, ersConnection))
            {
                cmd.CommandTimeout = 1200;
                using (SqlDataReader areader = cmd.ExecuteReader())
                {
                    while (areader.Read())
                    {
                        RTransfersJson transferJson = new RTransfersJson();
                        TransferHeader transfer = new TransferHeader();
                        transferJson.TransferFromHO = transfer;

                        transfer.CreationDate = Convert.ToInt32(areader["LFS_CLOG_DATE"]);
                        transfer.DespatchDate = Convert.ToInt32(areader["LFS_DATLFS"]);
                        transfer.FromStore = Convert.ToInt32(areader["FROM_STORE"]);
                        transfer.RequestUser = Convert.ToInt32(areader["LFS_CLOG_USER"]);
                        transfer.ToStore = Convert.ToInt32(areader["TO_STORE"]);
                        transfer.TrasferNo = areader["TRANSFER_NO"].ToString();
                        transfer.Details = new List<TransferDetails>();

                        int origin = Convert.ToInt32(areader["LFS_ORIGNR"]);
                        int andAnr = Convert.ToInt32(areader["LFS_ANG_ANR"]);
                        int lfs = Convert.ToInt32(areader["LFS_LFS"]);

                        String trSql = "select  LZL_ZNR, LZL_REFNR[SKU_ID], LZL_MENGE [QTY] , " +
                            " CASE " +
                            "   WHEN LZL_STAT_EKDM = 0 THEN LZL_EEK " +
                            "   ELSE LZL_STAT_EKDM " +
                            " END[COST], " +
                            " CASE " +
                            "   WHEN LZL_STAT_VKDM = 0 THEN LZL_EVKB " +
                            "   ELSE LZL_STAT_VKDM " +
                            " END[RT_PRICE] " +
                            " from LIEFZEIL " +
                            " where LZL_REFNR <> 0  AND LZL_ORIGNR = @LZL_ORIGNR AND LZL_ANG_ANR = @LZL_ANG_ANR AND LZL_LFS = @LZL_LFS ";
                        using (SqlCommand trCmd = new SqlCommand(trSql, ersConnection))
                        {
                            trCmd.Parameters.AddWithValue("@LZL_ORIGNR", origin);
                            trCmd.Parameters.AddWithValue("@LZL_ANG_ANR", andAnr);
                            trCmd.Parameters.AddWithValue("@LZL_LFS", lfs);
                            using(SqlDataReader trReader = trCmd.ExecuteReader())
                            {
                                while (trReader.Read())
                                {
                                    TransferDetails adetail = new TransferDetails();
                                    transfer.Details.Add(adetail);
                                    adetail.Cost = Logging.strToDoubleDef(trReader["COST"].ToString(), 0);
                                    adetail.LineNo = Convert.ToInt32(trReader["LZL_ZNR"]);
                                    adetail.Qty = Logging.strToDoubleDef(trReader["QTY"].ToString(), 0);
                                    adetail.RetailPrice = Logging.strToDoubleDef(trReader["RT_PRICE"].ToString(), 0);
                                    adetail.SkuId = Convert.ToInt32(trReader["SKU_ID"]);
                                    adetail.TransferNo = transfer.TrasferNo;
                                }
                            }
                        }

                        String ajsonStr = SimpleJson.SerializeObject(transferJson).ToString();
                        String md5Contents = Logging.CreateMD5(ajsonStr);


                        String storedMd5 = getMd5(origin.ToString(), andAnr.ToString(),
                            lfs.ToString(), areader["LFS_CLOG_DATE"].ToString(), "1", "TRANSFERS_HONZ", Logging.strToInt64Def(dcSetup.TransfersFromHOUpdate, 0), ersConnection);

                        if (!md5Contents.Equals(storedMd5))
                        {
                            Logging.WriteDebug("JSON " + SimpleJson.SerializeObject(transferJson).ToString(), dcSetup.Debug);
                            SendNewMessageQueue(SimpleJson.SerializeObject(transferJson).ToString(), dcSetup.TransfersFromHOQueueName);
                            updateDaasExport(origin.ToString(), andAnr.ToString(), lfs.ToString(), areader["LFS_CLOG_DATE"].ToString(), "1", "TRANSFERS_HONZ", md5Contents, ersConnection);
                        }


                    }

                }
            }


            DateTime anow = DateTime.Now;
            String snow = anow.ToString("yyyyMMddhhmmss");
            dcSetup.TransfersFromHONZUpdate = snow;
            dcSetup.resetTransfersFromHoNZRange();

        }

        public void getInventoryAdjustmentsNZ(SqlConnection ersNZConnection)
        {
            Logging.WriteLog("Starting getInventoryAdjustmentsNZ");
            String lastUpdate = dcSetup.InventoryAdjustmentsNZUpdate;

            if (lastUpdate.Equals("0") || lastUpdate.Equals(""))
            {
                String iiSql = "delete from " + DaasExportTable + " where DAAS_SET_NAME = 'I_ADJUSTMENTNZ' ";
                using (SqlCommand cmd = new SqlCommand(iiSql, ersNZConnection))
                {
                    cmd.ExecuteNonQuery();
                }
            }

            if (lastUpdate.Length >= 8)
            {
                lastUpdate = lastUpdate.Substring(0, 8);
            }

            String top10 = " ";
            if (dcSetup.ResultSet > 0)
            {
                top10 = " top " + dcSetup.ResultSet.ToString();
            }

            int iLastUpdate = Logging.strToIntDef(lastUpdate, 0);
            int initialdate = dcSetup.InventoryAdjustmentsNZInitialDate;

            int dateFrom = dcSetup.InventoryAdjustmentNZFromDate;
            int dateTo = dcSetup.InventoryAdjustmentNZToDate;

            if (dateFrom == 0)
            {
                dateFrom = 20170101;
            }

            if (dateFrom != 0 || dateTo != 0)
            {
                String rSql = "delete from " + DaasExportTable + " where DAAS_SET_NAME = 'I_ADJUSTMENTNZ' and DAAS_KEY3 >= @mindate and DAAS_KEY3 <= @maxDate  ";
                using (SqlCommand cmd = new SqlCommand(rSql, ersNZConnection))
                {
                    cmd.Parameters.AddWithValue("@mindate", dateFrom.ToString());
                    cmd.Parameters.AddWithValue("@maxDate", dateTo.ToString());
                    cmd.ExecuteNonQuery();
                }

            }

            String anSql = "select " + top10 + " LKO_REFNUMMER, LKO_FILIALE, LKO_DATUM, LKO_UNIQUE, LKO_GRUND, LKO_TEXT, LKO_MENGE, LKO_ULOG_USER, ART_VKPREIS[RT_Price], ART_GRPNUMMER,  " +
                " case  " +
                "   when ART_SET_EKGEW_MODE <> 0 then ART_EK_GEWICHTET " +
                "   else ART_EK_DM " +
                " end[WeightedAverageCost] " +
                " , ISNULL(ILG_TEXT, '') [REASON] " +
                " from V_LAGERKOR " +
                " LEFT JOIN V_INVLKGRD ON ILG_GRUND = LKO_GRUND " +
                " JOIN V_ARTIKEL ON ART_REFNUMMER = LKO_REFNUMMER " +
                " WHERE ((LKO_DATUM >= " + initialdate + " AND LKO_DATUM >= " + iLastUpdate + ") or (LKO_DATUM >= " + dateFrom + " and LKO_DATUM <= " + dateTo + " )) " +
                " order by 3 ";

            Logging.WriteDebug(anSql, dcSetup.Debug);

            using (SqlCommand cmd = new SqlCommand(anSql, ersNZConnection))
            {
                cmd.CommandTimeout = 1200;
                using (SqlDataReader areader = cmd.ExecuteReader())
                {
                    while (areader.Read())
                    {
                        InventoryAdjustmentJson ajsonObj = new InventoryAdjustmentJson();
                        ajsonObj.InventoryAdjustmentLine = new InventoryAdjustment();
                        ajsonObj.InventoryAdjustmentLine.AdjustmentDate = Convert.ToInt32(areader["LKO_DATUM"]);
                        ajsonObj.InventoryAdjustmentLine.AdjustmentNumber = areader["LKO_REFNUMMER"].ToString() + "-" + areader["LKO_FILIALE"].ToString() + "-" + areader["LKO_DATUM"].ToString() +
                            "-" + areader["LKO_UNIQUE"].ToString();
                        ajsonObj.InventoryAdjustmentLine.AdjustmentQty = Logging.strToDoubleDef(areader["LKO_MENGE"].ToString(), 0);
                        ajsonObj.InventoryAdjustmentLine.AdjustmentReason = areader["REASON"].ToString();
                        ajsonObj.InventoryAdjustmentLine.AdjustmentReasonId = Convert.ToInt32(areader["LKO_GRUND"].ToString());
                        ajsonObj.InventoryAdjustmentLine.BranchNo = Convert.ToInt32(areader["LKO_FILIALE"].ToString());
                        ajsonObj.InventoryAdjustmentLine.EmployeeId = Convert.ToInt32(areader["LKO_ULOG_USER"].ToString());
                        ajsonObj.InventoryAdjustmentLine.RetailPrice = Logging.strToDoubleDef(areader["RT_Price"].ToString(), 0);
                        ajsonObj.InventoryAdjustmentLine.SkuId = Convert.ToInt32(areader["LKO_REFNUMMER"]);
                        ajsonObj.InventoryAdjustmentLine.UniqueNo = Convert.ToInt32(areader["LKO_UNIQUE"]);
                        ajsonObj.InventoryAdjustmentLine.WeightedAverageCost = Logging.strToDoubleDef(areader["WeightedAverageCost"].ToString(), 0);
                        ajsonObj.InventoryAdjustmentLine.AdjustmentText = areader["LKO_TEXT"].ToString();

                        ajsonObj.InventoryAdjustmentLine.GroupNo = Logging.strToIntDef(areader["ART_GRPNUMMER"].ToString(), 0);

                        String ajsonStr = SimpleJson.SerializeObject(ajsonObj).ToString();
                        String md5Contents = Logging.CreateMD5(ajsonStr);


                        String storedMd5 = getMd5(ajsonObj.InventoryAdjustmentLine.BranchNo.ToString(), ajsonObj.InventoryAdjustmentLine.SkuId.ToString(),
                            ajsonObj.InventoryAdjustmentLine.AdjustmentDate.ToString(), ajsonObj.InventoryAdjustmentLine.UniqueNo.ToString(), "1", "I_ADJUSTMENTNZ",
                            Logging.strToInt64Def(dcSetup.InventoryAdjustmentsUpdate, 0),
                            ersNZConnection);

                        if (!md5Contents.Equals(storedMd5))
                        {
                            Logging.WriteDebug("JSON " + SimpleJson.SerializeObject(ajsonObj).ToString(), dcSetup.Debug);
                            SendNewMessageQueue(SimpleJson.SerializeObject(ajsonObj).ToString(), dcSetup.InventoryAdjustmentsQueueName);
                            updateDaasExport(ajsonObj.InventoryAdjustmentLine.BranchNo.ToString(), ajsonObj.InventoryAdjustmentLine.SkuId.ToString(),
                                ajsonObj.InventoryAdjustmentLine.AdjustmentDate.ToString(), ajsonObj.InventoryAdjustmentLine.UniqueNo.ToString(), "1", "I_ADJUSTMENTNZ",
                            md5Contents, ersNZConnection);
                        }


                    }
                }

            }

            DateTime anow = DateTime.Now;
            String snow = anow.ToString("yyyyMMddhhmmss");
            dcSetup.InventoryAdjustmentsNZUpdate = snow;
            dcSetup.resetInventoryAdjustmentNZDateRange();

        }

        public void getInventoryAdjustments(SqlConnection ersConnection)
        {
            Logging.WriteLog("Starting getInventoryAdjustments");
            String lastUpdate = dcSetup.InventoryAdjustmentsUpdate;

            if (lastUpdate.Equals("0") || lastUpdate.Equals(""))
            {
                String iiSql = "delete from " + DaasExportTable + " where DAAS_SET_NAME = 'I_ADJUSTMENT' ";
                using (SqlCommand cmd = new SqlCommand(iiSql, ersConnection))
                {
                    cmd.ExecuteNonQuery();
                }
            }

            if (lastUpdate.Length >= 8)
            {
                lastUpdate = lastUpdate.Substring(0, 8);
            }

            String top10 = " ";
            if (dcSetup.ResultSet > 0)
            {
                top10 = " top " + dcSetup.ResultSet.ToString();
            }

            int iLastUpdate = Logging.strToIntDef(lastUpdate, 0);
            int initialdate = dcSetup.InventoryAdjustmentsInitialDate;

            int dateFrom = dcSetup.InventoryAdjustmentFromDate;
            int dateTo = dcSetup.InventoryAdjustmentToDate;

            if (dateFrom == 0)
            {
                dateFrom = 20170101;
            }

            if (dateFrom != 0 || dateTo != 0)
            {
                String rSql = "delete from " + DaasExportTable + " where DAAS_SET_NAME = 'I_ADJUSTMENT' and DAAS_KEY3 >= @mindate and DAAS_KEY3 <= @maxDate  ";
                using (SqlCommand cmd = new SqlCommand(rSql, ersConnection))
                {
                    cmd.Parameters.AddWithValue("@mindate", dateFrom.ToString());
                    cmd.Parameters.AddWithValue("@maxDate", dateTo.ToString());
                    cmd.ExecuteNonQuery();
                }

            }

            String anSql = "select " + top10 + " LKO_REFNUMMER, LKO_FILIALE, LKO_DATUM, LKO_UNIQUE, LKO_GRUND, LKO_TEXT, LKO_MENGE, LKO_ULOG_USER, ART_VKPREIS[RT_Price], ART_GRPNUMMER,  " +
                " case  " +
                "   when ART_SET_EKGEW_MODE <> 0 then ART_EK_GEWICHTET " +
                "   else ART_EK_DM " +
                " end[WeightedAverageCost] " +
                " , ISNULL(ILG_TEXT, '') [REASON] " + 
                " from V_LAGERKOR " +
                " LEFT JOIN V_INVLKGRD ON ILG_MANDANT = 1 AND ILG_GRUND = LKO_GRUND " + 
                " JOIN V_ARTIKEL ON ART_MANDANT = 1 AND ART_REFNUMMER = LKO_REFNUMMER " +
                " WHERE LKO_MANDANT = 1 AND ((LKO_DATUM >= " + initialdate + " AND LKO_DATUM >= " + iLastUpdate + ") or (LKO_DATUM >= " + dateFrom + " and LKO_DATUM <= " + dateTo + " )) " +
                " order by 3 ";

            Logging.WriteDebug(anSql, dcSetup.Debug);

            using (SqlCommand cmd = new SqlCommand(anSql, ersConnection))
            {
                cmd.CommandTimeout = 1200;
                using (SqlDataReader areader = cmd.ExecuteReader())
                {
                    while(areader.Read())
                    {
                        InventoryAdjustmentJson ajsonObj = new InventoryAdjustmentJson();
                        ajsonObj.InventoryAdjustmentLine = new InventoryAdjustment();
                        ajsonObj.InventoryAdjustmentLine.AdjustmentDate = Convert.ToInt32(areader["LKO_DATUM"]);
                        ajsonObj.InventoryAdjustmentLine.AdjustmentNumber = areader["LKO_REFNUMMER"].ToString() + "-" + areader["LKO_FILIALE"].ToString() + "-" + areader["LKO_DATUM"].ToString() +
                            "-" + areader["LKO_UNIQUE"].ToString();
                        ajsonObj.InventoryAdjustmentLine.AdjustmentQty = Logging.strToDoubleDef(areader["LKO_MENGE"].ToString(), 0);
                        ajsonObj.InventoryAdjustmentLine.AdjustmentReason = areader["REASON"].ToString();
                        ajsonObj.InventoryAdjustmentLine.AdjustmentReasonId = Convert.ToInt32(areader["LKO_GRUND"].ToString());
                        ajsonObj.InventoryAdjustmentLine.BranchNo = Convert.ToInt32(areader["LKO_FILIALE"].ToString());
                        ajsonObj.InventoryAdjustmentLine.EmployeeId = Convert.ToInt32(areader["LKO_ULOG_USER"].ToString());
                        ajsonObj.InventoryAdjustmentLine.RetailPrice = Logging.strToDoubleDef(areader["RT_Price"].ToString(), 0);
                        ajsonObj.InventoryAdjustmentLine.SkuId = Convert.ToInt32(areader["LKO_REFNUMMER"]);
                        ajsonObj.InventoryAdjustmentLine.UniqueNo = Convert.ToInt32(areader["LKO_UNIQUE"]);
                        ajsonObj.InventoryAdjustmentLine.WeightedAverageCost = Logging.strToDoubleDef(areader["WeightedAverageCost"].ToString(), 0);
                        ajsonObj.InventoryAdjustmentLine.AdjustmentText = areader["LKO_TEXT"].ToString();

                        ajsonObj.InventoryAdjustmentLine.GroupNo =   Logging.strToIntDef(areader["ART_GRPNUMMER"].ToString(), 0);

                        String ajsonStr = SimpleJson.SerializeObject(ajsonObj).ToString();
                        String md5Contents = Logging.CreateMD5(ajsonStr);


                        String storedMd5 = getMd5(ajsonObj.InventoryAdjustmentLine.BranchNo.ToString(), ajsonObj.InventoryAdjustmentLine.SkuId.ToString(),
                            ajsonObj.InventoryAdjustmentLine.AdjustmentDate.ToString(), ajsonObj.InventoryAdjustmentLine.UniqueNo.ToString(), "1", "I_ADJUSTMENT", 
                            Logging.strToInt64Def(dcSetup.InventoryAdjustmentsUpdate, 0),
                            ersConnection);

                        if (!md5Contents.Equals(storedMd5))
                        {
                            Logging.WriteDebug("JSON " + SimpleJson.SerializeObject(ajsonObj).ToString(), dcSetup.Debug);
                            SendNewMessageQueue(SimpleJson.SerializeObject(ajsonObj).ToString(), dcSetup.InventoryAdjustmentsQueueName);
                            updateDaasExport(ajsonObj.InventoryAdjustmentLine.BranchNo.ToString(), ajsonObj.InventoryAdjustmentLine.SkuId.ToString(),
                                ajsonObj.InventoryAdjustmentLine.AdjustmentDate.ToString(), ajsonObj.InventoryAdjustmentLine.UniqueNo.ToString(), "1", "I_ADJUSTMENT", 
                            md5Contents, ersConnection);
                        }


                    }
                }

            }

            DateTime anow = DateTime.Now;
            String snow = anow.ToString("yyyyMMddhhmmss");
            dcSetup.InventoryAdjustmentsUpdate = snow;
            dcSetup.resetInventoryAdjustmentDateRange();

        }

        private Dictionary<String, StockTransferLine> PopulateStockTransit(SqlConnection ersConnection)
        {
            Dictionary<String, StockTransferLine> adict = new Dictionary<string, StockTransferLine>();

            String anSql = "SELECT FTK_AN_NUMMER, FTR_REFNUMMER,  max(FTK_LIEFERDATUM) [FTK_LIEFERDATUM] , sum(FTR_ANZAHL) [FTR_ANZAHL]  FROM FTR_KOPF " +
                " JOIN FTR_DATA ON FTR_MANDANT = 1 AND FTR_FILIALE = FTK_FILIALE AND FTR_KASSE = FTK_KASSE AND FTK_NUMMER = FTR_NUMMER " +
                " WHERE FTK_TYP = 2 AND FTK_MANDANT = 1 and FTR_ANZAHL <> 0 and FTR_REFNUMMER <> 0 " +
                " group by FTK_AN_NUMMER, FTR_REFNUMMER " + 
                " ORDER BY 1, 2, 3 ";
            using (SqlCommand cmd = new SqlCommand(anSql, ersConnection))
            {
                using(SqlDataReader areader = cmd.ExecuteReader())
                {
                    while (areader.Read())
                    {
                        StockTransferLine aline = new StockTransferLine();
                        String akey = areader["FTK_AN_NUMMER"].ToString() + "~" + areader["FTR_REFNUMMER"].ToString();
                        aline.BranchNo = Convert.ToInt32(areader["FTK_AN_NUMMER"]);
                        aline.Sku = Convert.ToInt32(areader["FTR_REFNUMMER"]);
                        aline.Qty = Logging.strToDoubleDef(areader["FTR_ANZAHL"].ToString(), 0);
                        aline.TransferDate = Convert.ToInt32(areader["FTK_LIEFERDATUM"]);

                        adict.Add(akey, aline);
                    }
                }
            }
            return adict;

        }

        public void getInventory(SqlConnection ersConnection)
        {
            Logging.WriteLog("Starting getInventory");
            String lastUpdate = dcSetup.InventoryUpdate;

            if (lastUpdate.Equals("0") || lastUpdate.Equals(""))
            {
                String iiSql = "delete from " + DaasExportTable + " where DAAS_SET_NAME = 'INVENTORY' ";
                using (SqlCommand cmd = new SqlCommand(iiSql, ersConnection))
                {
                    cmd.ExecuteNonQuery();
                }
            }

            String top10 = " ";
            if (dcSetup.ResultSet > 0)
            {
                top10 = " top " + dcSetup.ResultSet.ToString();
            }



            if (lastUpdate.Length >= 8)
            {
                lastUpdate = lastUpdate.Substring(0, 8); 
            }

            Dictionary<String, StockTransferLine> stockTransitLst = PopulateStockTransit(ersConnection);


            String anSql = "select  LAG_REFNUMMER, LAG_FILIALE, " +

                " CASE " +
                "   WHEN LAG_EK_GEW_VALID = 1 THEN LAG_EK_GEWICHTET " +
                "   WHEN ART_EK_GEW_VALID = 1 THEN ART_EK_GEWICHTET " +
                "   ELSE ART_EK_DM " +
                " END[LAG_EK_GEWICHTET], " +
                " CASE " +
                "    WHEN LAG_LETZTEREINGANG > LAG_LETZTERVERKAUF THEN LAG_LETZTEREINGANG " +
                "    ELSE LAG_LETZTERVERKAUF " +
                " END[UPDATED_DATE], " +

                " LAG_BESTAND  + ISNULL(LGD_DELTA, 0)[LAG_BESTAND], LAG_CLOG_DATE  " +
                " FROM V_LAGER " +
                " JOIN ARTIKEL ON ART_MANDANT = 1 AND ART_REFNUMMER = LAG_REFNUMMER " + 
                " LEFT JOIN (SELECT LGD_REFNUMMER, LGD_FILIALE, SUM(LGD_DELTA)[LGD_DELTA] FROM  V_LAGDELTA WHERE LGD_MANDANT = 1 AND LGD_TYP <> 2 GROUP BY  LGD_REFNUMMER, LGD_FILIALE)TBL " + 
                "    ON LGD_REFNUMMER = LAG_REFNUMMER AND LGD_FILIALE = LAG_FILIALE " + 
                " WHERE LAG_MANDANT = 1 "; 

            if (lastUpdate.Equals("0") || lastUpdate.Equals(""))
            {
                anSql = anSql + " and LAG_BESTAND  + ISNULL(LGD_DELTA, 0) <> 0 ";
            } else
            {
                anSql = anSql + " and (LAG_CLOG_DATE >= " + lastUpdate + " OR LAG_LETZTEREINGANG  >= " + lastUpdate + " OR LAG_LETZTERVERKAUF  >= " + lastUpdate + ") ";
            }

            Logging.WriteDebug(anSql, dcSetup.Debug);

            using (SqlCommand cmd = new SqlCommand(anSql, ersConnection))
            {
                cmd.CommandTimeout = 1200;

                using (SqlDataReader areader = cmd.ExecuteReader())
                {
                    while(areader.Read())
                    {
                        InventoryJson inventoryJson = new InventoryJson();
                        InventoryItem inventoryLine = new InventoryItem();
                        inventoryJson.InventoryLine = inventoryLine;

                        inventoryLine.Sku = Convert.ToInt32(areader["LAG_REFNUMMER"].ToString());



                      //  inventoryLine.Inventorydate = Convert.ToInt32(areader["LAG_CLOG_DATE"]);
                        inventoryLine.Inventorydate = Convert.ToInt32(areader["UPDATED_DATE"]);
                        inventoryLine.StockOnHand = Logging.strToDoubleDef(areader["LAG_BESTAND"].ToString(), 0);
                    //    inventoryLine.StockonTransit = 0;
                        inventoryLine.WeightedAverageCost = Logging.strToDoubleDef(areader["LAG_EK_GEWICHTET"].ToString(), 0);
                        inventoryLine.BranchNo = Logging.strToIntDef(areader["LAG_FILIALE"].ToString(), 0);

                        String ajsonStr = SimpleJson.SerializeObject(inventoryJson).ToString();
                        String md5Contents = Logging.CreateMD5(ajsonStr);

                        String aKey = inventoryLine.BranchNo.ToString() + "~" + inventoryLine.Sku.ToString();
                        inventoryLine.StockInTransit = 0;
                        if (stockTransitLst.ContainsKey(aKey))
                        {
                            StockTransferLine aline = stockTransitLst[aKey];
                            aline.Taken = true;
                            if(aline.TransferDate >= Logging.strToIntDef(lastUpdate, 0))
                            {
                                inventoryLine.StockInTransit = aline.Qty;
                            }

                        }

                        if(inventoryLine.Inventorydate == 0)
                        {
                            DateTime anowy = DateTime.Now;
                            String snowy = anowy.ToString("yyyyMMdd");
                            inventoryLine.Inventorydate = Convert.ToInt32(snowy);

                        }

                        String storedMd5 = getMd5(inventoryLine.BranchNo.ToString(), inventoryLine.Sku.ToString(), inventoryLine.Inventorydate.ToString(), "1", "1", "INVENTORY", Logging.strToInt64Def(dcSetup.InventoryUpdate, 0),
                            ersConnection);

                        if (!md5Contents.Equals(storedMd5))
                        {
                            Logging.WriteDebug("JSON " + SimpleJson.SerializeObject(inventoryJson).ToString(), dcSetup.Debug);
                            SendNewMessageQueue(SimpleJson.SerializeObject(inventoryJson).ToString(), dcSetup.InventoryQueueName);
                            updateDaasExport(inventoryLine.BranchNo.ToString(), inventoryLine.Sku.ToString(), inventoryLine.Inventorydate.ToString(), "1", "1", "INVENTORY", md5Contents, ersConnection);
                        }


                    }
                }

                foreach(var atrLine in stockTransitLst.Values)
                {
                    if(atrLine.Taken)
                    {
                        continue;
                    }
                    if(atrLine.TransferDate < Logging.strToIntDef(lastUpdate, 0))
                    {
                        continue;
                    }
                    InventoryJson inventoryJson = new InventoryJson();
                    InventoryItem inventoryLine = new InventoryItem();
                    inventoryJson.InventoryLine = inventoryLine;
                    inventoryLine.Sku = atrLine.Sku;
                    inventoryLine.Inventorydate = atrLine.TransferDate;
                    inventoryLine.StockOnHand = 0;
                    inventoryLine.StockInTransit = atrLine.Qty;
                    inventoryLine.BranchNo = atrLine.BranchNo;
                    inventoryLine.WeightedAverageCost = 0;

                    String MORESql = "select LAG_REFNUMMER, LAG_FILIALE, " +

                    " CASE " +
                    "   WHEN LAG_EK_GEW_VALID = 1 THEN LAG_EK_GEWICHTET " +
                    "   WHEN ART_EK_GEW_VALID = 1 THEN ART_EK_GEWICHTET " +
                    "   ELSE ART_EK_DM " +
                    " END[LAG_EK_GEWICHTET], " +

                    " LAG_BESTAND  + ISNULL(LGD_DELTA, 0)[LAG_BESTAND], LAG_CLOG_DATE  " +
                    " FROM V_LAGER " +
                    " JOIN ARTIKEL ON ART_MANDANT = 1 AND ART_REFNUMMER = LAG_REFNUMMER " +
                    " LEFT JOIN (SELECT LGD_REFNUMMER, LGD_FILIALE, SUM(LGD_DELTA)[LGD_DELTA] FROM  V_LAGDELTA WHERE LGD_MANDANT = 1 AND LGD_TYP <> 2 GROUP BY  LGD_REFNUMMER, LGD_FILIALE)TBL " +
                    "    ON LGD_REFNUMMER = LAG_REFNUMMER AND LGD_FILIALE = LAG_FILIALE " +
                    " WHERE LAG_MANDANT = 1 AND LAG_REFNUMMER = " + atrLine.Sku + " AND LAG_FILIALE = " + atrLine.BranchNo ;
                    using(SqlCommand cmdM = new SqlCommand(MORESql, ersConnection))
                    {
                        using(SqlDataReader areader = cmdM.ExecuteReader())
                        {
                            if(areader.Read())
                            {
                                inventoryLine.StockOnHand = Logging.strToDoubleDef(areader["LAG_BESTAND"].ToString(), 0);
                                inventoryLine.WeightedAverageCost = Logging.strToDoubleDef(areader["LAG_EK_GEWICHTET"].ToString(), 0);

                            }
                        }
                    }



                    String ajsonStr = SimpleJson.SerializeObject(inventoryJson).ToString();
                    String md5Contents = Logging.CreateMD5(ajsonStr);

                    String storedMd5 = getMd5(inventoryLine.BranchNo.ToString(), inventoryLine.Sku.ToString(), inventoryLine.Inventorydate.ToString(), "1", "1", "INVENTORY", Logging.strToInt64Def(dcSetup.InventoryUpdate, 0),
                        ersConnection);

                    if (!md5Contents.Equals(storedMd5))
                    {
                        Logging.WriteDebug("JSON " + SimpleJson.SerializeObject(inventoryJson).ToString(), dcSetup.Debug);
                        SendNewMessageQueue(SimpleJson.SerializeObject(inventoryJson).ToString(), dcSetup.InventoryQueueName);
                        updateDaasExport(inventoryLine.BranchNo.ToString(), inventoryLine.Sku.ToString(), inventoryLine.Inventorydate.ToString(), "1", "1", "INVENTORY", md5Contents, ersConnection);
                    }



                }


                DateTime anow = DateTime.Now;
                String snow = anow.ToString("yyyyMMddhhmmss");
                dcSetup.InventoryUpdate = snow;


            }


        }

        int getKAS_ZEIT(SqlConnection ersConnection, int kasMandant, int kasDatum, int kasFiliale, int kasKasse, int kasBonnr)
        {

            String ansql = "select KAS_ZEIT from V_KASSE  " +
                 " where KAS_DATUM = @KAS_DATUM and KAS_FILIALE = @KAS_FILIALE " +
                 " and KAS_KASSE = @KAS_KASSE and  KAS_BONNR = @KAS_BONNR and KAS_MANDANT = @KAS_MANDANT ";

            using( SqlCommand cmd = new SqlCommand(ansql, ersConnection) )
            {
                cmd.Parameters.AddWithValue("@KAS_DATUM", kasDatum);
                cmd.Parameters.AddWithValue("@KAS_FILIALE", kasFiliale);
                cmd.Parameters.AddWithValue("@KAS_KASSE", kasKasse);
                cmd.Parameters.AddWithValue("@KAS_BONNR", kasBonnr);
                cmd.Parameters.AddWithValue("@KAS_MANDANT", kasMandant);
                using(SqlDataReader areader = cmd.ExecuteReader())
                {
                    if(areader.Read())
                    {
                        return areader.GetInt32(0);
                    }
                }

            }

            ansql = "select KAS_ZEIT from V_KASSTRNS  " +
                " where KAS_DATUM = @KAS_DATUM and KAS_FILIALE = @KAS_FILIALE " +
                " and KAS_KASSE = @KAS_KASSE and  KAS_BONNR = @KAS_BONNR and KAS_MANDANT = @KAS_MANDANT ";

            using (SqlCommand cmd = new SqlCommand(ansql, ersConnection))
            {
                cmd.Parameters.AddWithValue("@KAS_DATUM", kasDatum);
                cmd.Parameters.AddWithValue("@KAS_FILIALE", kasFiliale);
                cmd.Parameters.AddWithValue("@KAS_KASSE", kasKasse);
                cmd.Parameters.AddWithValue("@KAS_BONNR", kasBonnr);
                cmd.Parameters.AddWithValue("@KAS_MANDANT", kasMandant);
                using (SqlDataReader areader = cmd.ExecuteReader())
                {
                    if (areader.Read())
                    {
                        return areader.GetInt32(0);
                    }
                }

            }

            ansql = "select KAS_ZEIT from V_KASIDLTA  " +
                " where KAS_DATUM = @KAS_DATUM and KAS_FILIALE = @KAS_FILIALE " +
                " and KAS_KASSE = @KAS_KASSE and  KAS_BONNR = @KAS_BONNR and KAS_MANDANT = @KAS_MANDANT ";

            using (SqlCommand cmd = new SqlCommand(ansql, ersConnection))
            {
                cmd.Parameters.AddWithValue("@KAS_DATUM", kasDatum);
                cmd.Parameters.AddWithValue("@KAS_FILIALE", kasFiliale);
                cmd.Parameters.AddWithValue("@KAS_KASSE", kasKasse);
                cmd.Parameters.AddWithValue("@KAS_BONNR", kasBonnr);
                cmd.Parameters.AddWithValue("@KAS_MANDANT", kasMandant);
                using (SqlDataReader areader = cmd.ExecuteReader())
                {
                    if (areader.Read())
                    {
                        return areader.GetInt32(0);
                    }
                }

            }



            return 0;
        }


        int getKAS_ZEITNZ(SqlConnection ersConnection, int kasDatum, int kasFiliale, int kasKasse, int kasBonnr)
        {

            String ansql = "select KAS_ZEIT from V_KASSE  " +
                 " where KAS_DATUM = @KAS_DATUM and KAS_FILIALE = @KAS_FILIALE " +
                 " and KAS_KASSE = @KAS_KASSE and  KAS_BONNR = @KAS_BONNR  ";

            using (SqlCommand cmd = new SqlCommand(ansql, ersConnection))
            {
                cmd.Parameters.AddWithValue("@KAS_DATUM", kasDatum);
                cmd.Parameters.AddWithValue("@KAS_FILIALE", kasFiliale);
                cmd.Parameters.AddWithValue("@KAS_KASSE", kasKasse);
                cmd.Parameters.AddWithValue("@KAS_BONNR", kasBonnr);
                using (SqlDataReader areader = cmd.ExecuteReader())
                {
                    if (areader.Read())
                    {
                        return areader.GetInt32(0);
                    }
                }

            }

            ansql = "select KAS_ZEIT from V_KASSTRNS  " +
                " where KAS_DATUM = @KAS_DATUM and KAS_FILIALE = @KAS_FILIALE " +
                " and KAS_KASSE = @KAS_KASSE and  KAS_BONNR = @KAS_BONNR ";

            using (SqlCommand cmd = new SqlCommand(ansql, ersConnection))
            {
                cmd.Parameters.AddWithValue("@KAS_DATUM", kasDatum);
                cmd.Parameters.AddWithValue("@KAS_FILIALE", kasFiliale);
                cmd.Parameters.AddWithValue("@KAS_KASSE", kasKasse);
                cmd.Parameters.AddWithValue("@KAS_BONNR", kasBonnr);
                using (SqlDataReader areader = cmd.ExecuteReader())
                {
                    if (areader.Read())
                    {
                        return areader.GetInt32(0);
                    }
                }

            }

            ansql = "select KAS_ZEIT from V_KASIDLTA  " +
                " where KAS_DATUM = @KAS_DATUM and KAS_FILIALE = @KAS_FILIALE " +
                " and KAS_KASSE = @KAS_KASSE and  KAS_BONNR = @KAS_BONNR  ";

            using (SqlCommand cmd = new SqlCommand(ansql, ersConnection))
            {
                cmd.Parameters.AddWithValue("@KAS_DATUM", kasDatum);
                cmd.Parameters.AddWithValue("@KAS_FILIALE", kasFiliale);
                cmd.Parameters.AddWithValue("@KAS_KASSE", kasKasse);
                cmd.Parameters.AddWithValue("@KAS_BONNR", kasBonnr);
                using (SqlDataReader areader = cmd.ExecuteReader())
                {
                    if (areader.Read())
                    {
                        return areader.GetInt32(0);
                    }
                }

            }



            return 0;
        }



        public void salesSecondPass(SqlConnection ersConnection)
        {
            Logging.WriteLog("salesSecondPass");
            String startingDate = dcSetup.MinSendDate.ToString();
            String top10 = " ";
            if (dcSetup.ResultSet > 0)
            {
                top10 = " top " + dcSetup.ResultSet.ToString();
            }

            int dayDiff = dcSetup.WacIntervalDays;

            String anSql = "SELECT " + top10 + " DAAS_KEY1, DAAS_KEY2, DAAS_KEY3, DAAS_KEY4, DAAS_KEY5 " +
                " FROM " + DaasExportTable  +
                " where DAAS_SET_NAME = 'SALE' AND datediff(DAY, convert(DATETIME, DAAS_KEY1, 112), convert(DATETIME, SUBSTRING(CAST(DAAS_UPDATE_TIME AS VARCHAR), 1, 8), 112)  ) < " + dayDiff +
                " AND DATEDIFF(DAY, convert(DATETIME, DAAS_KEY1, 112), GETDATE()) > " + dayDiff;
            Logging.WriteDebug("salesSecondPass Sql: ", dcSetup.Debug);
            Logging.WriteDebug(anSql, dcSetup.Debug);

            using(SqlCommand cmd = new SqlCommand(anSql, ersConnection))
            {
                using (SqlDataReader areader = cmd.ExecuteReader())
                {
                    while (areader.Read())
                    {
                        int kDate = Logging.strToIntDef(areader["DAAS_KEY1"].ToString(), 0);
                        int kFiliale = Logging.strToIntDef(areader["DAAS_KEY2"].ToString(), 0);
                        int kasse = Logging.strToIntDef(areader["DAAS_KEY4"].ToString(), 0);
                        int kbonir = Logging.strToIntDef(areader["DAAS_KEY5"].ToString(), 0);
                        int kMandant = Logging.strToIntDef(areader["DAAS_KEY3"].ToString(), 0);

                        int kazZeilt = getKAS_ZEIT(ersConnection, kMandant, kDate, kFiliale, kasse, kbonir);
                        String custNo = getCustNoFromTransaction(ersConnection, kDate, kFiliale, kasse, kbonir, kMandant);

                        processTransaction(ersConnection, kMandant, kDate, kFiliale, kasse, kbonir, kazZeilt, custNo);

                    }
                }
            }



        }


        public void salesSecondPassNZ(SqlConnection ersConnection)
        {
            Logging.WriteLog("salesSecondPass");
            String startingDate = dcSetup.MinSendDate.ToString();
            String top10 = " ";
            if (dcSetup.ResultSet > 0)
            {
                top10 = " top " + dcSetup.ResultSet.ToString();
            }

            int dayDiff = dcSetup.WacIntervalDays;

            String anSql = "SELECT " + top10 + " DAAS_KEY1, DAAS_KEY2, DAAS_KEY3, DAAS_KEY4  " +
                " FROM " + DaasExportTable +
                " where DAAS_SET_NAME = 'SALENZ' AND datediff(DAY, convert(DATETIME, DAAS_KEY1, 112), convert(DATETIME, SUBSTRING(CAST(DAAS_UPDATE_TIME AS VARCHAR), 1, 8), 112)  ) < " + dayDiff +
                " AND DATEDIFF(DAY, convert(DATETIME, DAAS_KEY1, 112), GETDATE()) > " + dayDiff;
            Logging.WriteDebug("salesSecondPass Sql: ", dcSetup.Debug);
            Logging.WriteDebug(anSql, dcSetup.Debug);

            using (SqlCommand cmd = new SqlCommand(anSql, ersConnection))
            {
                using (SqlDataReader areader = cmd.ExecuteReader())
                {
                    while (areader.Read())
                    {
                        int kDate = Logging.strToIntDef(areader["DAAS_KEY1"].ToString(), 0);
                        int kFiliale = Logging.strToIntDef(areader["DAAS_KEY2"].ToString(), 0);
                        int kasse = Logging.strToIntDef(areader["DAAS_KEY4"].ToString(), 0);
                        int kbonir = Logging.strToIntDef(areader["DAAS_KEY5"].ToString(), 0);
 
                        int kazZeilt = getKAS_ZEITNZ(ersConnection, kDate, kFiliale, kasse, kbonir);
                        String custNo = getCustNoFromTransactionNZ(ersConnection, kDate, kFiliale, kasse, kbonir);

                        processTransactionNZ(ersConnection, kDate, kFiliale, kasse, kbonir, kazZeilt, custNo);

                    }
                }
            }



        }



        public void getSales(SqlConnection ersConnection)
        {
            populateDiscount(ersConnection);
            Logging.WriteLog("Starting getSales");
            String lastUpdate = dcSetup.SaleUpdate;
            String sqlLastUpdate = Logging.FuturaDateTimeAddMins(lastUpdate, -30);
            if (sqlLastUpdate.Equals("0"))
            {
                String anSql = "delete from " + DaasExportTable + " where DAAS_SET_NAME = 'SALE' ";
                using (SqlCommand cmd = new SqlCommand(anSql, ersConnection))
                {
                    cmd.ExecuteNonQuery();
                }
            }

            String startingDate = dcSetup.MinSendDate.ToString();

            int dayInterval = dcSetup.LookupIntervalDays;
            if (dayInterval <= 0)
            {
                dayInterval = 1000;
            }

            String top10 = " ";
            if (dcSetup.ResultSet > 0)
            {
                top10 = " top " + dcSetup.ResultSet.ToString();
            }

            DateTime fromDate = DateTime.Now.AddDays(-1 * dayInterval);
            String anInterval = fromDate.ToString("yyyyMMdd");
            String dateFrom = dcSetup.DateFrom.ToString();
            String dateTo = dcSetup.DateTo.ToString();

            if(dcSetup.DateFrom > 0 && dcSetup.DateTo > 0)
            {
                String anSql = "delete from " + DaasExportTable + " where DAAS_SET_NAME = 'SALE' and DAAS_KEY1 >= @mindate and DAAS_KEY1 <= @maxDate  ";
                using (SqlCommand cmd = new SqlCommand(anSql, ersConnection))
                {
                    cmd.Parameters.AddWithValue("@mindate", dateFrom.ToString());
                    cmd.Parameters.AddWithValue("@maxDate", dateTo.ToString());
                    cmd.ExecuteNonQuery();
                }

            }

            String sqlstr = "select distinct " + top10 + " KAS_MANDANT, K.KAS_DATUM, K.KAS_FILIALE, K.KAS_KASSE, K.KAS_BONNR, KAS_BERICHT, IsNull(F.FIL_INDEX, '') [FIL_INDEX],  " +
                " IsNull(KAS_ZEIT, 0) [KAS_ZEIT], isNull(ZZO_STD_NAME, '') [ZZO_STD_NAME]  " +
                " from V_KASIDLTA K  " +
                " left join V_FILIALEN F on K.KAS_FILIALE = F.FIL_NUMMER and F.FIL_MANDANT = K.KAS_MANDANT" +
                " left join V_ZEITZONE Z on FIL_ZEITZONE = Z.ZZO_INDEX and ZZO_MANDANT = K.KAS_MANDANT " +
                " where ((K.KAS_DATUM > " + anInterval + " and  K.KAS_DATUM > " + startingDate + ") or (K.KAS_DATUM >= " + dateFrom + " and K.KAS_DATUM <= " + dateTo + ") ) " +
                " and not exists(select * from " + DaasExportTable + " D " +
                "   where DAAS_KEY1 = K.KAS_DATUM and K.KAS_FILIALE = DAAS_KEY2 and K.KAS_MANDANT = DAAS_KEY3 " +
                "   and K.KAS_KASSE = DAAS_KEY4 and K.KAS_BONNR = DAAS_KEY5 AND DAAS_SET_NAME = 'SALE'  ) " +

                " and not exists(select * from  V_KASIDLTA D  " +
                " where D.KAS_DATUM = K.KAS_DATUM and K.KAS_FILIALE = D.KAS_FILIALE and KAS_SATZART in ( 10, 12, 20) and K.KAS_MANDANT = D.KAS_MANDANT " +
                " and K.KAS_KASSE = D.KAS_KASSE and K.KAS_BONNR = D.KAS_BONNR) " +

                " union all " +
                " select distinct " + top10 + " KAS_MANDANT, K.KAS_DATUM, K.KAS_FILIALE, K.KAS_KASSE, K.KAS_BONNR, KAS_BERICHT, IsNull(F.FIL_INDEX, '') [FIL_INDEX], " +
                " IsNull(KAS_ZEIT, 0) [KAS_ZEIT], isNull(ZZO_STD_NAME, '') [ZZO_STD_NAME]   " +
                " from V_KASSTRNS K   " +
                " left join V_FILIALEN F on K.KAS_FILIALE = F.FIL_NUMMER and F.FIL_MANDANT = K.KAS_MANDANT" +
                " left join V_ZEITZONE Z on FIL_ZEITZONE = Z.ZZO_INDEX and ZZO_MANDANT = K.KAS_MANDANT " +
                " where ((K.KAS_VK_DATUM > " + anInterval + " and  K.KAS_VK_DATUM > " + startingDate + ") or (K.KAS_VK_DATUM >= " + dateFrom + " and K.KAS_VK_DATUM <= " + dateTo + ") ) " +

                " and not exists(select * from " + DaasExportTable + " D " +
                "   where DAAS_KEY1 = K.KAS_DATUM and K.KAS_FILIALE = DAAS_KEY2 and K.KAS_MANDANT = DAAS_KEY3 " +
                "   and K.KAS_KASSE = DAAS_KEY4 and K.KAS_BONNR = DAAS_KEY5 AND DAAS_SET_NAME = 'SALE' ) " +

                " and not exists(select * from  V_KASSTRNS D  " +
                " where D.KAS_DATUM = K.KAS_DATUM and K.KAS_FILIALE = D.KAS_FILIALE and KAS_SATZART in ( 10, 12, 20) and K.KAS_MANDANT = D.KAS_MANDANT " +
                " and K.KAS_KASSE = D.KAS_KASSE and K.KAS_BONNR = D.KAS_BONNR) " +

                " union all " +
                " select distinct " + top10 + " KAS_MANDANT, K.KAS_DATUM, K.KAS_FILIALE, K.KAS_KASSE, K.KAS_BONNR, KAS_BERICHT, IsNull(F.FIL_INDEX, '') [FIL_INDEX], " +
                " IsNull(KAS_ZEIT, 0) [KAS_ZEIT], isNull(ZZO_STD_NAME, '') [ZZO_STD_NAME]   " +
                " from V_KASSE K   " +
                " left join V_FILIALEN F on K.KAS_FILIALE = F.FIL_NUMMER and F.FIL_MANDANT = K.KAS_MANDANT" +
                " left join V_ZEITZONE Z on FIL_ZEITZONE = Z.ZZO_INDEX and ZZO_MANDANT = K.KAS_MANDANT " +
                " where ((K.KAS_DATUM > " + anInterval + " and  K.KAS_DATUM > " + startingDate + ") or (K.KAS_DATUM >= " + dateFrom + " and K.KAS_DATUM <= " + dateTo + ") ) " +
                " and not exists(select * from " + DaasExportTable + " D " +
                "   where DAAS_KEY1 = K.KAS_DATUM and K.KAS_FILIALE = DAAS_KEY2 and K.KAS_MANDANT = DAAS_KEY3 " +
                "   and K.KAS_KASSE = DAAS_KEY4 and K.KAS_BONNR = DAAS_KEY5 AND DAAS_SET_NAME = 'SALE'  ) " +
                " and not exists(select * from V_KASSE D  " +
                " where D.KAS_DATUM = K.KAS_DATUM and K.KAS_FILIALE = D.KAS_FILIALE and KAS_SATZART in ( 10, 12, 20) and K.KAS_MANDANT = D.KAS_MANDANT" +
                " and K.KAS_KASSE = D.KAS_KASSE and K.KAS_BONNR = D.KAS_BONNR) ";

            sqlstr = "select KAS_MANDANT, KAS_DATUM, KAS_FILIALE, KAS_KASSE, KAS_BONNR, max(ZZO_STD_NAME) [ZZO_STD_NAME] , max(KAS_ZEIT)[KAS_ZEIT], max(FIL_INDEX) [FIL_INDEX] from (" + sqlstr +

                " ) tbl group by KAS_MANDANT, KAS_DATUM, KAS_FILIALE, KAS_KASSE, KAS_BONNR  order by 2,3,4,5,1    option (force order)";

            Logging.WriteDebug("sales Sql: ", dcSetup.Debug);
            Logging.WriteDebug(sqlstr, dcSetup.Debug);

            using (SqlCommand cmd = new SqlCommand(sqlstr, ersConnection))
            {
                cmd.CommandTimeout = 1200;
                using (SqlDataReader areader = cmd.ExecuteReader())
                {
                    while (areader.Read())
                    {
                        int kasDatum = Convert.ToInt32(areader["KAS_DATUM"]);
                        int kasFiliale = Convert.ToInt32(areader["KAS_FILIALE"]);
                        int kasKasse = Convert.ToInt32(areader["KAS_KASSE"]);
                        int kasBonnr = Convert.ToInt32(areader["KAS_BONNR"]);
                        int kasZeit = Convert.ToInt32(areader["KAS_ZEIT"]);
                        int kasMandant = Convert.ToInt32(areader["KAS_MANDANT"]);

                        String custNo = getCustNoFromTransaction(ersConnection, kasDatum, kasFiliale, kasKasse, kasBonnr, kasMandant);
                        processTransaction(ersConnection, kasMandant, kasDatum, kasFiliale, kasKasse, kasBonnr, kasZeit, custNo);



                    }
                }

            }

            dcSetup.resetSalesDateRange();


        }

        public void getSalesNZ(SqlConnection ersConnection)
        {
            populateDiscountNZ(ersConnection);
            Logging.WriteLog("Starting getSalesNZ");
            String lastUpdate = dcSetup.SaleUpdateNZ;
            String sqlLastUpdate = Logging.FuturaDateTimeAddMins(lastUpdate, -30);
            if (sqlLastUpdate.Equals("0"))
            {
                String anSql = "delete from " + DaasExportTable + " where DAAS_SET_NAME = 'SALENZ' ";
                using (SqlCommand cmd = new SqlCommand(anSql, ersConnection))
                {
                    cmd.ExecuteNonQuery();
                }
            }

            String startingDate = dcSetup.MinSendDateNZ.ToString();

            int dayInterval = dcSetup.LookupIntervalDays;
            if (dayInterval <= 0)
            {
                dayInterval = 1000;
            }

            String top10 = " ";
            if (dcSetup.ResultSet > 0)
            {
                top10 = " top " + dcSetup.ResultSet.ToString();
            }

            DateTime fromDate = DateTime.Now.AddDays(-1 * dayInterval);
            String anInterval = fromDate.ToString("yyyyMMdd");
            String dateFrom = dcSetup.DateFromNZ.ToString();
            String dateTo = dcSetup.DateToNZ.ToString();

            if (dcSetup.DateFrom > 0 && dcSetup.DateTo > 0)
            {
                String anSql = "delete from " + DaasExportTable + " where DAAS_SET_NAME = 'SALENZ' and DAAS_KEY1 >= @mindate and DAAS_KEY1 <= @maxDate  ";
                using (SqlCommand cmd = new SqlCommand(anSql, ersConnection))
                {
                    cmd.Parameters.AddWithValue("@mindate", dateFrom.ToString());
                    cmd.Parameters.AddWithValue("@maxDate", dateTo.ToString());
                    cmd.ExecuteNonQuery();
                }

            }

            String sqlstr = "select distinct " + top10 + " K.KAS_DATUM, K.KAS_FILIALE, K.KAS_KASSE, K.KAS_BONNR, KAS_BERICHT, IsNull(F.FIL_INDEX, '') [FIL_INDEX],  " +
                " IsNull(KAS_ZEIT, 0) [KAS_ZEIT], isNull(ZZO_STD_NAME, '') [ZZO_STD_NAME]  " +
                " from V_KASIDLTA K  " +
                " left join V_FILIALEN F on K.KAS_FILIALE = F.FIL_NUMMER  " +
                " left join V_ZEITZONE Z on FIL_ZEITZONE = Z.ZZO_INDEX  " +
                " where ((K.KAS_DATUM > " + anInterval + " and  K.KAS_DATUM > " + startingDate + ") or (K.KAS_DATUM >= " + dateFrom + " and K.KAS_DATUM <= " + dateTo + ") ) " +
                " and not exists(select * from " + DaasExportTable + " D " +
                "   where DAAS_KEY1 = K.KAS_DATUM and K.KAS_FILIALE = DAAS_KEY2  " +
                "   and K.KAS_KASSE = DAAS_KEY4 and K.KAS_BONNR = DAAS_KEY5 AND DAAS_SET_NAME = 'SALENZ'  ) " +

                " and not exists(select * from  V_KASIDLTA D  " +
                " where D.KAS_DATUM = K.KAS_DATUM and K.KAS_FILIALE = D.KAS_FILIALE and KAS_SATZART in ( 10, 12, 20)  " +
                " and K.KAS_KASSE = D.KAS_KASSE and K.KAS_BONNR = D.KAS_BONNR) " +

                " union all " +
                " select distinct " + top10 + " K.KAS_DATUM, K.KAS_FILIALE, K.KAS_KASSE, K.KAS_BONNR, KAS_BERICHT, IsNull(F.FIL_INDEX, '') [FIL_INDEX], " +
                " IsNull(KAS_ZEIT, 0) [KAS_ZEIT], isNull(ZZO_STD_NAME, '') [ZZO_STD_NAME]   " +
                " from V_KASSTRNS K   " +
                " left join V_FILIALEN F on K.KAS_FILIALE = F.FIL_NUMMER " +
                " left join V_ZEITZONE Z on FIL_ZEITZONE = Z.ZZO_INDEX  " +
                " where ((K.KAS_VK_DATUM > " + anInterval + " and  K.KAS_VK_DATUM > " + startingDate + ") or (K.KAS_VK_DATUM >= " + dateFrom + " and K.KAS_VK_DATUM <= " + dateTo + ") ) " +

                " and not exists(select * from " + DaasExportTable + " D " +
                "   where DAAS_KEY1 = K.KAS_DATUM and K.KAS_FILIALE = DAAS_KEY2  " +
                "   and K.KAS_KASSE = DAAS_KEY4 and K.KAS_BONNR = DAAS_KEY5 AND DAAS_SET_NAME = 'SALENZ' ) " +

                " and not exists(select * from  V_KASSTRNS D  " +
                " where D.KAS_DATUM = K.KAS_DATUM and K.KAS_FILIALE = D.KAS_FILIALE and KAS_SATZART in ( 10, 12, 20) " +
                " and K.KAS_KASSE = D.KAS_KASSE and K.KAS_BONNR = D.KAS_BONNR) " +

                " union all " +
                " select distinct " + top10 + " K.KAS_DATUM, K.KAS_FILIALE, K.KAS_KASSE, K.KAS_BONNR, KAS_BERICHT, IsNull(F.FIL_INDEX, '') [FIL_INDEX], " +
                " IsNull(KAS_ZEIT, 0) [KAS_ZEIT], isNull(ZZO_STD_NAME, '') [ZZO_STD_NAME]   " +
                " from V_KASSE K   " +
                " left join V_FILIALEN F on K.KAS_FILIALE = F.FIL_NUMMER " +
                " left join V_ZEITZONE Z on FIL_ZEITZONE = Z.ZZO_INDEX " +
                " where ((K.KAS_DATUM > " + anInterval + " and  K.KAS_DATUM > " + startingDate + ") or (K.KAS_DATUM >= " + dateFrom + " and K.KAS_DATUM <= " + dateTo + ") ) " +
                " and not exists(select * from " + DaasExportTable + " D " +
                "   where DAAS_KEY1 = K.KAS_DATUM and K.KAS_FILIALE = DAAS_KEY2 " +
                "   and K.KAS_KASSE = DAAS_KEY4 and K.KAS_BONNR = DAAS_KEY5 AND DAAS_SET_NAME = 'SALENZ'  ) " +
                " and not exists(select * from V_KASSE D  " +
                " where D.KAS_DATUM = K.KAS_DATUM and K.KAS_FILIALE = D.KAS_FILIALE and KAS_SATZART in ( 10, 12, 20) " +
                " and K.KAS_KASSE = D.KAS_KASSE and K.KAS_BONNR = D.KAS_BONNR) ";

            sqlstr = "select KAS_DATUM, KAS_FILIALE, KAS_KASSE, KAS_BONNR, max(ZZO_STD_NAME) [ZZO_STD_NAME] , max(KAS_ZEIT)[KAS_ZEIT], max(FIL_INDEX) [FIL_INDEX] from (" + sqlstr +

                " ) tbl group by KAS_DATUM, KAS_FILIALE, KAS_KASSE, KAS_BONNR  order by 1,3,4,5    option (force order)";

            Logging.WriteDebug("salesnz Sql: ", dcSetup.Debug);
            Logging.WriteDebug(sqlstr, dcSetup.Debug);

            using (SqlCommand cmd = new SqlCommand(sqlstr, ersConnection))
            {
                cmd.CommandTimeout = 1200;
                using (SqlDataReader areader = cmd.ExecuteReader())
                {
                    while (areader.Read())
                    {
                        int kasDatum = Convert.ToInt32(areader["KAS_DATUM"]);
                        int kasFiliale = Convert.ToInt32(areader["KAS_FILIALE"]);
                        int kasKasse = Convert.ToInt32(areader["KAS_KASSE"]);
                        int kasBonnr = Convert.ToInt32(areader["KAS_BONNR"]);
                        int kasZeit = Convert.ToInt32(areader["KAS_ZEIT"]);
  
                        String custNo = getCustNoFromTransactionNZ(ersConnection, kasDatum, kasFiliale, kasKasse, kasBonnr);
                        processTransactionNZ(ersConnection, kasDatum, kasFiliale, kasKasse, kasBonnr, kasZeit, custNo);



                    }
                }

            }

            dcSetup.resetSalesNZDateRange();


        }


        private String getCurrency(int amandant)
        {
            if (amandant == 1)
                return "AUD";
            else
                return "NZD";
        }


        private void processTransaction(SqlConnection ersConnection, int kasMandant, int kasDatum, int kasFiliale, int kasKasse, int kasBonnr, int kasZeit, String custNo)
        {
            Logging.WriteLog("processTransaction - kasdatum: " + kasDatum + " fasFiliale: " + kasFiliale + " kasKasse: " + kasKasse +
                " kasBonnr: " + kasBonnr + " date: " + kasDatum.ToString() + " time: " + kasZeit.ToString());
            String sdate = kasDatum.ToString();
            if (sdate.Length != 8)
            {
                Logging.WriteErrorLog("kasDatum: " + kasDatum + " wrong format");
                updateDaasExport(kasDatum.ToString(), kasFiliale.ToString(), kasMandant.ToString(), kasKasse.ToString(), kasBonnr.ToString(), "SALE", "", ersConnection);
                return;
            }


            localCurrency = getCurrency(kasMandant);



            String stime = kasZeit.ToString();
            if (stime.Length != 6 && stime.Length != 5)
            {
                Logging.WriteErrorLog("kasZeit: " + stime + " wrong format");
                stime = "121212";
            }
            if (stime.Length != 6)
                stime = "0" + stime;

            String theDate = sdate.Substring(0, 4) + "-" + sdate.Substring(4, 2) + "-" + sdate.Substring(6, 2);
            String thetime = stime.Substring(0, 2) + ":" + stime.Substring(2, 2) + ":" + stime.Substring(4, 2);
            String timestamp = theDate + "T" + thetime;

            String anSql = "select KAS_DATUM, KAS_FILIALE, KAS_KASSE, KAS_BONNR, KAS_POSNR, IsNull(ART_LFID_NUMMER, '')[ART_LFID_NUMMER], KAS_REFNUMMER, KAS_SATZART, KAS_ZEIT, KAS_RETOUR, IsNull(AGR_GRPNUMMER, 0) [AGR_GRPNUMMER], isNull(PC.BEZ_TEXT, '')PETTYCASH,  round(KAS_BETRAG * KAS_ANZAHL, 2)[LINE_AMOUNT], " +
                 " KAS_FEHLBON, KAS_BETRAG, KAS_ANZAHL, KAS_INFO, IsNull(WAR_NUMMER, 0) [PROD_GROUP_NO],   WAR_TEXT[PROD_GROUP],  KAS_VK_DATUM, " +
                 " IsNull(KRF_BONTEXT2, '') + ' ' + IsNull(KRF_BONTEXT2, '') [ITEM_DESC], IsNull(ART_EINHEIT, '') [SIZE], IsNull(ART_EIGENSCHAFT, '') [COLOR], " +
                 " isNull(LIF_INDEX, '') [SUPPLIER], IsNull(AGR_TEXT, '') [ITEM_NAME], IsNull(P.BEZ_TEXT, '') [PAYMENT_METHOD],   IsNull( P.BEZ_NUMMER, 0) [PAYMENT_TYPE_ID], " +
                 " IsNull(P.BEZ_RUECK_FLAG, 0) BEZ_RUECK_FLAG, isNull(BG.BEZ_TEXT, '')[GIFT_CARD_TEXT], isNull(BG.BEZ_NUMMER, 0)[GIFT_CARD_PAYNO], IsNull(ART_GEWICHT, 0)[WEIGHT], RTRIM(LTrim(substring(KAS_INFO,7,2))) [PAYMENT_NO]  " +
                  " , IsNull((select top 1 UST_PROZENT from V_UMSATZST where UST_NUMMER = KAS_USTKEY and UST_MANDANT = KAS_MANDANT order by UST_DATUM desc), 0.0) [UST_PROZENT] " +
                 " ,IsNull((select top 1 UST_TEXT from V_UMSATZST where UST_NUMMER = KAS_USTKEY and UST_MANDANT = KAS_MANDANT order by UST_DATUM desc), '') [UST_TEXT], IsNull(RTG_TEXT, '') [REFUND_REASON], KAS_DATUM as REAL_DATE, KAS_USTKEY " +

                 " from V_KASIDLTA    " +
                 " LEFT JOIN V_WARENGRP on LTRIM(Str(WAR_NUMMER, 10)) = RTRIM(LTrim(SubString(KAS_INFO, 15, 3))) and WAR_MANDANT = KAS_MANDANT " +
                 " LEFT JOIN V_KASSREF on KAS_REFNUMMER = KRF_REFNUMMER and KRF_MANDANT = KAS_MANDANT " +
                 " left join V_ARTIKEL on ART_REFNUMMER = KAS_REFNUMMER and ART_MANDANT = KAS_MANDANT " +
                 " left join V_ART_KOPF on ART_WARENGR = AGR_WARENGR and ART_ABTEILUNG = AGR_ABTEILUNG and ART_TYPE = AGR_TYPE and ART_GRPNUMMER = AGR_GRPNUMMER and AGR_MANDANT = KAS_MANDANT " +
                 " left join V_LIEFERTN on AGR_LIEFERANT = LIF_NUMMER and LIF_MANDANT = KAS_MANDANT " +
                 " left join V_BEZ_ART P on LTRIM(RTrim(substring(KAS_INFO,7,2))) = RTRIM(LTrim(Str(P.BEZ_NUMMER, 3))) and P.BEZ_MANDANT = KAS_MANDANT  " +
                 " left join V_BEZ_ART BG on LTRIM(Str(BG.BEZ_NUMMER, 10)) = RTRIM(LTrim(SubString(KAS_INFO, 15, 3))) and  SubString(KAS_INFO, 13, 2) = '03' and BG.BEZ_SPEZIAL_BEZ = 1 and BG.BEZ_MANDANT = KAS_MANDANT " +
                 " left join V_BEZ_ART PC on LTRIM(Str(PC.BEZ_NUMMER, 10)) = RTRIM(LTrim(SubString(KAS_INFO, 6, 2)))  and PC.BEZ_MANDANT = KAS_MANDANT " +
                 " left join V_RETGRUND on  SUBSTRING(KAS_INFO, 14, 1) = LTRIM(Str(RTG_NUMMER, 10)) AND RTG_MANDANT = @KAS_MANDANT " +
                 " where KAS_DATUM = @KAS_DATUM and KAS_FILIALE = @KAS_FILIALE " +
                 " and KAS_KASSE = @KAS_KASSE and  KAS_BONNR = @KAS_BONNR and KAS_MANDANT = @KAS_MANDANT" +
                 " union " +
                  "select KAS_DATUM, KAS_FILIALE, KAS_KASSE, KAS_BONNR, KAS_POSNR, IsNull(ART_LFID_NUMMER, '')[ART_LFID_NUMMER],  KAS_REFNUMMER, KAS_SATZART, KAS_ZEIT, KAS_RETOUR, IsNull(AGR_GRPNUMMER, 0) [AGR_GRPNUMMER], isNull(PC.BEZ_TEXT, '')PETTYCASH, round(KAS_BETRAG * KAS_ANZAHL, 2)[LINE_AMOUNT],  " +
                 " KAS_FEHLBON, KAS_BETRAG, KAS_ANZAHL, KAS_INFO, IsNull(WAR_NUMMER, 0) [PROD_GROUP_NO],   WAR_TEXT[PROD_GROUP], KAS_VK_DATUM, " +
                 " IsNull(KRF_BONTEXT2, '') + ' ' + IsNull(KRF_BONTEXT2, '') [ITEM_DESC], IsNull(ART_EINHEIT, '') [SIZE], IsNull(ART_EIGENSCHAFT, '') [COLOR], " +
                 " IsNull(LIF_INDEX, '') [SUPPLIER], IsNull(AGR_TEXT, '') [ITEM_NAME], IsNull(P.BEZ_TEXT, '') [PAYMENT_METHOD],  IsNull( P.BEZ_NUMMER, 0) [PAYMENT_TYPE_ID], " +
                 " IsNull(P.BEZ_RUECK_FLAG, 0) BEZ_RUECK_FLAG, isNull(BG.BEZ_TEXT, '')[GIFT_CARD_TEXT], isNull(BG.BEZ_NUMMER, 0)[GIFT_CARD_PAYNO], IsNull(ART_GEWICHT, 0)[WEIGHT],  RTRIM(LTrim(substring(KAS_INFO,7,2))) [PAYMENT_NO] " +
                 " , IsNull((select top 1 UST_PROZENT from V_UMSATZST where UST_NUMMER = KAS_USTKEY and UST_MANDANT = KAS_MANDANT order by UST_DATUM desc), 0.0) [UST_PROZENT] " +
                 " ,IsNull((select top 1 UST_TEXT from V_UMSATZST where UST_NUMMER = KAS_USTKEY and UST_MANDANT = KAS_MANDANT order by UST_DATUM desc), '') [UST_TEXT], IsNull(RTG_TEXT, '') [REFUND_REASON],  KAS_VK_DATUM as REAL_DATE, KAS_USTKEY   " +
                 " from V_KASSTRNS " +
                 " LEFT JOIN V_WARENGRP on LTRIM(Str(WAR_NUMMER, 10)) = RTRIM(LTrim(SubString(KAS_INFO, 15, 3))) and WAR_MANDANT = KAS_MANDANT " +
                 " LEFT JOIN V_KASSREF on KAS_REFNUMMER = KRF_REFNUMMER and KRF_MANDANT = KAS_MANDANT " +
                 " left join V_ARTIKEL on ART_REFNUMMER = KAS_REFNUMMER and ART_MANDANT = KAS_MANDANT " +
                 " left join V_ART_KOPF on ART_WARENGR = AGR_WARENGR and ART_ABTEILUNG = AGR_ABTEILUNG and ART_TYPE = AGR_TYPE and ART_GRPNUMMER = AGR_GRPNUMMER and AGR_MANDANT = KAS_MANDANT " +
                 " left join V_LIEFERTN on AGR_LIEFERANT = LIF_NUMMER and LIF_MANDANT = KAS_MANDANT " +
                 " left join V_BEZ_ART P on LTRIM(RTrim(substring(KAS_INFO,7,2))) = RTRIM(LTrim(Str(P.BEZ_NUMMER, 3))) and P.BEZ_MANDANT = KAS_MANDANT  " +
                 " left join V_BEZ_ART BG on LTRIM(Str(BG.BEZ_NUMMER, 10)) = RTRIM(LTrim(SubString(KAS_INFO, 15, 3))) and  SubString(KAS_INFO, 13, 2) = '03' and BG.BEZ_SPEZIAL_BEZ = 1 and BG.BEZ_MANDANT = KAS_MANDANT " +
                 //                " where KAS_DATUM = " + kasDatum.ToString() + " and KAS_FILIALE = " + kasFiliale.ToString() + 
                 //                " and KAS_KASSE = " + kasKasse.ToString() + " and  KAS_BONNR = " + kasBonnr + " " +
                 " left join V_BEZ_ART PC on LTRIM(Str(PC.BEZ_NUMMER, 10)) = RTRIM(LTrim(SubString(KAS_INFO, 6, 2)))  and PC.BEZ_MANDANT = KAS_MANDANT " +
                 " left join V_RETGRUND on  SUBSTRING(KAS_INFO, 14, 1) = LTRIM(Str(RTG_NUMMER, 10)) AND RTG_MANDANT = @KAS_MANDANT " +
                 " where KAS_DATUM = @KAS_DATUM and KAS_FILIALE = @KAS_FILIALE " +
                 " and KAS_KASSE = @KAS_KASSE and  KAS_BONNR = @KAS_BONNR and KAS_MANDANT = @KAS_MANDANT " +

                 " union " +
                  "select KAS_DATUM, KAS_FILIALE, KAS_KASSE, KAS_BONNR, KAS_POSNR, IsNull(ART_LFID_NUMMER, '')[ART_LFID_NUMMER], KAS_REFNUMMER, KAS_SATZART, KAS_ZEIT, KAS_RETOUR, IsNull(AGR_GRPNUMMER, 0) [AGR_GRPNUMMER],  isNull(PC.BEZ_TEXT, '')PETTYCASH,  round(KAS_BETRAG * KAS_ANZAHL, 2)[LINE_AMOUNT]," +
                 " KAS_FEHLBON, KAS_BETRAG, KAS_ANZAHL, KAS_INFO, IsNull(WAR_NUMMER, 0) [PROD_GROUP_NO],   WAR_TEXT[PROD_GROUP], KAS_VK_DATUM, " +
                 " IsNull(KRF_BONTEXT2, '') + ' ' + IsNull(KRF_BONTEXT2, '') [ITEM_DESC], IsNull(ART_EINHEIT, '') [SIZE], IsNull(ART_EIGENSCHAFT, '') [COLOR], " +
                 " IsNull(LIF_INDEX, '') [SUPPLIER], IsNull(AGR_TEXT, '') [ITEM_NAME], IsNull(P.BEZ_TEXT, '') [PAYMENT_METHOD],   IsNull( P.BEZ_NUMMER, 0) [PAYMENT_TYPE_ID],   " +
                 " IsNull(P.BEZ_RUECK_FLAG, 0) BEZ_RUECK_FLAG, isNull(BG.BEZ_TEXT, '')[GIFT_CARD_TEXT], isNull(BG.BEZ_NUMMER, 0)[GIFT_CARD_PAYNO], IsNull(ART_GEWICHT, 0)[WEIGHT],  RTRIM(LTrim(substring(KAS_INFO,7,2))) [PAYMENT_NO]  " +
                 " , IsNull((select top 1 UST_PROZENT from V_UMSATZST where UST_NUMMER = KAS_USTKEY and UST_MANDANT = KAS_MANDANT order by UST_DATUM desc), 0.0) [UST_PROZENT] " +
                 " ,IsNull((select top 1 UST_TEXT from V_UMSATZST where UST_NUMMER = KAS_USTKEY and UST_MANDANT = KAS_MANDANT order by UST_DATUM desc), '') [UST_TEXT], IsNull(RTG_TEXT, '') [REFUND_REASON], KAS_DATUM as REAL_DATE, KAS_USTKEY  " +

                 " from V_KASSE  " +
                 " LEFT JOIN V_WARENGRP on LTRIM(Str(WAR_NUMMER, 10)) = RTRIM(LTrim(SubString(KAS_INFO, 15, 3))) and WAR_MANDANT = KAS_MANDANT " +
                 " LEFT JOIN V_KASSREF on KAS_REFNUMMER = KRF_REFNUMMER and KRF_MANDANT = KAS_MANDANT " +
                 " left join V_ARTIKEL on ART_REFNUMMER = KAS_REFNUMMER and ART_MANDANT = KAS_MANDANT " +
                 " left join V_ART_KOPF on ART_WARENGR = AGR_WARENGR and ART_ABTEILUNG = AGR_ABTEILUNG and ART_TYPE = AGR_TYPE and ART_GRPNUMMER = AGR_GRPNUMMER and AGR_MANDANT = KAS_MANDANT " +
                 " left join V_LIEFERTN on AGR_LIEFERANT = LIF_NUMMER and LIF_MANDANT = KAS_MANDANT " +
                 " left join V_BEZ_ART P on LTRIM(RTrim(substring(KAS_INFO,7,2))) = RTRIM(LTrim(Str(P.BEZ_NUMMER, 3))) and P.BEZ_MANDANT = KAS_MANDANT  " +
                 " left join V_BEZ_ART BG on LTRIM(Str(BG.BEZ_NUMMER, 10)) = RTRIM(LTrim(SubString(KAS_INFO, 15, 3))) and  SubString(KAS_INFO, 13, 2) = '03' and BG.BEZ_SPEZIAL_BEZ = 1 and BG.BEZ_MANDANT = KAS_MANDANT  " +
                 " left join V_BEZ_ART PC on LTRIM(Str(PC.BEZ_NUMMER, 10)) = RTRIM(LTrim(SubString(KAS_INFO, 6, 2)))  and PC.BEZ_MANDANT = KAS_MANDANT " +
                 " left join V_RETGRUND on  SUBSTRING(KAS_INFO, 14, 1) = LTRIM(Str(RTG_NUMMER, 10)) AND RTG_MANDANT = @KAS_MANDANT " +
                 " where KAS_DATUM = @KAS_DATUM and KAS_FILIALE = @KAS_FILIALE " +
                 " and KAS_KASSE = @KAS_KASSE and  KAS_BONNR = @KAS_BONNR and KAS_MANDANT = @KAS_MANDANT" +


                 " order by 1 ,2, 3, 4, 5  option (force order)";

            DataTable dsDetails = null;
            //    Logging.WriteLog(anSql);
            using (SqlDataAdapter daTrans = new SqlDataAdapter(anSql, ersConnection))
            {
                daTrans.SelectCommand.CommandTimeout = 600;
                daTrans.SelectCommand.Parameters.AddWithValue("@KAS_DATUM", kasDatum);
                daTrans.SelectCommand.Parameters.AddWithValue("@KAS_FILIALE", kasFiliale);
                daTrans.SelectCommand.Parameters.AddWithValue("@KAS_KASSE", kasKasse);
                daTrans.SelectCommand.Parameters.AddWithValue("@KAS_BONNR", kasBonnr);
                daTrans.SelectCommand.Parameters.AddWithValue("@KAS_MANDANT", kasMandant);
                dsDetails = new DataTable();
                daTrans.Fill(dsDetails);
            }
            if (dsDetails == null)
            {
                updateDaasExport(kasDatum.ToString(), kasFiliale.ToString(), kasMandant.ToString(), kasKasse.ToString(), kasBonnr.ToString(), "SALE", "", ersConnection);
                return;
            }

            SalesJson salesJson = new SalesJson();
            salesJson.FuturaSale = new Sale();

            string sStaffno = "1";
            salesJson.FuturaSale.BranchNo = kasFiliale.ToString();
            salesJson.FuturaSale.ReceiptId = kasFiliale.ToString() + "/" + kasKasse.ToString() + "/" + kasBonnr.ToString() + "/" + kasDatum.ToString();
            salesJson.FuturaSale.EmployeeId = sStaffno;
            salesJson.FuturaSale.CashierId = sStaffno;
            salesJson.FuturaSale.ReceiptMode = "NORMAL";
            salesJson.FuturaSale.ReceiptState = "FINISHED";
            salesJson.FuturaSale.TillNo = kasKasse.ToString();
            salesJson.FuturaSale.ReceiptNo = kasBonnr.ToString();
            salesJson.FuturaSale.Timestamp = timestamp;
            salesJson.FuturaSale.SaleType = "NORMAL";
            salesJson.FuturaSale.SaleLines = new List<SaleLine>();
            salesJson.FuturaSale.PaymentLines = new List<PaymentLine>();
            salesJson.FuturaSale.CustomerNo = custNo;

            int isVoid = 1;
            double totSales = 0;
            double totAmountPaid = 0;
            bool arefund = false;
            bool apurchase = false;
            String taxName = "";
            String afterPayId = "";
            bool paymentExist = false;
            bool itemExist = false;
            bool giftcardIssue = false;

            String pettyCash = "";
            String paymPetty = "";


            SaleLine saleLine = null;

            int saleLineNo = 0;
            int paymentLineNo = 0;

            String transactionDate = "";

            for (int k = 0; k < dsDetails.Rows.Count; k++)
            {
                DataRow arow = dsDetails.Rows[k];

                if(k == 0)
                {
                    String stdate = arow["KAS_VK_DATUM"].ToString() ;
                    try
                    {
                        transactionDate = stdate.Substring(0, 4) + "-" + stdate.Substring(4, 2) + "-" + stdate.Substring(6, 2);
                    }
                    catch (Exception e)
                    {
                        transactionDate = theDate;
                    }
                }

                int recType = Convert.ToInt32(arow["KAS_SATZART"]);
                double aqty = Convert.ToDouble(arow["KAS_ANZAHL"]);

                int posNo = Convert.ToInt32(arow["KAS_POSNR"]);

                int kasRetour = 0;
                try
                {
                    kasRetour = Convert.ToInt32(arow["KAS_RETOUR"]);
                }
                catch (Exception e) { }


                if (recType == 17 && aqty == 4)
                {
                    afterPayId = arow["KAS_INFO"].ToString().Trim();
                    if (giftcardIssue)
                    {
                        if (saleLine != null)
                        {
                            saleLine.VoucherNumber = afterPayId;
                            giftcardIssue = false;
                            afterPayId = "";
                        }
                    }
                }

                if (recType == 17)
                {
                  //  String astr = arow["KAS_INFO"].ToString().Trim();
                  //  if (astr.ToUpper().Contains("LAYB"))
                  //  {
                  //      if (saleLine != null)
                  //      {
                  //          saleLine.SalesMode = "LAYAWAY";
                  //      }
                  //  }
                }


                if (recType == 17 && Convert.ToDouble(arow["KAS_ANZAHL"]) == 16)
                {
                    if (saleLine != null)
                    {
                        saleLine.ReturnReason = arow["REFUND_REASON"].ToString().Trim();
                    }
                }
                if (recType == 17 && Convert.ToDouble(arow["KAS_ANZAHL"]) == 5)
                {
                    if (saleLine != null)
                    {
                        DiscountLine discountLine = new DiscountLine();
                        saleLine.DiscountLines.Add(discountLine);
                        discountLine.PosNo = posNo;
                        populateDiscount(kasMandant, arow["KAS_INFO"].ToString(), discountLine);
                    }
                }

                if (recType == 14)
                {
                    pettyCash = arow["PETTYCASH"].ToString();
                }

                if (recType == 16 && arow["KAS_FEHLBON"] != null && arow["KAS_FEHLBON"].ToString().Equals("0") && Convert.ToDouble(arow["KAS_BETRAG"]) != 0)
                {
                    paymentExist = true;
                    PaymentLine paymentLine = new PaymentLine();
                    salesJson.FuturaSale.PaymentLines.Add(paymentLine);
                    paymentLine.PosNo = posNo;

                    double paymentAmount = Convert.ToDouble(arow["KAS_BETRAG"]);

                    if (arow["BEZ_RUECK_FLAG"].ToString().Equals("1"))
                    {
                        paymentAmount *= -1;
                    }


                    paymentLine.Amount = paymentAmount;
                    paymentLine.PaymentType = arow["PAYMENT_METHOD"].ToString();
                    paymentLine.PaymentTypeId = Convert.ToInt32(arow["PAYMENT_TYPE_ID"].ToString());

                    if (arow["PAYMENT_METHOD"].ToString().ToUpper().Contains("PETTY"))
                    {
                        paymentLine.PaymentType = arow["PAYMENT_METHOD"].ToString() + " " + pettyCash;
                        pettyCash = "";
                        paymPetty = "petty cash";
                    }
                    paymentLine.RefNumber = afterPayId;
                    afterPayId = "";

                    paymentLine.Currency = localCurrency;

                    totAmountPaid += paymentAmount;

                }

                if (recType == 15)
                {

                    if (arow["KAS_FEHLBON"] == null || !arow["KAS_FEHLBON"].ToString().Equals("0"))
                    {
                        continue;
                    }


                    String anInfo = arow["KAS_INFO"].ToString().Trim();
                    String[] theFields = anInfo.Split(' ');
                    String amtField = "";
                    for (int iii = 0; iii < theFields.Length; iii++)
                    {
                        String afield = theFields[iii].Trim();
                        if (afield.Equals(""))
                        {
                            continue;
                        }
                        amtField = afield.Substring(0, afield.Length - 1);
                    }

                    int idisc = Logging.strToIntDef(amtField, 0);
                    itemExist = true;

                    giftcardIssue = false;

                    saleLine = new SaleLine();
                    salesJson.FuturaSale.SaleLines.Add(saleLine);
                    saleLine.PosNo = posNo;

                    saleLine.DiscountLines = new List<DiscountLine>();

                    saleLine.SalesMode = "MODE_NORMAL";

                    itemExist = true;

                    saleLine.Qty = Math.Abs(aqty);
                    saleLine.Price = Math.Abs( Convert.ToDouble(arow["KAS_BETRAG"]));
                    saleLine.OriginalPrice = Math.Abs((idisc / 100.00));
                    saleLine.LineValueGross = Convert.ToDouble(arow["LINE_AMOUNT"]);
                    double anamount = Convert.ToDouble(arow["LINE_AMOUNT"]);
                    double anamountAbs = Math.Abs(anamount);

                    double atax = Convert.ToDouble(arow["UST_PROZENT"]);

                    saleLine.VatPercent = atax;

                    double anamountExTaxAbs = anamountAbs;
                    if (atax != 0)
                    {
                        anamountExTaxAbs = anamountAbs / (1 + (1 / atax));
                    }

                    if (anamount < 0)
                    {
                        anamountExTaxAbs = -1 * anamountExTaxAbs;
                    }
                    saleLine.VatHeadEntityId = (Convert.ToInt32(arow["KAS_USTKEY"])).ToString();
                    saleLine.VatAmount = anamountAbs - anamountExTaxAbs;
//                    saleLine.LineValueGross = anamount;
                    saleLine.LineValueGross = anamountAbs;
                    saleLine.LineValueNet = anamountExTaxAbs;


                    String kasInfo = arow["KAS_INFO"].ToString();
                    if (kasInfo.Length > 5)
                        sStaffno = kasInfo.Substring(0, 6);


                    saleLine.SalesPersonId = sStaffno;
                     saleLine.ProductGroupId = Convert.ToInt32(arow["PROD_GROUP_NO"]);
                    saleLine.SkuId = Convert.ToInt32(arow["KAS_REFNUMMER"]);
                    saleLine.VoucherNumber = "";
                    saleLine.VoucherPaymentTypeId = "";
                    saleLine.VoucherPaymentType = "";
                    if (saleLine.SkuId == 0)
                    {
                        String giftIssue = arow["GIFT_CARD_TEXT"].ToString();
                        if (!giftIssue.Equals(""))
                        {
                            saleLine.VoucherNumber = arow["GIFT_CARD_PAYNO"].ToString();
                            giftcardIssue = true;
                            saleLine.VoucherPaymentTypeId = arow["GIFT_CARD_PAYNO"].ToString();
                            saleLine.VoucherPaymentType = arow["GIFT_CARD_TEXT"].ToString();
                        }
                    }

                    saleLine.ReturnReason = "";
                    bool isReturn = false;
                    saleLine.SaleLineType = "SALE";
                    if (kasRetour == 1)
                    {
                        saleLine.SaleLineType = "RETURN";
                        arefund = true;
                        isReturn = true;
                        saleLine.Qty = -1 * saleLine.Qty;

                        saleLine.LineValueGross = -1 * saleLine.LineValueGross;
                        saleLine.LineValueNet = -1 * saleLine.LineValueNet;
                        saleLine.VatAmount = -1 * saleLine.VatAmount;
                        saleLine.ReturnReason = arow["REFUND_REASON"].ToString();
                        saleLine.SalesMode = "RETOUR";

                    }
                    totSales += saleLine.LineValueGross;

                    saleLine.Wac = getWac(saleLine.SkuId, kasFiliale, kasDatum, isReturn, ersConnection);

                }





            }

            if(arefund)
            {
                salesJson.FuturaSale.SaleType = "RETURN";
            }

            if(!itemExist)
            {
                salesJson.FuturaSale.SaleType = "LAYBY";
            }

            salesJson.FuturaSale.EmployeeId = sStaffno;
            salesJson.FuturaSale.CashierId = sStaffno;
            salesJson.FuturaSale.TransactionDate = transactionDate;
            String ajsonStr = SimpleJson.SerializeObject(salesJson).ToString();
            String md5Contents = Logging.CreateMD5(ajsonStr);

            String storedMd5 = getMd5(kasDatum.ToString(), kasFiliale.ToString(), kasMandant.ToString(), kasKasse.ToString(), kasBonnr.ToString(), "SALE", Logging.strToInt64Def(dcSetup.SaleUpdate, 0),
                ersConnection);

            if (!md5Contents.Equals(storedMd5))
            {
                Logging.WriteDebug("JSON " + SimpleJson.SerializeObject(salesJson).ToString(), dcSetup.Debug);
                SendNewMessageQueue(SimpleJson.SerializeObject(salesJson).ToString(), dcSetup.SalesQueueName);
            }
            updateDaasExport(kasDatum.ToString(), kasFiliale.ToString(), kasMandant.ToString(), kasKasse.ToString(), kasBonnr.ToString(), "SALE", md5Contents, ersConnection);

            DateTime anow = DateTime.Now;
            String snow = anow.ToString("yyyyMMddhhmmss");
            dcSetup.SaleUpdate = snow;


        }

        private void processTransactionNZ(SqlConnection ersConnection, int kasDatum, int kasFiliale, int kasKasse, int kasBonnr, int kasZeit, String custNo)
        {
            Logging.WriteLog("processTransactionNZ - kasdatum: " + kasDatum + " fasFiliale: " + kasFiliale + " kasKasse: " + kasKasse +
                " kasBonnr: " + kasBonnr + " date: " + kasDatum.ToString() + " time: " + kasZeit.ToString());
            String sdate = kasDatum.ToString();
            if (sdate.Length != 8)
            {
                Logging.WriteErrorLog("kasDatum: " + kasDatum + " wrong format");
                updateDaasExport(kasDatum.ToString(), kasFiliale.ToString(), "1", kasKasse.ToString(), kasBonnr.ToString(), "SALENZ", "", ersConnection);
                return;
            }


            String nzCurrency = getCurrency(2);



            String stime = kasZeit.ToString();
            if (stime.Length != 6 && stime.Length != 5)
            {
                Logging.WriteErrorLog("kasZeit: " + stime + " wrong format");
                stime = "121212";
            }
            if (stime.Length != 6)
                stime = "0" + stime;

            String theDate = sdate.Substring(0, 4) + "-" + sdate.Substring(4, 2) + "-" + sdate.Substring(6, 2);
            String thetime = stime.Substring(0, 2) + ":" + stime.Substring(2, 2) + ":" + stime.Substring(4, 2);
            String timestamp = theDate + "T" + thetime;

            String anSql = "select KAS_DATUM, KAS_FILIALE, KAS_KASSE, KAS_BONNR, KAS_POSNR, IsNull(ART_LFID_NUMMER, '')[ART_LFID_NUMMER], KAS_REFNUMMER, KAS_SATZART, KAS_ZEIT, KAS_RETOUR, IsNull(AGR_GRPNUMMER, 0) [AGR_GRPNUMMER], isNull(PC.BEZ_TEXT, '')PETTYCASH,  round(KAS_BETRAG * KAS_ANZAHL, 2)[LINE_AMOUNT], " +
                 " KAS_FEHLBON, KAS_BETRAG, KAS_ANZAHL, KAS_INFO, IsNull(WAR_NUMMER, 0) [PROD_GROUP_NO],   WAR_TEXT[PROD_GROUP],  KAS_VK_DATUM, " +
                 " IsNull(KRF_BONTEXT2, '') + ' ' + IsNull(KRF_BONTEXT2, '') [ITEM_DESC], IsNull(ART_EINHEIT, '') [SIZE], IsNull(ART_EIGENSCHAFT, '') [COLOR], " +
                 " isNull(LIF_INDEX, '') [SUPPLIER], IsNull(AGR_TEXT, '') [ITEM_NAME], IsNull(P.BEZ_TEXT, '') [PAYMENT_METHOD],   IsNull( P.BEZ_NUMMER, 0) [PAYMENT_TYPE_ID], " +
                 " IsNull(P.BEZ_RUECK_FLAG, 0) BEZ_RUECK_FLAG, isNull(BG.BEZ_TEXT, '')[GIFT_CARD_TEXT], isNull(BG.BEZ_NUMMER, 0)[GIFT_CARD_PAYNO], IsNull(ART_GEWICHT, 0)[WEIGHT], RTRIM(LTrim(substring(KAS_INFO,7,2))) [PAYMENT_NO]  " +
                  " , IsNull((select top 1 UST_PROZENT from V_UMSATZST where UST_NUMMER = KAS_USTKEY  order by UST_DATUM desc), 0.0) [UST_PROZENT] " +
                 " ,IsNull((select top 1 UST_TEXT from V_UMSATZST where UST_NUMMER = KAS_USTKEY order by UST_DATUM desc), '') [UST_TEXT], IsNull(RTG_TEXT, '') [REFUND_REASON], KAS_DATUM as REAL_DATE, KAS_USTKEY " +

                 " from V_KASIDLTA    " +
                 " LEFT JOIN V_WARENGRP on LTRIM(Str(WAR_NUMMER, 10)) = RTRIM(LTrim(SubString(KAS_INFO, 15, 3)))  " +
                 " LEFT JOIN V_KASSREF on KAS_REFNUMMER = KRF_REFNUMMER  " +
                 " left join V_ARTIKEL on ART_REFNUMMER = KAS_REFNUMMER  " +
                 " left join V_ART_KOPF on ART_WARENGR = AGR_WARENGR and ART_ABTEILUNG = AGR_ABTEILUNG and ART_TYPE = AGR_TYPE and ART_GRPNUMMER = AGR_GRPNUMMER  " +
                 " left join V_LIEFERTN on AGR_LIEFERANT = LIF_NUMMER  " +
                 " left join V_BEZ_ART P on LTRIM(RTrim(substring(KAS_INFO,7,2))) = RTRIM(LTrim(Str(P.BEZ_NUMMER, 3)))   " +
                 " left join V_BEZ_ART BG on LTRIM(Str(BG.BEZ_NUMMER, 10)) = RTRIM(LTrim(SubString(KAS_INFO, 15, 3))) and  SubString(KAS_INFO, 13, 2) = '03' and BG.BEZ_SPEZIAL_BEZ = 1  " +
                 " left join V_BEZ_ART PC on LTRIM(Str(PC.BEZ_NUMMER, 10)) = RTRIM(LTrim(SubString(KAS_INFO, 6, 2)))   " +
                 " left join V_RETGRUND on  SUBSTRING(KAS_INFO, 14, 1) = LTRIM(Str(RTG_NUMMER, 10)) " +
                 " where KAS_DATUM = @KAS_DATUM and KAS_FILIALE = @KAS_FILIALE " +
                 " and KAS_KASSE = @KAS_KASSE and  KAS_BONNR = @KAS_BONNR " +
                 " union " +
                  "select KAS_DATUM, KAS_FILIALE, KAS_KASSE, KAS_BONNR, KAS_POSNR, IsNull(ART_LFID_NUMMER, '')[ART_LFID_NUMMER],  KAS_REFNUMMER, KAS_SATZART, KAS_ZEIT, KAS_RETOUR, IsNull(AGR_GRPNUMMER, 0) [AGR_GRPNUMMER], isNull(PC.BEZ_TEXT, '')PETTYCASH, round(KAS_BETRAG * KAS_ANZAHL, 2)[LINE_AMOUNT],  " +
                 " KAS_FEHLBON, KAS_BETRAG, KAS_ANZAHL, KAS_INFO, IsNull(WAR_NUMMER, 0) [PROD_GROUP_NO],   WAR_TEXT[PROD_GROUP], KAS_VK_DATUM, " +
                 " IsNull(KRF_BONTEXT2, '') + ' ' + IsNull(KRF_BONTEXT2, '') [ITEM_DESC], IsNull(ART_EINHEIT, '') [SIZE], IsNull(ART_EIGENSCHAFT, '') [COLOR], " +
                 " IsNull(LIF_INDEX, '') [SUPPLIER], IsNull(AGR_TEXT, '') [ITEM_NAME], IsNull(P.BEZ_TEXT, '') [PAYMENT_METHOD],  IsNull( P.BEZ_NUMMER, 0) [PAYMENT_TYPE_ID], " +
                 " IsNull(P.BEZ_RUECK_FLAG, 0) BEZ_RUECK_FLAG, isNull(BG.BEZ_TEXT, '')[GIFT_CARD_TEXT], isNull(BG.BEZ_NUMMER, 0)[GIFT_CARD_PAYNO], IsNull(ART_GEWICHT, 0)[WEIGHT],  RTRIM(LTrim(substring(KAS_INFO,7,2))) [PAYMENT_NO] " +
                 " , IsNull((select top 1 UST_PROZENT from V_UMSATZST where UST_NUMMER = KAS_USTKEY  order by UST_DATUM desc), 0.0) [UST_PROZENT] " +
                 " ,IsNull((select top 1 UST_TEXT from V_UMSATZST where UST_NUMMER = KAS_USTKEY  order by UST_DATUM desc), '') [UST_TEXT], IsNull(RTG_TEXT, '') [REFUND_REASON],  KAS_VK_DATUM as REAL_DATE, KAS_USTKEY   " +
                 " from V_KASSTRNS " +
                 " LEFT JOIN V_WARENGRP on LTRIM(Str(WAR_NUMMER, 10)) = RTRIM(LTrim(SubString(KAS_INFO, 15, 3)))  " +
                 " LEFT JOIN V_KASSREF on KAS_REFNUMMER = KRF_REFNUMMER " +
                 " left join V_ARTIKEL on ART_REFNUMMER = KAS_REFNUMMER " +
                 " left join V_ART_KOPF on ART_WARENGR = AGR_WARENGR and ART_ABTEILUNG = AGR_ABTEILUNG and ART_TYPE = AGR_TYPE and ART_GRPNUMMER = AGR_GRPNUMMER  " +
                 " left join V_LIEFERTN on AGR_LIEFERANT = LIF_NUMMER  " +
                 " left join V_BEZ_ART P on LTRIM(RTrim(substring(KAS_INFO,7,2))) = RTRIM(LTrim(Str(P.BEZ_NUMMER, 3)))   " +
                 " left join V_BEZ_ART BG on LTRIM(Str(BG.BEZ_NUMMER, 10)) = RTRIM(LTrim(SubString(KAS_INFO, 15, 3))) and  SubString(KAS_INFO, 13, 2) = '03' and BG.BEZ_SPEZIAL_BEZ = 1 " +
                 //                " where KAS_DATUM = " + kasDatum.ToString() + " and KAS_FILIALE = " + kasFiliale.ToString() + 
                 //                " and KAS_KASSE = " + kasKasse.ToString() + " and  KAS_BONNR = " + kasBonnr + " " +
                 " left join V_BEZ_ART PC on LTRIM(Str(PC.BEZ_NUMMER, 10)) = RTRIM(LTrim(SubString(KAS_INFO, 6, 2)))  " +
                 " left join V_RETGRUND on  SUBSTRING(KAS_INFO, 14, 1) = LTRIM(Str(RTG_NUMMER, 10)) " +
                 " where KAS_DATUM = @KAS_DATUM and KAS_FILIALE = @KAS_FILIALE " +
                 " and KAS_KASSE = @KAS_KASSE and  KAS_BONNR = @KAS_BONNR " +

                 " union " +
                  "select KAS_DATUM, KAS_FILIALE, KAS_KASSE, KAS_BONNR, KAS_POSNR, IsNull(ART_LFID_NUMMER, '')[ART_LFID_NUMMER], KAS_REFNUMMER, KAS_SATZART, KAS_ZEIT, KAS_RETOUR, IsNull(AGR_GRPNUMMER, 0) [AGR_GRPNUMMER],  isNull(PC.BEZ_TEXT, '')PETTYCASH,  round(KAS_BETRAG * KAS_ANZAHL, 2)[LINE_AMOUNT]," +
                 " KAS_FEHLBON, KAS_BETRAG, KAS_ANZAHL, KAS_INFO, IsNull(WAR_NUMMER, 0) [PROD_GROUP_NO],   WAR_TEXT[PROD_GROUP], KAS_VK_DATUM, " +
                 " IsNull(KRF_BONTEXT2, '') + ' ' + IsNull(KRF_BONTEXT2, '') [ITEM_DESC], IsNull(ART_EINHEIT, '') [SIZE], IsNull(ART_EIGENSCHAFT, '') [COLOR], " +
                 " IsNull(LIF_INDEX, '') [SUPPLIER], IsNull(AGR_TEXT, '') [ITEM_NAME], IsNull(P.BEZ_TEXT, '') [PAYMENT_METHOD],   IsNull( P.BEZ_NUMMER, 0) [PAYMENT_TYPE_ID],   " +
                 " IsNull(P.BEZ_RUECK_FLAG, 0) BEZ_RUECK_FLAG, isNull(BG.BEZ_TEXT, '')[GIFT_CARD_TEXT], isNull(BG.BEZ_NUMMER, 0)[GIFT_CARD_PAYNO], IsNull(ART_GEWICHT, 0)[WEIGHT],  RTRIM(LTrim(substring(KAS_INFO,7,2))) [PAYMENT_NO]  " +
                 " , IsNull((select top 1 UST_PROZENT from V_UMSATZST where UST_NUMMER = KAS_USTKEY  order by UST_DATUM desc), 0.0) [UST_PROZENT] " +
                 " ,IsNull((select top 1 UST_TEXT from V_UMSATZST where UST_NUMMER = KAS_USTKEY order by UST_DATUM desc), '') [UST_TEXT], IsNull(RTG_TEXT, '') [REFUND_REASON], KAS_DATUM as REAL_DATE, KAS_USTKEY  " +

                 " from V_KASSE  " +
                 " LEFT JOIN V_WARENGRP on LTRIM(Str(WAR_NUMMER, 10)) = RTRIM(LTrim(SubString(KAS_INFO, 15, 3))) " +
                 " LEFT JOIN V_KASSREF on KAS_REFNUMMER = KRF_REFNUMMER " +
                 " left join V_ARTIKEL on ART_REFNUMMER = KAS_REFNUMMER " +
                 " left join V_ART_KOPF on ART_WARENGR = AGR_WARENGR and ART_ABTEILUNG = AGR_ABTEILUNG and ART_TYPE = AGR_TYPE and ART_GRPNUMMER = AGR_GRPNUMMER " +
                 " left join V_LIEFERTN on AGR_LIEFERANT = LIF_NUMMER " +
                 " left join V_BEZ_ART P on LTRIM(RTrim(substring(KAS_INFO,7,2))) = RTRIM(LTrim(Str(P.BEZ_NUMMER, 3))) " +
                 " left join V_BEZ_ART BG on LTRIM(Str(BG.BEZ_NUMMER, 10)) = RTRIM(LTrim(SubString(KAS_INFO, 15, 3))) and  SubString(KAS_INFO, 13, 2) = '03' and BG.BEZ_SPEZIAL_BEZ = 1 " +
                 " left join V_BEZ_ART PC on LTRIM(Str(PC.BEZ_NUMMER, 10)) = RTRIM(LTrim(SubString(KAS_INFO, 6, 2))) " +
                 " left join V_RETGRUND on  SUBSTRING(KAS_INFO, 14, 1) = LTRIM(Str(RTG_NUMMER, 10)) " +
                 " where KAS_DATUM = @KAS_DATUM and KAS_FILIALE = @KAS_FILIALE " +
                 " and KAS_KASSE = @KAS_KASSE and  KAS_BONNR = @KAS_BONNR " +


                 " order by 1 ,2, 3, 4, 5  option (force order)";

            DataTable dsDetails = null;
            //    Logging.WriteLog(anSql);
            using (SqlDataAdapter daTrans = new SqlDataAdapter(anSql, ersConnection))
            {
                daTrans.SelectCommand.CommandTimeout = 600;
                daTrans.SelectCommand.Parameters.AddWithValue("@KAS_DATUM", kasDatum);
                daTrans.SelectCommand.Parameters.AddWithValue("@KAS_FILIALE", kasFiliale);
                daTrans.SelectCommand.Parameters.AddWithValue("@KAS_KASSE", kasKasse);
                daTrans.SelectCommand.Parameters.AddWithValue("@KAS_BONNR", kasBonnr);
                dsDetails = new DataTable();
                daTrans.Fill(dsDetails);
            }
            if (dsDetails == null)
            {
                updateDaasExport(kasDatum.ToString(), kasFiliale.ToString(), "1", kasKasse.ToString(), kasBonnr.ToString(), "SALENZ", "", ersConnection);
                return;
            }

            SalesJson salesJson = new SalesJson();
            salesJson.FuturaSale = new Sale();

            string sStaffno = "1";
            salesJson.FuturaSale.BranchNo = kasFiliale.ToString();
            salesJson.FuturaSale.ReceiptId = kasFiliale.ToString() + "/" + kasKasse.ToString() + "/" + kasBonnr.ToString() + "/" + kasDatum.ToString();
            salesJson.FuturaSale.EmployeeId = sStaffno;
            salesJson.FuturaSale.CashierId = sStaffno;
            salesJson.FuturaSale.ReceiptMode = "NORMAL";
            salesJson.FuturaSale.ReceiptState = "FINISHED";
            salesJson.FuturaSale.TillNo = kasKasse.ToString();
            salesJson.FuturaSale.ReceiptNo = kasBonnr.ToString();
            salesJson.FuturaSale.Timestamp = timestamp;
            salesJson.FuturaSale.SaleType = "NORMAL";
            salesJson.FuturaSale.SaleLines = new List<SaleLine>();
            salesJson.FuturaSale.PaymentLines = new List<PaymentLine>();
            salesJson.FuturaSale.CustomerNo = custNo;

            int isVoid = 1;
            double totSales = 0;
            double totAmountPaid = 0;
            bool arefund = false;
            bool apurchase = false;
            String taxName = "";
            String afterPayId = "";
            bool paymentExist = false;
            bool itemExist = false;
            bool giftcardIssue = false;

            String pettyCash = "";
            String paymPetty = "";


            SaleLine saleLine = null;

            int saleLineNo = 0;
            int paymentLineNo = 0;

            String transactionDate = "";
            String stdate = "";

            for (int k = 0; k < dsDetails.Rows.Count; k++)
            {
                DataRow arow = dsDetails.Rows[k];

                if (k == 0)
                {
                    stdate = arow["KAS_VK_DATUM"].ToString();
                    salesJson.FuturaSale.ReceiptId = kasFiliale.ToString() + "/" + kasKasse.ToString() + "/" + kasBonnr.ToString() + "/" + stdate;
                    theDate = stdate.Substring(0, 4) + "-" + stdate.Substring(4, 2) + "-" + stdate.Substring(6, 2);
                    timestamp = theDate + "T" + thetime;
                    salesJson.FuturaSale.Timestamp = timestamp;


                    try
                    {
                        transactionDate = stdate.Substring(0, 4) + "-" + stdate.Substring(4, 2) + "-" + stdate.Substring(6, 2);
                    }
                    catch (Exception e)
                    {
                        transactionDate = theDate;
                    }
                }

                int recType = Convert.ToInt32(arow["KAS_SATZART"]);
                double aqty = Convert.ToDouble(arow["KAS_ANZAHL"]);

                int posNo = Convert.ToInt32(arow["KAS_POSNR"]);

                int kasRetour = 0;
                try
                {
                    kasRetour = Convert.ToInt32(arow["KAS_RETOUR"]);
                }
                catch (Exception e) { }


                if (recType == 17 && aqty == 4)
                {
                    afterPayId = arow["KAS_INFO"].ToString().Trim();
                    if (giftcardIssue)
                    {
                        if (saleLine != null)
                        {
                            saleLine.VoucherNumber = afterPayId;
                            giftcardIssue = false;
                            afterPayId = "";
                        }
                    }
                }

                if (recType == 17)
                {
                    //  String astr = arow["KAS_INFO"].ToString().Trim();
                    //  if (astr.ToUpper().Contains("LAYB"))
                    //  {
                    //      if (saleLine != null)
                    //      {
                    //          saleLine.SalesMode = "LAYAWAY";
                    //      }
                    //  }
                }


                if (recType == 17 && Convert.ToDouble(arow["KAS_ANZAHL"]) == 16)
                {
                    if (saleLine != null)
                    {
                        saleLine.ReturnReason = arow["REFUND_REASON"].ToString().Trim();
                    }
                }
                if (recType == 17 && Convert.ToDouble(arow["KAS_ANZAHL"]) == 5)
                {
                    if (saleLine != null)
                    {
                        DiscountLine discountLine = new DiscountLine();
                        saleLine.DiscountLines.Add(discountLine);
                        discountLine.PosNo = posNo;
                        populateDiscountNZ(arow["KAS_INFO"].ToString(), discountLine);
                    }
                }

                if (recType == 14)
                {
                    pettyCash = arow["PETTYCASH"].ToString();
                }

                if (recType == 16 && arow["KAS_FEHLBON"] != null && arow["KAS_FEHLBON"].ToString().Equals("0") && Convert.ToDouble(arow["KAS_BETRAG"]) != 0)
                {
                    paymentExist = true;
                    PaymentLine paymentLine = new PaymentLine();
                    salesJson.FuturaSale.PaymentLines.Add(paymentLine);
                    paymentLine.PosNo = posNo;

                    double paymentAmount = Convert.ToDouble(arow["KAS_BETRAG"]);

                    if (arow["BEZ_RUECK_FLAG"].ToString().Equals("1"))
                    {
                        paymentAmount *= -1;
                    }


                    paymentLine.Amount = paymentAmount;
                    paymentLine.PaymentType = arow["PAYMENT_METHOD"].ToString();
                    paymentLine.PaymentTypeId = Convert.ToInt32(arow["PAYMENT_TYPE_ID"].ToString());

                    if (arow["PAYMENT_METHOD"].ToString().ToUpper().Contains("PETTY"))
                    {
                        paymentLine.PaymentType = arow["PAYMENT_METHOD"].ToString() + " " + pettyCash;
                        pettyCash = "";
                        paymPetty = "petty cash";
                    }
                    paymentLine.RefNumber = afterPayId;
                    afterPayId = "";

                    paymentLine.Currency = nzCurrency;

                    totAmountPaid += paymentAmount;

                }

                if (recType == 15)
                {

                    if (arow["KAS_FEHLBON"] == null || !arow["KAS_FEHLBON"].ToString().Equals("0"))
                    {
                        continue;
                    }


                    String anInfo = arow["KAS_INFO"].ToString().Trim();
                    String[] theFields = anInfo.Split(' ');
                    String amtField = "";
                    for (int iii = 0; iii < theFields.Length; iii++)
                    {
                        String afield = theFields[iii].Trim();
                        if (afield.Equals(""))
                        {
                            continue;
                        }
                        amtField = afield.Substring(0, afield.Length - 1);
                    }

                    int idisc = Logging.strToIntDef(amtField, 0);
                    itemExist = true;

                    giftcardIssue = false;

                    saleLine = new SaleLine();
                    salesJson.FuturaSale.SaleLines.Add(saleLine);
                    saleLine.PosNo = posNo;

                    saleLine.DiscountLines = new List<DiscountLine>();

                    saleLine.SalesMode = "MODE_NORMAL";

                    itemExist = true;

                    saleLine.Qty = Math.Abs(aqty);
                    saleLine.Price = Math.Abs(Convert.ToDouble(arow["KAS_BETRAG"]));
                    saleLine.OriginalPrice = Math.Abs((idisc / 100.00));
                    saleLine.LineValueGross = Convert.ToDouble(arow["LINE_AMOUNT"]);
                    double anamount = Convert.ToDouble(arow["LINE_AMOUNT"]);
                    double anamountAbs = Math.Abs(anamount);

                    double atax = Convert.ToDouble(arow["UST_PROZENT"]);

                    saleLine.VatPercent = atax;

                    double anamountExTaxAbs = anamountAbs;
                    if (atax != 0)
                    {
                        anamountExTaxAbs = anamountAbs / (1 + (1 / atax));
                    }

                    if (anamount < 0)
                    {
                        anamountExTaxAbs = -1 * anamountExTaxAbs;
                    }
                    saleLine.VatHeadEntityId = (Convert.ToInt32(arow["KAS_USTKEY"])).ToString();
                    saleLine.VatAmount = anamountAbs - anamountExTaxAbs;
                    //                    saleLine.LineValueGross = anamount;
                    saleLine.LineValueGross = anamountAbs;
                    saleLine.LineValueNet = anamountExTaxAbs;


                    String kasInfo = arow["KAS_INFO"].ToString();
                    if (kasInfo.Length > 5)
                        sStaffno = kasInfo.Substring(0, 6);


                    saleLine.SalesPersonId = sStaffno;
                    saleLine.ProductGroupId = Convert.ToInt32(arow["PROD_GROUP_NO"]);
                    saleLine.SkuId = Convert.ToInt32(arow["KAS_REFNUMMER"]);
                    saleLine.VoucherNumber = "";
                    saleLine.VoucherPaymentTypeId = "";
                    saleLine.VoucherPaymentType = "";
                    if (saleLine.SkuId == 0)
                    {
                        String giftIssue = arow["GIFT_CARD_TEXT"].ToString();
                        if (!giftIssue.Equals(""))
                        {
                            saleLine.VoucherNumber = arow["GIFT_CARD_PAYNO"].ToString();
                            giftcardIssue = true;
                            saleLine.VoucherPaymentTypeId = arow["GIFT_CARD_PAYNO"].ToString();
                            saleLine.VoucherPaymentType = arow["GIFT_CARD_TEXT"].ToString();
                        }
                    }

                    saleLine.ReturnReason = "";
                    bool isReturn = false;
                    saleLine.SaleLineType = "SALE";
                    if (kasRetour == 1)
                    {
                        saleLine.SaleLineType = "RETURN";
                        arefund = true;
                        isReturn = true;
                        saleLine.Qty = -1 * saleLine.Qty;

                        saleLine.LineValueGross = -1 * saleLine.LineValueGross;
                        saleLine.LineValueNet = -1 * saleLine.LineValueNet;
                        saleLine.VatAmount = -1 * saleLine.VatAmount;
                        saleLine.ReturnReason = arow["REFUND_REASON"].ToString();
                        saleLine.SalesMode = "RETOUR";

                    }
                    totSales += saleLine.LineValueGross;

                    saleLine.Wac = getWacNZ(saleLine.SkuId, kasFiliale, kasDatum, isReturn, ersConnection);

                }





            }

            if (arefund)
            {
                salesJson.FuturaSale.SaleType = "RETURN";
            }

            if (!itemExist)
            {
                salesJson.FuturaSale.SaleType = "LAYBY";
            }

            salesJson.FuturaSale.EmployeeId = sStaffno;
            salesJson.FuturaSale.CashierId = sStaffno;
            salesJson.FuturaSale.TransactionDate = transactionDate;
            String ajsonStr = SimpleJson.SerializeObject(salesJson).ToString();
            String md5Contents = Logging.CreateMD5(ajsonStr);

            String storedMd5 = getMd5(kasDatum.ToString(), kasFiliale.ToString(), "1", kasKasse.ToString(), kasBonnr.ToString(), "SALENZ", Logging.strToInt64Def(dcSetup.SaleUpdate, 0),
                ersConnection);

            if (!md5Contents.Equals(storedMd5))
            {
                Logging.WriteDebug("JSON " + SimpleJson.SerializeObject(salesJson).ToString(), dcSetup.Debug);
                SendNewMessageQueue(SimpleJson.SerializeObject(salesJson).ToString(), dcSetup.SalesQueueName);
            }
            updateDaasExport(kasDatum.ToString(), kasFiliale.ToString(), "1", kasKasse.ToString(), kasBonnr.ToString(), "SALENZ", md5Contents, ersConnection);

            DateTime anow = DateTime.Now;
            String snow = anow.ToString("yyyyMMddhhmmss");
            dcSetup.SaleUpdateNZ = snow;


        }



        private void populateDiscount(SqlConnection ersConnection)
        {
            lstDiscount.Clear();
            String anSql = "select Cast(PAG_MANDANT as varchar) + '~' + Cast(PAG_NUMMER as varchar) as DISCOUNT, PAG_TEXT from PA_GRUND";
            using (SqlCommand cmd = new SqlCommand(anSql, ersConnection))
            {
                using (SqlDataReader areader = cmd.ExecuteReader())
                {
                    while (areader.Read())
                    {
                        String akey = areader[0].ToString();
                        String avalue = areader[1].ToString();
                        lstDiscount.Add(akey, avalue);
                    }
                }
            }
        }

        private void populateDiscountNZ(SqlConnection ersConnection)
        {
            lstDiscountNZ.Clear();
            String anSql = "select  Cast(PAG_NUMMER as varchar) as DISCOUNT, PAG_TEXT from PA_GRUND";
            using (SqlCommand cmd = new SqlCommand(anSql, ersConnection))
            {
                using (SqlDataReader areader = cmd.ExecuteReader())
                {
                    while (areader.Read())
                    {
                        String akey = areader[0].ToString();
                        String avalue = areader[1].ToString();
                        lstDiscountNZ.Add(akey, avalue);
                    }
                }
            }
        }

        public void process()
        {
            Logging.WriteDebug("Starting Process ", dcSetup.Debug);
            SqlConnection ersConnection = null;
            ersConnection = openERSSQLConnection();
            if (ersConnection == null)
            {
                return;
            }

            SqlConnection ersConnectionNZ = null;
            ersConnectionNZ = openERSNZSQLConnection();
            if (ersConnectionNZ == null)
            {
                return;
            }


            try
            {
                if (!dcSetup.BlockSales)
                {
                    getSales(ersConnection);
                    salesSecondPass(ersConnection);
                }

                if (!dcSetup.BlockSalesNZ)
                {
                    getSalesNZ(ersConnectionNZ);
                    salesSecondPassNZ(ersConnectionNZ);
                }


                if (!dcSetup.BlockProducts)
                {
                    getProducts_1(ersConnection);
                }

                if (!dcSetup.BlockPrices)
                {
                    getPrices_1(ersConnection);
                }

                if (!dcSetup.BlockPricesNZ)
                {
                    getPrices_1NZ(ersConnectionNZ);
                }


                if (!dcSetup.BlockPermanentMarkdowns)
                {
                    getMarkdowns(ersConnection);
                }

                if (!dcSetup.BlockPermanentMarkdownsNZ)
                {
                    getMarkdownsNZ(ersConnection, ersConnectionNZ);
                }

                if (!dcSetup.BlockInventory)
                {
                    getInventory(ersConnection);
                }

                if(!dcSetup.BlockInventoryAdjustments)
                {
                    getInventoryAdjustments(ersConnection);
                }

                if (!dcSetup.BlockInventoryAdjustmentsNZ)
                {
                    getInventoryAdjustmentsNZ(ersConnectionNZ);
                }


                if (!dcSetup.BlockTransfersFromHO)
                {
                    getRetailTransfers(ersConnection);
                }

                if (!dcSetup.BlockTransfersFromHONZ)
                {
                    getRetailTransfersNZ(ersConnectionNZ);
                }


                if (!dcSetup.BlockTransfersFromBranches)
                {
                    getTransfersFromBranches(ersConnection);
                }

                if (!dcSetup.BlockTransfersFromBranchesNZ)
                {
                    getTransfersFromBranchesNZ(ersConnectionNZ);
                }


                if (!dcSetup.BlockOrders)
                {
                    getPOs(ersConnection);
                }

                if(!dcSetup.BlockShipments)
                {
                    getShipments(ersConnection);
                }

                if (!dcSetup.BlockShipmentsNZ)
                {
                    getShipmentsNZ(ersConnectionNZ);
                }


                ACounter++;
                Logging.WriteDebug("ACounter: " + ACounter.ToString(), dcSetup.Debug);
                if (ACounter >= 100)
                {
                    ACounter = 0;
                }



                int aRem = 0;
               if (aRem == 0 && (!dcSetup.BlockLocations))
                {
                    getLocations(ersConnection);
                }
                ersConnection.Close();
            }
            finally
            {
                try
                {
                    ersConnection.Close();
                }
                catch { }
            }

            try
            {
                houseKeeping(AppDomain.CurrentDomain.BaseDirectory + "\\log", "log*.txt");
                houseKeeping(AppDomain.CurrentDomain.BaseDirectory + "\\log", "errorlog*.txt");
            }
            catch { }


        }

        private void getProducts_1(SqlConnection ersConnection)
        {
            Logging.WriteLog("Starting getProducts");
            String lastUpdate = dcSetup.ProductUpdate;
            String sqlLastUpdate = Logging.FuturaDateTimeAddMins(lastUpdate, -30);

            String anSql = "SELECT FORMAT(max(modify_date), 'yyyyMMddhhmmss')[MODIFY_DATE] " +
                " FROM sys.tables where name in ( 'ART_KOPF', 'ARTIKEL'   )";
            String sAnsModify = "0";
            using (SqlCommand cmd = new SqlCommand(anSql, ersConnection))
            {
                var aresult = cmd.ExecuteScalar();
                if (aresult != null)
                {
                    sAnsModify = aresult.ToString();
                }
            }


            //if (sAnsModify.CompareTo(sqlLastUpdate) < 0)
            //{
            //    return;
            //}

            String sTop = " ";
            if (dcSetup.ResultSet > 0)
            {
                sTop = " top " + dcSetup.ResultSet.ToString();
            }

            String supdateDate = "0";
            String supdateTime = "0";
            if (sqlLastUpdate.ToString().Length == 14)
            {
                supdateDate = sqlLastUpdate.ToString().Substring(0, 8);
                supdateTime = sqlLastUpdate.ToString().Substring(8);
            }

            anSql = "select " + sTop + " AGR_WARENGR [ProductGroup], AGR_ABTEILUNG[Subgroup], AGR_TYPE[Type],AGR_GRPNUMMER [GroupNumber], AGR_TEXT [SupplierItemDescription], " +
                " AGR_BONTEXT [ReceiptText], ISNULL(TBL.TEXT, '') [LongDescription], AGR_LIEFERANT[DeliveryType], AGR_LFARTGRP[SupplierItemGroup], " +
                "  isnull(LIN_TEXT, '')[SupplierItemGroupIndex], ISNULL(EKB_NUMMER, 0)[ORIGEN], ISNULL(EKB_TEXT, '')[ORIGEN_TEXT], AGR_SERIENFLAG[SerialNumberEntry] , " +
                " AGR_VK_BEREICH[SalesAreaNo], ISNULL(VKB_TEXT, '')[SalesArea], AGR_ETITYP[LabelType], AGR_ETIZAHL[LabelPerPiece], " +
                " cast(AGR_ULOG_DATE as varchar) + right('000000' + cast(AGR_ULOG_TIME AS VARCHAR), 6), WAR_TEXT[ProductGroupDescription], ABT_TEXT[SubgroupDescription], WAT_TEXT[TypeDescription] " +
                " from V_ART_KOPF " +
                " left join LFARTGRP on LIN_MANDANT = 1 and LIN_TEXTCODE = AGR_LFARTGRP_TCOD " + 
                " LEFT JOIN (SELECT ATX_WARENGR, ATX_ABTEILUNG, ATX_TYPE, ATX_GRPNUMMER, STUFF((SELECT ' ' + T2.ATX_TEXT " +
                "     FROM V_ART_TEXT T2 " +
                "      WHERE T2.ATX_MANDANT = 1 AND T2.ATX_WARENGR = T1.ATX_WARENGR AND T1.ATX_ABTEILUNG = T2.ATX_ABTEILUNG AND T1.ATX_TYPE = T2.ATX_TYPE AND T1.ATX_GRPNUMMER = T2.ATX_GRPNUMMER " +
                "       FOR XML PATH('')), 1, 1, '')[TEXT]  " +
                "       FROM V_ART_TEXT  T1 WHERE T1.ATX_MANDANT = 1 " +
                "        GROUP BY ATX_WARENGR, ATX_ABTEILUNG, ATX_TYPE, ATX_GRPNUMMER)TBL " +
                "        ON TBL.ATX_WARENGR = AGR_WARENGR AND TBL.ATX_ABTEILUNG = AGR_ABTEILUNG AND TBL.ATX_TYPE = AGR_TYPE AND TBL.ATX_GRPNUMMER = AGR_GRPNUMMER " +
                " LEFT JOIN V_EK_BER ON EKB_NUMMER = AGR_EK_BEREICH AND  EKB_MANDANT = 1 " +
                " LEFT JOIN V_VK_BER ON VKB_NUMMER = AGR_VK_BEREICH AND  VKB_MANDANT = 1 " +
                " LEFT JOIN WARENGRP ON WAR_MANDANT = 1 AND WAR_NUMMER = AGR_WARENGR " +
                " LEFT JOIN  ABTEIL ON ABT_MANDANT = 1 AND ABT_NUMMER = AGR_ABTEILUNG " +
                " LEFT JOIN WARENTYP ON WAT_MANDANT = 1 AND WAT_NUMMER = AGR_TYPE " +
                " where AGR_MANDANT = 1 AND  (AGR_ULOG_DATE > " + supdateDate + " or (AGR_ULOG_DATE = " + supdateDate + " and AGR_ULOG_TIME >= 0" + supdateTime + ") )";
            //"cast(AGR_ULOG_DATE as varchar) + right('000000' + cast(AGR_ULOG_TIME AS VARCHAR), 6) >= '" + sqlLastUpdate + "'";

            Logging.WriteDebug(anSql, dcSetup.Debug)
;

            String attrSql = "select ARC_CODE, ARC_VALUE, ARC_TEXT,  ISNULL(AEN_TEXT, '') [DESCRIPTION ]  FROM V_ART_CODE " +
                "LEFT JOIN ACODENUM ON AEN_MANDANT = ARC_MANDANT AND AEN_CODE = ARC_CODE AND AEN_ENUM = ARC_VALUE WHERE " +
                " ARC_MANDANT = 1 AND ARC_WARENGR = @ARC_WARENGR AND ARC_ABTEILUNG = @ARC_ABTEILUNG AND ARC_TYPE = @ARC_TYPE AND ARC_GRPNUMMER = @ARC_GRPNUMMER";


            using (SqlCommand cmdmain = new SqlCommand(anSql, ersConnection))
            {
                using (SqlDataReader mainReader = cmdmain.ExecuteReader())
                {
                    while (mainReader.Read())
                    {
                        ItemsJson itemsJson = new ItemsJson();
                        StockItem anItem = new StockItem();
                        itemsJson.Item = anItem;
                        anItem.Skus = null;
                        anItem.ProductGroup = Convert.ToInt32(mainReader["ProductGroup"]);
                        anItem.Subgroup = Convert.ToInt32(mainReader["Subgroup"]);
                        anItem.Type = Convert.ToInt32(mainReader["Type"]);
                        anItem.GroupNumber = Convert.ToInt32(mainReader["GroupNumber"]);
                        anItem.SupplierItemDescription = mainReader["SupplierItemDescription"].ToString();
                        anItem.ReceiptText = mainReader["ReceiptText"].ToString();
                        anItem.LongDescription = mainReader["LongDescription"].ToString();
                        anItem.DeliveryType = Convert.ToInt32(mainReader["DeliveryType"]);
                        anItem.SupplierItemGroup = Convert.ToInt32(mainReader["SupplierItemGroup"]);
                        anItem.Origin = Convert.ToInt32(mainReader["ORIGEN"]);
                        anItem.OriginText = mainReader["ORIGEN_TEXT"].ToString();
                        anItem.SerialNumberEntry = Convert.ToInt32(mainReader["SerialNumberEntry"]);
                        anItem.SalesAreaNo = Convert.ToInt32(mainReader["SalesAreaNo"]);
                        anItem.SalesArea = mainReader["SalesArea"].ToString();
                        anItem.SupplierItemGroupIndex = mainReader["SupplierItemGroupIndex"].ToString();

                      //  anItem.GroupNumberDescription = mainReader["SupplierItemDescription"].ToString();
                        anItem.ProductGroupDescription = mainReader["ProductGroupDescription"].ToString();
                        anItem.TypeDescription = mainReader["TypeDescription"].ToString();
                        anItem.SubgroupDescription = mainReader["SubgroupDescription"].ToString();

                        anItem.ItemAttributes = new List<StockAttribute>();
                        using (SqlCommand cmd = new SqlCommand(attrSql, ersConnection))
                        {
                            cmd.Parameters.AddWithValue("@ARC_WARENGR", anItem.ProductGroup);
                            cmd.Parameters.AddWithValue("@ARC_ABTEILUNG", anItem.Subgroup);
                            cmd.Parameters.AddWithValue("@ARC_TYPE", anItem.Type);
                            cmd.Parameters.AddWithValue("@ARC_GRPNUMMER", anItem.GroupNumber);
                            using (SqlDataReader areader = cmd.ExecuteReader())
                            {
                                while (areader.Read())
                                {
                                    StockAttribute anAttribute = new StockAttribute();
                                    anItem.ItemAttributes.Add(anAttribute);
                                    anAttribute.Code = areader.GetString(0);
                                    anAttribute.Value = areader.GetInt32(1);
                                    anAttribute.Text = areader.GetString(2);
                                    anAttribute.Description = areader.GetString(3);
                                }

                            }
                        }

                        String skuSql = "select ART_REFNUMMER[SkuId], ART_SORTIERUNG[Sort], ART_EINHEITTEXT[UnitText], ART_EIGENTEXT[VariantText], ART_LFID_NUMMER[RefNummer], " +
                            " ART_SAISON[StatisticalPeriodNo], SPE_TEXT[StatisticalPeriod], ART_MAXRABATT[MaximumDiscount], ART_KEIN_RABATT[FixedPrice], " +
                            //                           " ART_CMP_TYP[QtyTypeForComparativePrice], ART_CMP_ISTMENGE[ComparativeQtyForComparativePrice], ART_CMP_REFMENGE[QtyForComparativePrice], " +
                            " ART_CMP_TYP[QtyTypeForComparativePrice], ART_CMP_ISTMENGE[QtyForComparativePrice], ART_CMP_REFMENGE[ComparativeQtyForComparativePrice], " +
                            " ART_ZWEIT_LFID[POSupplierItemNumber], ART_VKPREIS[RT_Price], ART_EKWAEHRUNG[Currency], ART_EK_DM [PurchasePrice],ART_ZWEITLIEFERANT [PO_Supplier], " +
                            " case " +
                            "   when ART_SET_EKGEW_MODE<> 0 then ART_EK_GEWICHTET " +
                            "   else ART_EK_DM " +
                            " end[WeightedAverageCost] " +
                            " from V_ARTIKEL " +
                            " LEFT JOIN V_STATPERI ON SPE_SAISON = ART_SAISON AND SPE_MANDANT = 1" +
                            " where ART_MANDANT = 1 AND ART_WARENGR = @ART_WARENGR AND ART_ABTEILUNG = @ART_ABTEILUNG AND ART_TYPE = @ART_TYPE AND ART_GRPNUMMER = @ART_GRPNUMMER";


                        anItem.Skus = new List<Sku>();

                        using (SqlCommand cmd = new SqlCommand(skuSql, ersConnection))
                        {
                            cmd.Parameters.AddWithValue("@ART_WARENGR", anItem.ProductGroup);
                            cmd.Parameters.AddWithValue("@ART_ABTEILUNG", anItem.Subgroup);
                            cmd.Parameters.AddWithValue("@ART_TYPE", anItem.Type);
                            cmd.Parameters.AddWithValue("@ART_GRPNUMMER", anItem.GroupNumber);
                            using (SqlDataReader areader = cmd.ExecuteReader())
                            {
                                while (areader.Read())
                                {
                                    Sku sku = new Sku();
                                    anItem.Skus.Add(sku);
                                    sku.SkuId = Logging.strToIntDef(areader["SkuId"].ToString(), 0);
                                    sku.Sort = Logging.strToIntDef(areader["Sort"].ToString(), 0);
                                    sku.UnitText = areader["UnitText"].ToString();
                                    sku.VariantText = areader["VariantText"].ToString();
                                    sku.SpltmNo = areader["RefNummer"].ToString();
                                    sku.StatisticalPeriodNo = Logging.strToIntDef(areader["StatisticalPeriodNo"].ToString(), 0);
                                    sku.StatisticalPeriod = areader["StatisticalPeriod"].ToString();
                                    sku.MaximumDiscount = Logging.strToDoubleDef(areader["MaximumDiscount"].ToString(), 0);
                                    sku.FixedPrice = Logging.strToDoubleDef(areader["FixedPrice"].ToString(), 0);
                                 //   sku.WeightedAverageCost = Logging.strToDoubleDef(areader["WeightedAverageCost"].ToString(), 0);
                                    sku.POSupplierItemNumber = areader["POSupplierItemNumber"].ToString();
                                    sku.QtyTypeForComparativePrice = areader["QtyTypeForComparativePrice"].ToString();
                                    sku.ComparativeQtyForComparativePrice = Logging.strToDoubleDef(areader["ComparativeQtyForComparativePrice"].ToString(), 0);
                                    sku.QtyForComparativePrice = Logging.strToDoubleDef(areader["QtyForComparativePrice"].ToString(), 0);
                                    sku.RT_Price = Logging.strToDoubleDef(areader["RT_Price"].ToString(), 0);
                                    sku.Currency = areader["Currency"].ToString();
                                    sku.PP_Price = Logging.strToDoubleDef(areader["PurchasePrice"].ToString(), 0);
                                    sku.PO_Supplier = areader["PO_Supplier"].ToString();
                                    sku.EanCodes = new List<EanCode>();
                                    String eanSql = "SELECT AEA_EANCODE, AEA_SORTIERUNG FROM V_ART_EANS WHERE AEA_MANDANT = 1 AND AEA_REFNUMMER = @AEA_REFNUMMER ";

                                    using (SqlCommand cmdEan = new SqlCommand(eanSql, ersConnection))
                                    {
                                        cmdEan.Parameters.AddWithValue("@AEA_REFNUMMER", sku.SkuId);
                                        using (SqlDataReader eanReader = cmdEan.ExecuteReader())
                                        {
                                            while (eanReader.Read())
                                            {
                                                EanCode eanCode = new EanCode();
                                                sku.EanCodes.Add(eanCode);
                                                eanCode.ECode = eanReader["AEA_EANCODE"].ToString();
                                                eanCode.Sorting = Logging.strToIntDef(eanReader["AEA_SORTIERUNG"].ToString(), 0);
                                            }
                                        }
                                    }

                                    String attrSkuSql = "select ADC_CODE, ADC_VALUE, ADC_TEXT,  ISNULL(AEN_TEXT, '') [DESCRIPTION] from V_ART_DCOD " +
                                    " LEFT JOIN ACODENUM ON AEN_MANDANT = ADC_MANDANT AND AEN_CODE = ADC_CODE AND AEN_ENUM = ADC_VALUE " +
                                    " where ADC_MANDANT = 1 AND ADC_REFNUMMER = @ADC_REFNUMMERY";
                                    sku.skuAttributes = new List<StockAttribute>();

                                    using (SqlCommand attrCmd = new SqlCommand(attrSkuSql, ersConnection))
                                    {

                                        try
                                        {
                                            Logging.WriteDebug("Here + " + sku.SkuId, dcSetup.Debug);
                                            attrCmd.Parameters.AddWithValue("@ADC_REFNUMMERY", sku.SkuId);
                                            using (SqlDataReader attrReader = attrCmd.ExecuteReader())
                                            {
                                                while (attrReader.Read())
                                                {
                                                    StockAttribute anAttribute = new StockAttribute();
                                                    sku.skuAttributes.Add(anAttribute);
                                                    anAttribute.Code = attrReader["ADC_CODE"].ToString();
                                                    anAttribute.Text = attrReader["ADC_TEXT"].ToString();
                                                    anAttribute.Value = Logging.strToIntDef(attrReader["ADC_VALUE"].ToString(), 0);
                                                    anAttribute.Description = attrReader["DESCRIPTION"].ToString();
                                                }
                                            }
                                        }
                                        catch (Exception e)
                                        {
                                            Logging.WriteErrorLog("here " + sku.SkuId + " " + e.Message);
                                        }

                                    }



                                }
                            }
                        }

                        String ajsonStr = SimpleJson.SerializeObject(itemsJson).ToString();
                        String md5Contents = Logging.CreateMD5(ajsonStr);

                        String storedMd5 = getMd5(anItem.ProductGroup.ToString(), anItem.Subgroup.ToString(), anItem.Type.ToString(), anItem.GroupNumber.ToString(), "1", "PRODUCT", Logging.strToInt64Def(dcSetup.ProductUpdate, 0),
                            ersConnection);

                        if (!md5Contents.Equals(storedMd5))
                        {
                            Logging.WriteDebug("JSON " + SimpleJson.SerializeObject(itemsJson).ToString(), dcSetup.Debug);
                            SendNewMessageQueue(SimpleJson.SerializeObject(itemsJson).ToString(), dcSetup.ProductsQueueName);
                            updateDaasExport(anItem.ProductGroup.ToString(), anItem.Subgroup.ToString(), anItem.Type.ToString(), anItem.GroupNumber.ToString(), "1", "PRODUCT", md5Contents, ersConnection);
                        }



                    }
                }
            }
            DateTime anow = DateTime.Now;
            String snow = anow.ToString("yyyyMMddhhmmss");
            dcSetup.ProductUpdate = snow;


        }


        private void getMarkdownsNZ(SqlConnection ersConnection, SqlConnection ersNZConnection)
        {
            Logging.WriteLog("Starting getMarkdownsNZ");
            String lastUpdate = dcSetup.PermanentMarkdownNZUpdate;
            if (lastUpdate.Length < 5)
            {
                lastUpdate = "20000101121223";


                String delSql = "delete from " + DaasExportTable + " where DAAS_SET_NAME = 'MARKDOWN_NZ' ";
                using (SqlCommand cmd = new SqlCommand(delSql, ersConnection))
                {
                    cmd.ExecuteNonQuery();
                }

            }

            int dateFrom = dcSetup.MarkdownsNZFromDate;
            int dateTo = dcSetup.MarkdownsNZToDate;

            if (dateFrom != 0 || dateTo != 0)
            {
                String deleteSql = "delete from " + DaasExportTable + " where DAAS_SET_NAME = 'MARKDOWN_NZ' and DAAS_KEY1 >= @mindate and DAAS_KEY1 <= @maxDate  ";
                using (SqlCommand cmd = new SqlCommand(deleteSql, ersConnection))
                {
                    cmd.Parameters.AddWithValue("@mindate", dateFrom.ToString());
                    cmd.Parameters.AddWithValue("@maxDate", dateTo.ToString());
                    cmd.ExecuteNonQuery();
                }

            }

            String sqlLastUpdate = Logging.FuturaDateTimeAddMins(lastUpdate, -60 * 24).Substring(0, 8);
            int mdownInitialDate = dcSetup.PermanentMarkDownsNZInitialDate;

            String sTop = " ";
            if (dcSetup.ResultSet > 0)
            {
                sTop = " top " + dcSetup.ResultSet.ToString();
            }

            String anSql = "  select distinct PDT_DATUM, PDT_NUMMER, PDT_WARENGR, PDT_ABTEILUNG, PDT_TYPE, PDT_GRPNUMMER, PDT_REFNUMMER, PAE_TEXT, PDT_NEUVKP_DATUM from PR_AEND " +
                " join PR_ZEIL on PAE_MANDANT = PDT_MANDANT AND PAE_DATUM = PDT_DATUM AND PAE_NUMMER = PDT_NUMMER " +
                " join PR_LIN on PLN_MANDANT = PAE_MANDANT  AND PAE_DATUM = PLN_DATUM AND PLN_NUMMER = PDT_NUMMER and PLN_PREISLINIE IN (-2, 11)  and PLN_NEUVKPREIS> 0.001 " +
                //            " where PAE_GEDRUCKT = 1 and PAE_MANDANT = 1 and PAE_DATUM >= " + sqlLastUpdate + " and PAE_DATUM >= " + mdownInitialDate + "    order by 1, 2 ";
                " where PAE_GEDRUCKT = 1 and PAE_MANDANT = 1 order by 1, 2 ";

            Dictionary<string, Markdown> prAends = new Dictionary<string, Markdown>();
            Dictionary<string, Markdown> prAendsEff = new Dictionary<string, Markdown>();

            Logging.WriteDebug(anSql, dcSetup.Debug);

            using (SqlCommand cmd = new SqlCommand(anSql, ersConnection))
            {
                cmd.CommandTimeout = 1200;
                using (SqlDataReader areader = cmd.ExecuteReader())
                {
                    while (areader.Read())
                    {
                        Markdown markdown = new Markdown();

                        int pdtDatum = areader.GetInt32(0);
                        int pdtNummer = Logging.strToIntDef(areader["PDT_NUMMER"].ToString(), 0);
                        int pdtWareng = Logging.strToIntDef(areader["PDT_WARENGR"].ToString(), 0);
                        int pdtAbte = Logging.strToIntDef(areader["PDT_ABTEILUNG"].ToString(), 0);
                        int pdtType = Logging.strToIntDef(areader["PDT_TYPE"].ToString(), 0);
                        int pdtGroupNo = Logging.strToIntDef(areader["PDT_GRPNUMMER"].ToString(), 0);
                        int pdtRefNo = Logging.strToIntDef(areader["PDT_REFNUMMER"].ToString(), 0);
                        int pdtEffectiveDate = Logging.strToIntDef(areader["PDT_NEUVKP_DATUM"].ToString(), 0);
                        String paeText = areader.GetString(7);

                        String aKey = pdtDatum.ToString() + "~" + pdtRefNo;
                        String aKeyEff = pdtEffectiveDate.ToString() + "~" + pdtRefNo;
                        if (pdtRefNo == 0)
                        {
                            aKey = pdtDatum.ToString() + "~" + pdtWareng + "~" + pdtAbte + "~" + pdtType + "~" + pdtGroupNo;
                            aKeyEff = pdtEffectiveDate.ToString() + "~" + pdtWareng + "~" + pdtAbte + "~" + pdtType + "~" + pdtGroupNo;
                        }

                        markdown.PdtDatum = pdtDatum;
                        markdown.PdtNummer = pdtNummer;
                        markdown.PdtText = paeText;
                        markdown.PdtEffectiveDate = pdtEffectiveDate;
                        if (!prAends.ContainsKey(aKey))
                        {
                            prAends.Add(aKey, markdown);
                        }

                        if (!prAendsEff.ContainsKey(aKeyEff))
                        {
                            prAendsEff.Add(aKeyEff, markdown);
                        }


                    }

                }
            }

            DateTime anow = DateTime.Now;
            DateTime tnow = anow.AddDays(1); // added one day ...

            String snow = tnow.ToString("yyyyMMdd");

            String lastUpdateShort = lastUpdate.Substring(0, 8);

            String mainSql = "select SAD_DATUM,SAI_WARENGR, SAI_ABTEILUNG, SAI_TYPE, SAI_GRPNUMMER, SAI_REFNUMMER, SAD_FILIALE,SAD_ZAHL,SAD_WERT, SAD_ID from STATADTA " +
                " JOIN STATAIDX on SAD_ID = SAI_ID   " +
                " JOIN FILIALEN on SAD_FILIALE = FIL_NUMMER " +
                " where SAD_KENNUNG = 1 and ((SAD_DATUM >= " + mdownInitialDate + " and SAD_DATUM >= " +
                Logging.FuturaDateTimeAddMins(lastUpdateShort + "060606", -48 * 60).Substring(0, 8) +
                " and SAD_DATUM <= " + snow + ") OR ( SAD_DATUM >= " + dateFrom + " and SAD_DATUM <= " + dateTo + " ) ) " +
                " and FIL_ARTTRANS_VK in (0, -2) " +
                " order by SAD_DATUM, SAI_REFNUMMER, SAD_FILIALE";

            Logging.WriteDebug(mainSql, dcSetup.Debug);

            using (SqlCommand cmd = new SqlCommand(mainSql, ersNZConnection))
            {
                cmd.CommandTimeout = 1200;
                SqlDataReader areader = cmd.ExecuteReader();
                while (areader.Read())
                {
                    String sadDatum = areader["SAD_DATUM"].ToString();
                    String satWarenger = areader["SAI_WARENGR"].ToString();
                    String satAbteilung = areader["SAI_ABTEILUNG"].ToString();
                    String satGroupNo = areader["SAI_GRPNUMMER"].ToString();
                    String satType = areader["SAI_TYPE"].ToString();
                    String sku = areader["SAI_REFNUMMER"].ToString();
                    String branch = areader["SAD_FILIALE"].ToString();
                    double qty = Logging.strToDoubleDef(areader["SAD_ZAHL"].ToString(), 0);
                    double value = Math.Truncate((Logging.strToDoubleDef(areader["SAD_WERT"].ToString(), 0) + 0.005) * 100) / 100.00;
                    if (value < 0)
                    {
                        value = Math.Truncate((Logging.strToDoubleDef(areader["SAD_WERT"].ToString(), 0) - 0.005) * 100) / 100.00;
                    }


                    String keyDatum = Logging.FuturaDateTimeAddMins(sadDatum + "060606", -24 * 60).Substring(0, 8);

                    String akey = keyDatum + "~" + sku;
                    String akeyEff = sadDatum + "~" + sku;
                    if (sku.Equals("0"))
                    {
                        akey = keyDatum + "~" + satWarenger + "~" + satAbteilung + "~" + satType + "~" + satGroupNo;
                        akeyEff = sadDatum + "~" + satWarenger + "~" + satAbteilung + "~" + satType + "~" + satGroupNo;
                    }




                    MarkdownJson ajson = new MarkdownJson();
                    ajson.PermanentMD = new PermanentMarkdown();
                    ajson.PermanentMD.Branch = branch;
                    ajson.PermanentMD.Date = sadDatum;
                    ajson.PermanentMD.ID = sadDatum + "/0";
                    ajson.PermanentMD.Description = "manual markdown";
                    ajson.PermanentMD.SadId = areader["SAD_ID"].ToString();
                    ajson.PermanentMD.EffectiveDate = sadDatum;
                    Markdown amarkdown = null;
                    // if (prAends.ContainsKey(akey))
                    // {
                    //     amarkdown = prAends[akey];
                    // }
                    // if(amarkdown != null)
                    // {
                    //     ajson.PermanentMD.Description = amarkdown.PdtText;
                    //     ajson.PermanentMD.ID = amarkdown.PdtDatum.ToString() + "/" + amarkdown.PdtNummer;
                    //     ajson.PermanentMD.EffectiveDate = amarkdown.PdtEffectiveDate.ToString();
                    // }

                    if (prAendsEff.ContainsKey(akeyEff))
                    {
                        amarkdown = prAendsEff[akeyEff];
                    }
                    if (amarkdown != null)
                    {
                        ajson.PermanentMD.Description = amarkdown.PdtText;
                        ajson.PermanentMD.ID = amarkdown.PdtDatum.ToString() + "/" + amarkdown.PdtNummer;
                        ajson.PermanentMD.EffectiveDate = amarkdown.PdtEffectiveDate.ToString();
                    }

                    ajson.PermanentMD.SubGroup = satAbteilung;
                    ajson.PermanentMD.GroupNo = satGroupNo;
                    ajson.PermanentMD.ProductGroup = satWarenger;
                    ajson.PermanentMD.Qty = qty;
                    ajson.PermanentMD.SKU = sku;
                    ajson.PermanentMD.Value = value;


                    String ajsonStr = SimpleJson.SerializeObject(ajson).ToString();
                    String md5Contents = Logging.CreateMD5(ajsonStr);
                    Logging.WriteDebug("MD5", dcSetup.Debug);
                    String storedMd5 = getMd5(ajson.PermanentMD.Date, ajson.PermanentMD.ProductGroup, ajson.PermanentMD.GroupNo,
                       ajson.PermanentMD.SKU, ajson.PermanentMD.Branch, "MARKDOWN_NZ", Logging.strToInt64Def(dcSetup.PermanentMarkdownNZUpdate, 0), ersConnection);
                    Logging.WriteDebug("Store MD5", dcSetup.Debug);
                    if (!md5Contents.Equals(storedMd5) || dcSetup.PermanentMarkdownNZUpdate.Equals(""))
                    {
                        Logging.WriteDebug("JSON " + SimpleJson.SerializeObject(ajson).ToString(), dcSetup.Debug);
                        SendNewMessageQueue(SimpleJson.SerializeObject(ajson).ToString(), dcSetup.PermanentMarkdownsQueueName);
                        updateDaasExport(ajson.PermanentMD.Date, ajson.PermanentMD.ProductGroup, ajson.PermanentMD.GroupNo,
                            ajson.PermanentMD.SKU, ajson.PermanentMD.Branch, "MARKDOWN_NZ", md5Contents, ersConnection);
                    }



                }
            }

            dcSetup.resetMarkdownsNZDateRange();


            DateTime aanow = DateTime.Now;
            String ssnow = aanow.ToString("yyyyMMddhhmmss");
            dcSetup.PermanentMarkdownNZUpdate = ssnow;


        }

        private void getMarkdowns(SqlConnection ersConnection)
        {
            Logging.WriteLog("Starting getMarkdowns");
            String lastUpdate = dcSetup.PermanentMarkdownUpdate ;
            if(lastUpdate.Length < 5)
            {
                lastUpdate = "20000101121223";


                String delSql = "delete from " + DaasExportTable + " where DAAS_SET_NAME = 'MARKDOWN' ";
                using (SqlCommand cmd = new SqlCommand(delSql, ersConnection))
                {
                    cmd.ExecuteNonQuery();
                }

            }

            int dateFrom = dcSetup.MarkdownsFromDate;
            int dateTo = dcSetup.MarkdownsToDate;

            if (dateFrom != 0 || dateTo != 0)
            {
                String deleteSql = "delete from " + DaasExportTable + " where DAAS_SET_NAME = 'MARKDOWN' and DAAS_KEY1 >= @mindate and DAAS_KEY1 <= @maxDate  ";
                using (SqlCommand cmd = new SqlCommand(deleteSql, ersConnection))
                {
                    cmd.Parameters.AddWithValue("@mindate", dateFrom.ToString());
                    cmd.Parameters.AddWithValue("@maxDate", dateTo.ToString());
                    cmd.ExecuteNonQuery();
                }

            }

            String sqlLastUpdate = Logging.FuturaDateTimeAddMins(lastUpdate, -60 * 24).Substring(0, 8);
            int mdownInitialDate = dcSetup.PermanentMarkDownsInitialDate;

            String sTop = " ";
            if (dcSetup.ResultSet > 0)
            {
                sTop = " top " + dcSetup.ResultSet.ToString();
            }

            String anSql = "  select distinct PDT_DATUM, PDT_NUMMER, PDT_WARENGR, PDT_ABTEILUNG, PDT_TYPE, PDT_GRPNUMMER, PDT_REFNUMMER, PAE_TEXT, PDT_NEUVKP_DATUM from PR_AEND " +
                " join PR_ZEIL on PAE_MANDANT = PDT_MANDANT AND PAE_DATUM = PDT_DATUM AND PAE_NUMMER = PDT_NUMMER " +
                " join PR_LIN on PLN_MANDANT = PAE_MANDANT  AND PAE_DATUM = PLN_DATUM AND PLN_NUMMER = PDT_NUMMER and PLN_PREISLINIE IN (-2, 11)  and PLN_NEUVKPREIS> 0.001 " +
    //            " where PAE_GEDRUCKT = 1 and PAE_MANDANT = 1 and PAE_DATUM >= " + sqlLastUpdate + " and PAE_DATUM >= " + mdownInitialDate + "    order by 1, 2 ";
                " where PAE_GEDRUCKT = 1 and PAE_MANDANT = 1 order by 1, 2 ";

            Dictionary<string, Markdown> prAends = new Dictionary<string, Markdown>();
            Dictionary<string, Markdown> prAendsEff = new Dictionary<string, Markdown>();

            Logging.WriteDebug(anSql, dcSetup.Debug);

            using (SqlCommand cmd = new SqlCommand(anSql, ersConnection) ) 
            {
                cmd.CommandTimeout = 1200;
                using (SqlDataReader areader = cmd.ExecuteReader())
                {
                    while(areader.Read())
                    {
                        Markdown markdown = new Markdown();

                        int pdtDatum = areader.GetInt32(0);
                        int pdtNummer = Logging.strToIntDef( areader["PDT_NUMMER"].ToString(), 0) ;
                        int pdtWareng = Logging.strToIntDef(areader["PDT_WARENGR"].ToString(), 0);
                        int pdtAbte = Logging.strToIntDef(areader["PDT_ABTEILUNG"].ToString(), 0);
                        int pdtType = Logging.strToIntDef(areader["PDT_TYPE"].ToString(), 0);
                        int pdtGroupNo = Logging.strToIntDef(areader["PDT_GRPNUMMER"].ToString(), 0);
                        int pdtRefNo = Logging.strToIntDef(areader["PDT_REFNUMMER"].ToString(), 0);
                        int pdtEffectiveDate = Logging.strToIntDef(areader["PDT_NEUVKP_DATUM"].ToString(), 0);
                        String paeText = areader.GetString(7);

                        String aKey = pdtDatum.ToString() + "~" + pdtRefNo;
                        String aKeyEff = pdtEffectiveDate.ToString() + "~" + pdtRefNo;
                        if (pdtRefNo == 0)
                        {
                            aKey = pdtDatum.ToString() + "~" + pdtWareng + "~" + pdtAbte + "~" + pdtType + "~" + pdtGroupNo;
                            aKeyEff = pdtEffectiveDate.ToString() + "~" + pdtWareng + "~" + pdtAbte + "~" + pdtType + "~" + pdtGroupNo;
                        }

                        markdown.PdtDatum = pdtDatum;
                        markdown.PdtNummer = pdtNummer;
                        markdown.PdtText = paeText;
                        markdown.PdtEffectiveDate = pdtEffectiveDate;
                        if (!prAends.ContainsKey(aKey)) 
                        {
                            prAends.Add(aKey, markdown);
                        }

                        if(!prAendsEff.ContainsKey(aKeyEff))
                        {
                            prAendsEff.Add(aKeyEff, markdown);
                        }


                    }
                   
                }
            }

            DateTime anow = DateTime.Now;
            DateTime tnow = anow.AddDays(1); // added one day ...

           String snow = tnow.ToString("yyyyMMdd");

            String lastUpdateShort = lastUpdate.Substring(0, 8);

            String mainSql = "select SAD_DATUM,SAI_WARENGR, SAI_ABTEILUNG, SAI_TYPE, SAI_GRPNUMMER, SAI_REFNUMMER, SAD_FILIALE,SAD_ZAHL,SAD_WERT, SAD_ID from STATADTA " +
                " JOIN STATAIDX on SAD_ID = SAI_ID  and SAI_MANDANT = SAD_MANDANT " +
                " JOIN FILIALEN on SAD_MANDANT = FIL_MANDANT and SAD_FILIALE = FIL_NUMMER " +
                " where SAD_MANDANT = 1 and SAD_KENNUNG = 1 and ((SAD_DATUM >= " + mdownInitialDate + " and SAD_DATUM >= " +
                Logging.FuturaDateTimeAddMins(lastUpdateShort + "060606", -48 * 60).Substring(0, 8) +
                " and SAD_DATUM <= " + snow + ") OR ( SAD_DATUM >= " + dateFrom + " and SAD_DATUM <= " + dateTo + " ) ) " +
                " and FIL_ARTTRANS_VK in (0, -2) " +
                " order by SAD_DATUM, SAI_REFNUMMER, SAD_FILIALE";

            Logging.WriteDebug(mainSql, dcSetup.Debug);

            using (SqlCommand cmd = new SqlCommand(mainSql, ersConnection))
            {
                cmd.CommandTimeout = 1200;
                SqlDataReader areader = cmd.ExecuteReader();
                while(areader.Read())
                {
                    String sadDatum = areader["SAD_DATUM"].ToString();
                    String satWarenger = areader["SAI_WARENGR"].ToString();
                    String satAbteilung = areader["SAI_ABTEILUNG"].ToString();
                    String satGroupNo = areader["SAI_GRPNUMMER"].ToString();
                    String satType = areader["SAI_TYPE"].ToString();
                    String sku = areader["SAI_REFNUMMER"].ToString();
                    String branch = areader["SAD_FILIALE"].ToString();
                    double qty = Logging.strToDoubleDef(areader["SAD_ZAHL"].ToString(), 0);
                    double value = Math.Truncate((Logging.strToDoubleDef(areader["SAD_WERT"].ToString(), 0) + 0.005) * 100 ) / 100.00;
                    if( value  < 0 )
                    {
                        value = Math.Truncate((Logging.strToDoubleDef(areader["SAD_WERT"].ToString(), 0) - 0.005) * 100) / 100.00;
                    }


                    String keyDatum = Logging.FuturaDateTimeAddMins(sadDatum + "060606", -24 * 60).Substring(0, 8);

                    String akey = keyDatum + "~" + sku;
                    String akeyEff = sadDatum + "~" + sku;
                    if (sku.Equals("0"))
                    {
                        akey = keyDatum + "~" + satWarenger + "~" + satAbteilung + "~" + satType + "~" + satGroupNo;
                        akeyEff = sadDatum + "~" + satWarenger + "~" + satAbteilung + "~" + satType + "~" + satGroupNo;
                    }

                    
                    

                    MarkdownJson ajson = new MarkdownJson();
                    ajson.PermanentMD  = new PermanentMarkdown();
                    ajson.PermanentMD.Branch = branch;
                    ajson.PermanentMD.Date = sadDatum;
                    ajson.PermanentMD.ID = sadDatum + "/0";
                    ajson.PermanentMD.Description = "manual markdown";
                    ajson.PermanentMD.SadId = areader["SAD_ID"].ToString();
                    ajson.PermanentMD.EffectiveDate = sadDatum;
                    Markdown amarkdown = null;
                   // if (prAends.ContainsKey(akey))
                   // {
                   //     amarkdown = prAends[akey];
                   // }
                   // if(amarkdown != null)
                   // {
                   //     ajson.PermanentMD.Description = amarkdown.PdtText;
                   //     ajson.PermanentMD.ID = amarkdown.PdtDatum.ToString() + "/" + amarkdown.PdtNummer;
                   //     ajson.PermanentMD.EffectiveDate = amarkdown.PdtEffectiveDate.ToString();
                   // }

                    if(prAendsEff.ContainsKey(akeyEff))
                    {
                        amarkdown = prAendsEff[akeyEff];
                    }
                    if (amarkdown != null)
                    {
                        ajson.PermanentMD.Description = amarkdown.PdtText;
                        ajson.PermanentMD.ID = amarkdown.PdtDatum.ToString() + "/" + amarkdown.PdtNummer;
                        ajson.PermanentMD.EffectiveDate = amarkdown.PdtEffectiveDate.ToString();
                    }

                    ajson.PermanentMD.SubGroup = satAbteilung;
                    ajson.PermanentMD.GroupNo = satGroupNo;
                    ajson.PermanentMD.ProductGroup = satWarenger;
                    ajson.PermanentMD.Qty = qty;
                    ajson.PermanentMD.SKU = sku;
                    ajson.PermanentMD.Value = value;


                    String ajsonStr = SimpleJson.SerializeObject(ajson).ToString();
                    String md5Contents = Logging.CreateMD5(ajsonStr);
                    Logging.WriteDebug("MD5", dcSetup.Debug);
                    String storedMd5 = getMd5(ajson.PermanentMD.Date, ajson.PermanentMD.ProductGroup, ajson.PermanentMD.GroupNo,
                       ajson.PermanentMD.SKU, ajson.PermanentMD.Branch, "MARKDOWN", Logging.strToInt64Def(dcSetup.PermanentMarkdownUpdate, 0), ersConnection);
                    Logging.WriteDebug("Store MD5", dcSetup.Debug);
                    if (!md5Contents.Equals(storedMd5) || dcSetup.PermanentMarkdownUpdate.Equals(""))
                    {
                        Logging.WriteDebug("JSON " + SimpleJson.SerializeObject(ajson).ToString(), dcSetup.Debug);
                        SendNewMessageQueue(SimpleJson.SerializeObject(ajson).ToString(), dcSetup.PermanentMarkdownsQueueName);
                        updateDaasExport(ajson.PermanentMD.Date, ajson.PermanentMD.ProductGroup, ajson.PermanentMD.GroupNo,
                            ajson.PermanentMD.SKU, ajson.PermanentMD.Branch, "MARKDOWN", md5Contents, ersConnection);
                    }



                }
            }

            dcSetup.resetMarkdownsDateRange();


            DateTime aanow = DateTime.Now;
            String ssnow = aanow.ToString("yyyyMMddhhmmss");
            dcSetup.PermanentMarkdownUpdate = ssnow;



        }

        private void getPrices_1(SqlConnection ersConnection)
        {
            Logging.WriteLog("Starting getPrices");
            String lastUpdate = dcSetup.PriceUpdate;
            String sqlLastUpdate = Logging.FuturaDateTimeAddMins(lastUpdate, -30);

            String sTop = " ";
            if (dcSetup.ResultSet > 0)
            {
                sTop = " top " + dcSetup.ResultSet.ToString();
            }

            String anSql = "select " + sTop + " AGR_WARENGR [ProductGroup], AGR_ABTEILUNG[Subgroup], AGR_TYPE[Type],AGR_GRPNUMMER [GroupNumber], " +
                " WAR_TEXT[ProductGroupDescription], ABT_TEXT[SubgroupDescription], WAT_TEXT[TypeDescription], AGR_TEXT [GroupNumberDescription] " + 
                " from V_ART_KOPF " +
                " LEFT JOIN WARENGRP ON WAR_MANDANT = 1 AND WAR_NUMMER = AGR_WARENGR " +
                " LEFT JOIN  ABTEIL ON ABT_MANDANT = 1 AND ABT_NUMMER = AGR_ABTEILUNG " +
                " LEFT JOIN WARENTYP ON WAT_MANDANT = 1 AND WAT_NUMMER = AGR_TYPE " +

                " where AGR_MANDANT = 1 AND  cast(AGR_ULOG_DATE as varchar) + right('000000' + cast(AGR_ULOG_TIME AS VARCHAR), 6) >= '" + sqlLastUpdate + "'";

            Logging.WriteDebug(anSql, dcSetup.Debug);
            using (SqlCommand cmdMain = new SqlCommand(anSql, ersConnection))
            {
                using (SqlDataReader mainReader = cmdMain.ExecuteReader())
                {
                    while(mainReader.Read())
                    {
                        Logging.WriteDebug("Product", dcSetup.Debug);
                        PriceRoots aroot = new PriceRoots();
                        aroot.ProductPrice = new ProductPrices();
                        aroot.ProductPrice.Item = new PricesItem();

                        Logging.WriteDebug("Groupy", dcSetup.Debug);

                        aroot.ProductPrice.Item.GroupNumber = Convert.ToInt32(mainReader["GroupNumber"]);
                        aroot.ProductPrice.Item.ProductGroup = Convert.ToInt32(mainReader["ProductGroup"]);
                        aroot.ProductPrice.Item.Subgroup = Convert.ToInt32(mainReader["Subgroup"]);
                        aroot.ProductPrice.Item.Type = Convert.ToInt32(mainReader["Type"]);
                        aroot.ProductPrice.Item.Skus = new List<PricesSku>();
                      //  aroot.ProductPrice.Item.GroupNumberDescription = mainReader["GroupNumberDescription"].ToString();
                      //  aroot.ProductPrice.Item.ProductGroupDescription = mainReader["ProductGroupDescription"].ToString();
                      //  aroot.ProductPrice.Item.TypeDescription = mainReader["TypeDescription"].ToString();
                      //  aroot.ProductPrice.Item.SubgroupDescription = mainReader["SubgroupDescription"].ToString();

                        String skuSql = "select ART_REFNUMMER[SkuId], ART_MAXRABATT[MaximumDiscount], ART_KEIN_RABATT[FixedPrice], ART_EK_DM [PurchasePrice], " +
                            " ART_EKWAEHRUNG[Currency], ART_VKPREIS[RT_Price], ART_GHPREIS [WS_Price], " +
                            " case " +
                            "   when ART_SET_EKGEW_MODE<> 0 then ART_EK_GEWICHTET " +
                            "   else ART_EK_DM " +
                            " end[WeightedAverageCost] " +
                            " from V_ARTIKEL " +
                            " where ART_MANDANT = 1 AND ART_WARENGR = @ART_WARENGR AND ART_ABTEILUNG = @ART_ABTEILUNG AND ART_TYPE = @ART_TYPE AND ART_GRPNUMMER = @ART_GRPNUMMER";

                        using (SqlCommand cmd = new SqlCommand(skuSql, ersConnection))
                        {
                            cmd.Parameters.AddWithValue("@ART_WARENGR", aroot.ProductPrice.Item.ProductGroup);
                            cmd.Parameters.AddWithValue("@ART_ABTEILUNG", aroot.ProductPrice.Item.Subgroup);
                            cmd.Parameters.AddWithValue("@ART_TYPE", aroot.ProductPrice.Item.Type);
                            cmd.Parameters.AddWithValue("@ART_GRPNUMMER", aroot.ProductPrice.Item.GroupNumber);
                            using (SqlDataReader areader = cmd.ExecuteReader())
                            {
                                while (areader.Read())
                                {
                                    Logging.WriteDebug("sku", dcSetup.Debug);
                                    PricesSku anSku = new PricesSku();
                                    aroot.ProductPrice.Item.Skus.Add(anSku);
                                    anSku.SkuId = Logging.strToIntDef(areader["SkuId"].ToString(), 0);
                                    Logging.WriteLog("sku " + anSku.SkuId);
                                    anSku.RT_Price = Logging.strToDoubleDef(areader["RT_Price"].ToString(), 0);
                                    anSku.WS_Price = Logging.strToDoubleDef(areader["WS_Price"].ToString(), 0);
                                    anSku.Currency = areader["Currency"].ToString();
                                    anSku.PP_Price = Logging.strToDoubleDef(areader["PurchasePrice"].ToString(), 0);
                                    anSku.WeightedAverageCost = Logging.strToDoubleDef(areader["WeightedAverageCost"].ToString(), 0);

                                    String priceSql = "select   APR_PREISLINIE, APR_VKPREIS, iSnULL(AAP_DATUM, APR_VKP_DATUM) [APR_VKP_DATUM] from V_ART_PRGR " + 
                                        " LEFT JOIN V_ART_PHST ON AAP_MANDANT = 1 AND AAP_REFNUMMER = APR_REFNUMMER AND AAP_PREISLINIE = APR_PREISLINIE AND APR_VKPREIS = AAP_ALTPREIS " +
                                        " WHERE APR_MANDANT = 1 AND APR_REFNUMMER = @APR_REFNUMMERY ";
                                    anSku.Prices = new List<PricePerCode>();
                                    using (SqlCommand pricesCmd = new SqlCommand(priceSql, ersConnection))
                                    {
                                        Logging.WriteDebug("Price1", dcSetup.Debug);
                                        pricesCmd.Parameters.AddWithValue("@APR_REFNUMMERY", anSku.SkuId);
                                        using (SqlDataReader priceReader = pricesCmd.ExecuteReader())
                                        {
                                            while (priceReader.Read())
                                            {
                                                PricePerCode aprice = new PricePerCode();
                                                anSku.Prices.Add(aprice);
                                                aprice.PriceCode = priceReader.GetInt16(0);
                                                aprice.Price = Logging.strToDoubleDef(priceReader[1].ToString(), 0);
                                                aprice.Date = priceReader.GetInt32(2);
                                            }
                                        }
                                    }

                                    String price9Sql = "SELECT  KRF_FILIALE, KRF_VKPREIS, KRF_FILIAL_PREIS, KRF_MAXRABATT FROM V_KASSREF WHERE KRF_MANDANT = 1 AND KRF_REFNUMMER = @KRF_REFNUMMER ";
                                    anSku.PricesPerBranch = new List<PricePerBranch>();
                                    using (SqlCommand pricesCmd = new SqlCommand(price9Sql, ersConnection))
                                    {
                                        pricesCmd.Parameters.AddWithValue("@KRF_REFNUMMER", anSku.SkuId);
                                        using (SqlDataReader priceReader = pricesCmd.ExecuteReader())
                                        {
                                            while (priceReader.Read())
                                            {
                                                Logging.WriteDebug("Price2", dcSetup.Debug);
                                                PricePerBranch aprice = new PricePerBranch();
                                                anSku.PricesPerBranch.Add(aprice);
                                                aprice.BranchNo = priceReader.GetInt32(0);
                                                aprice.Price = Logging.strToDoubleDef(priceReader[1].ToString(), 0);
                                                aprice.BranchPrice = Logging.strToDoubleDef(priceReader[2].ToString(), 0);
                                                //aprice.MaxDiscount = Logging.strToDoubleDef(priceReader[3].ToString(), 0);
                                            }
                                        }
                                    }

                                }

                            }


                        }

                        if(aroot.ProductPrice.Item.Skus.Count == 0)
                        {
                            continue;
                        }

                        Logging.WriteDebug("Finishing price", dcSetup.Debug);
                        String ajsonStr = SimpleJson.SerializeObject(aroot).ToString();
                        //  String md5Contents = Logging.CreateMD5(ajsonStr);
                        String md5Contents = Logging.CreateMD5(ajsonStr);
                        Logging.WriteDebug("MD5", dcSetup.Debug);
                        String storedMd5 = getMd5(aroot.ProductPrice.Item.ProductGroup.ToString(), aroot.ProductPrice.Item.Subgroup.ToString(), aroot.ProductPrice.Item.Type.ToString(),
                           aroot.ProductPrice.Item.GroupNumber.ToString(), "1", "PRICE", Logging.strToInt64Def(dcSetup.PriceUpdate, 0), ersConnection);
                        Logging.WriteDebug("Store MD5", dcSetup.Debug);
                        if (!md5Contents.Equals(storedMd5) || dcSetup.PriceUpdate.Equals(""))
                        {
                            Logging.WriteDebug("JSON " + SimpleJson.SerializeObject(aroot).ToString(), dcSetup.Debug);
                            SendNewMessageQueue(SimpleJson.SerializeObject(aroot).ToString(), dcSetup.PricesQueueName);
                            updateDaasExport(aroot.ProductPrice.Item.ProductGroup.ToString(), aroot.ProductPrice.Item.Subgroup.ToString(), aroot.ProductPrice.Item.Type.ToString(),
                                aroot.ProductPrice.Item.GroupNumber.ToString(), "1", "PRICE", md5Contents, ersConnection);
                        }




                    }
                }
            }
            DateTime anow = DateTime.Now;
            String snow = anow.ToString("yyyyMMddhhmmss");
            dcSetup.PriceUpdate = snow;

        }

        private void getPrices_1NZ(SqlConnection ersConnection)
        {
            Logging.WriteLog("Starting getPricesNZ");
            String lastUpdate = dcSetup.PriceNZUpdate;
            String sqlLastUpdate = Logging.FuturaDateTimeAddMins(lastUpdate, -30);

            String sTop = " ";
            if (dcSetup.ResultSet > 0)
            {
                sTop = " top " + dcSetup.ResultSet.ToString();
            }

            String anSql = "select " + sTop + " AGR_WARENGR [ProductGroup], AGR_ABTEILUNG[Subgroup], AGR_TYPE[Type],AGR_GRPNUMMER [GroupNumber], " +
                " WAR_TEXT[ProductGroupDescription], ABT_TEXT[SubgroupDescription], WAT_TEXT[TypeDescription], AGR_TEXT [GroupNumberDescription] " +
                " from V_ART_KOPF " +
                " LEFT JOIN WARENGRP ON WAR_NUMMER = AGR_WARENGR " +
                " LEFT JOIN  ABTEIL ON ABT_NUMMER = AGR_ABTEILUNG " +
                " LEFT JOIN WARENTYP ON WAT_NUMMER = AGR_TYPE " +

                " where cast(AGR_ULOG_DATE as varchar) + right('000000' + cast(AGR_ULOG_TIME AS VARCHAR), 6) >= '" + sqlLastUpdate + "'";

            Logging.WriteDebug(anSql, dcSetup.Debug);
            using (SqlCommand cmdMain = new SqlCommand(anSql, ersConnection))
            {
                using (SqlDataReader mainReader = cmdMain.ExecuteReader())
                {
                    while (mainReader.Read())
                    {
                        Logging.WriteDebug("Product", dcSetup.Debug);
                        PriceRoots aroot = new PriceRoots();
                        aroot.ProductPrice = new ProductPrices();
                        aroot.ProductPrice.Item = new PricesItem();

                        Logging.WriteDebug("Groupy", dcSetup.Debug);

                        aroot.ProductPrice.Item.GroupNumber = Convert.ToInt32(mainReader["GroupNumber"]);
                        aroot.ProductPrice.Item.ProductGroup = Convert.ToInt32(mainReader["ProductGroup"]);
                        aroot.ProductPrice.Item.Subgroup = Convert.ToInt32(mainReader["Subgroup"]);
                        aroot.ProductPrice.Item.Type = Convert.ToInt32(mainReader["Type"]);
                        aroot.ProductPrice.Item.Skus = new List<PricesSku>();
                        //  aroot.ProductPrice.Item.GroupNumberDescription = mainReader["GroupNumberDescription"].ToString();
                        //  aroot.ProductPrice.Item.ProductGroupDescription = mainReader["ProductGroupDescription"].ToString();
                        //  aroot.ProductPrice.Item.TypeDescription = mainReader["TypeDescription"].ToString();
                        //  aroot.ProductPrice.Item.SubgroupDescription = mainReader["SubgroupDescription"].ToString();

                        String skuSql = "select ART_REFNUMMER[SkuId], ART_MAXRABATT[MaximumDiscount], ART_KEIN_RABATT[FixedPrice], ART_EK_DM [PurchasePrice], " +
                            " ART_EKWAEHRUNG[Currency], ART_VKPREIS[RT_Price], ART_GHPREIS [WS_Price], " +
                            " case " +
                            "   when ART_SET_EKGEW_MODE<> 0 then ART_EK_GEWICHTET " +
                            "   else ART_EK_DM " +
                            " end[WeightedAverageCost] " +
                            " from V_ARTIKEL " +
                            " where ART_WARENGR = @ART_WARENGR AND ART_ABTEILUNG = @ART_ABTEILUNG AND ART_TYPE = @ART_TYPE AND ART_GRPNUMMER = @ART_GRPNUMMER";

                        using (SqlCommand cmd = new SqlCommand(skuSql, ersConnection))
                        {
                            cmd.Parameters.AddWithValue("@ART_WARENGR", aroot.ProductPrice.Item.ProductGroup);
                            cmd.Parameters.AddWithValue("@ART_ABTEILUNG", aroot.ProductPrice.Item.Subgroup);
                            cmd.Parameters.AddWithValue("@ART_TYPE", aroot.ProductPrice.Item.Type);
                            cmd.Parameters.AddWithValue("@ART_GRPNUMMER", aroot.ProductPrice.Item.GroupNumber);
                            using (SqlDataReader areader = cmd.ExecuteReader())
                            {
                                while (areader.Read())
                                {
                                    Logging.WriteDebug("sku", dcSetup.Debug);
                                    PricesSku anSku = new PricesSku();
                                    aroot.ProductPrice.Item.Skus.Add(anSku);
                                    anSku.SkuId = Logging.strToIntDef(areader["SkuId"].ToString(), 0);
                                    Logging.WriteLog("sku " + anSku.SkuId);
                                    anSku.RT_Price = Logging.strToDoubleDef(areader["RT_Price"].ToString(), 0);
                                    anSku.WS_Price = Logging.strToDoubleDef(areader["WS_Price"].ToString(), 0);
                                    anSku.Currency = areader["Currency"].ToString();
                                    anSku.PP_Price = Logging.strToDoubleDef(areader["PurchasePrice"].ToString(), 0);
                                    anSku.WeightedAverageCost = Logging.strToDoubleDef(areader["WeightedAverageCost"].ToString(), 0);

                                    String priceSql = "select   APR_PREISLINIE, APR_VKPREIS, iSnULL(AAP_DATUM, APR_VKP_DATUM) [APR_VKP_DATUM] from V_ART_PRGR " +
                                        " LEFT JOIN V_ART_PHST ON AAP_REFNUMMER = APR_REFNUMMER AND AAP_PREISLINIE = APR_PREISLINIE AND APR_VKPREIS = AAP_ALTPREIS " +
                                        " WHERE APR_REFNUMMER = @APR_REFNUMMERY ";
                                    anSku.Prices = new List<PricePerCode>();
                                    using (SqlCommand pricesCmd = new SqlCommand(priceSql, ersConnection))
                                    {
                                        Logging.WriteDebug("Price1", dcSetup.Debug);
                                        pricesCmd.Parameters.AddWithValue("@APR_REFNUMMERY", anSku.SkuId);
                                        using (SqlDataReader priceReader = pricesCmd.ExecuteReader())
                                        {
                                            while (priceReader.Read())
                                            {
                                                PricePerCode aprice = new PricePerCode();
                                                anSku.Prices.Add(aprice);
                                                aprice.PriceCode = priceReader.GetInt16(0);
                                                aprice.Price = Logging.strToDoubleDef(priceReader[1].ToString(), 0);
                                                aprice.Date = priceReader.GetInt32(2);
                                            }
                                        }
                                    }

                                    String price9Sql = "SELECT  KRF_FILIALE, KRF_VKPREIS, KRF_FILIAL_PREIS, KRF_MAXRABATT FROM V_KASSREF WHERE KRF_REFNUMMER = @KRF_REFNUMMER ";
                                    anSku.PricesPerBranch = new List<PricePerBranch>();
                                    using (SqlCommand pricesCmd = new SqlCommand(price9Sql, ersConnection))
                                    {
                                        pricesCmd.Parameters.AddWithValue("@KRF_REFNUMMER", anSku.SkuId);
                                        using (SqlDataReader priceReader = pricesCmd.ExecuteReader())
                                        {
                                            while (priceReader.Read())
                                            {
                                                Logging.WriteDebug("Price2", dcSetup.Debug);
                                                PricePerBranch aprice = new PricePerBranch();
                                                anSku.PricesPerBranch.Add(aprice);
                                                aprice.BranchNo = priceReader.GetInt32(0);
                                                aprice.Price = Logging.strToDoubleDef(priceReader[1].ToString(), 0);
                                                aprice.BranchPrice = Logging.strToDoubleDef(priceReader[2].ToString(), 0);
                                                //aprice.MaxDiscount = Logging.strToDoubleDef(priceReader[3].ToString(), 0);
                                            }
                                        }
                                    }

                                }

                            }


                        }

                        if (aroot.ProductPrice.Item.Skus.Count == 0)
                        {
                            continue;
                        }

                        Logging.WriteDebug("Finishing price", dcSetup.Debug);
                        String ajsonStr = SimpleJson.SerializeObject(aroot).ToString();
                        //  String md5Contents = Logging.CreateMD5(ajsonStr);
                        String md5Contents = Logging.CreateMD5(ajsonStr);
                        Logging.WriteDebug("MD5", dcSetup.Debug);
                        String storedMd5 = getMd5(aroot.ProductPrice.Item.ProductGroup.ToString(), aroot.ProductPrice.Item.Subgroup.ToString(), aroot.ProductPrice.Item.Type.ToString(),
                           aroot.ProductPrice.Item.GroupNumber.ToString(), "1", "PRICENZ", Logging.strToInt64Def(dcSetup.PriceUpdate, 0), ersConnection);
                        Logging.WriteDebug("Store MD5", dcSetup.Debug);
                        if (!md5Contents.Equals(storedMd5) || dcSetup.PriceUpdate.Equals(""))
                        {
                            SendNewMessageQueue(SimpleJson.SerializeObject(aroot).ToString(), dcSetup.PricesQueueName);
                            Logging.WriteDebug("JSON " + SimpleJson.SerializeObject(aroot).ToString(), dcSetup.Debug);
                            updateDaasExport(aroot.ProductPrice.Item.ProductGroup.ToString(), aroot.ProductPrice.Item.Subgroup.ToString(), aroot.ProductPrice.Item.Type.ToString(),
                                aroot.ProductPrice.Item.GroupNumber.ToString(), "1", "PRICENZ", md5Contents, ersConnection);
                        }




                    }
                }
            }
            DateTime anow = DateTime.Now;
            String snow = anow.ToString("yyyyMMddhhmmss");
            dcSetup.PriceNZUpdate = snow;

        }


        private void getPrices(SqlConnection ersConnection)
        {
            Logging.WriteLog("Starting getPrices");
            String lastUpdate = dcSetup.PriceUpdate;
            String sqlLastUpdate = Logging.FuturaDateTimeAddMins(lastUpdate, -30);

            String sTop = " ";
            if (dcSetup.ResultSet > 0)
            {
                sTop = " top " + dcSetup.ResultSet.ToString();
            }

            String anSql = "select " + sTop + " AGR_WARENGR [ProductGroup], AGR_ABTEILUNG[Subgroup], AGR_TYPE[Type],AGR_GRPNUMMER [GroupNumber] " +
                " from V_ART_KOPF " +
                " where AGR_MANDANT = 1 AND  cast(AGR_ULOG_DATE as varchar) + right('000000' + cast(AGR_ULOG_TIME AS VARCHAR), 6) >= '" + sqlLastUpdate + "'";

            DataTable dsItems = null;
            using (SqlDataAdapter daTrans = new SqlDataAdapter(anSql, ersConnection))
            {
                dsItems = new DataTable();
                daTrans.Fill(dsItems);
            }
            if (dsItems == null)
                return;

            for (int k = 0; k < dsItems.Rows.Count; k++)
            {
                Logging.WriteDebug("Product", dcSetup.Debug);
                PriceRoots aroot = new PriceRoots();
                aroot.ProductPrice = new ProductPrices();
                aroot.ProductPrice.Item = new PricesItem();

                DataRow arow = dsItems.Rows[k];
                Logging.WriteDebug("Groupy", dcSetup.Debug);

                aroot.ProductPrice.Item.GroupNumber = Convert.ToInt32(arow["GroupNumber"]);
                aroot.ProductPrice.Item.ProductGroup = Convert.ToInt32(arow["ProductGroup"]);
                aroot.ProductPrice.Item.Subgroup = Convert.ToInt32(arow["Subgroup"]);
                aroot.ProductPrice.Item.Type = Convert.ToInt32(arow["Type"]);
                aroot.ProductPrice.Item.Skus = new List<PricesSku>();

                String skuSql = "select ART_REFNUMMER[SkuId], ART_MAXRABATT[MaximumDiscount], ART_KEIN_RABATT[FixedPrice], " +
                    " ART_EKWAEHRUNG[Currency], ART_VKPREIS[RT_Price] " +
                    " from V_ARTIKEL " +
                    " where ART_MANDANT = 1 AND ART_WARENGR = @ART_WARENGR AND ART_ABTEILUNG = @ART_ABTEILUNG AND ART_TYPE = @ART_TYPE AND ART_GRPNUMMER = @ART_GRPNUMMER";

                using(SqlCommand cmd = new SqlCommand(skuSql, ersConnection))
                {
                    cmd.Parameters.AddWithValue("@ART_WARENGR", aroot.ProductPrice.Item.ProductGroup);
                    cmd.Parameters.AddWithValue("@ART_ABTEILUNG", aroot.ProductPrice.Item.Subgroup);
                    cmd.Parameters.AddWithValue("@ART_TYPE", aroot.ProductPrice.Item.Type);
                    cmd.Parameters.AddWithValue("@ART_GRPNUMMER", aroot.ProductPrice.Item.GroupNumber);
                    using(SqlDataReader areader = cmd.ExecuteReader())
                    {
                        while (areader.Read())
                        {
                            Logging.WriteDebug("sku", dcSetup.Debug);
                            PricesSku anSku = new PricesSku();
                            aroot.ProductPrice.Item.Skus.Add(anSku);
                            anSku.SkuId = Logging.strToIntDef(areader["SkuId"].ToString(), 0);
                            anSku.RT_Price = Logging.strToDoubleDef(areader["RT_Price"].ToString(), 0);
                            anSku.Currency = areader["Currency"].ToString();

                            String priceSql = "select APR_PREISLINIE, APR_VKPREIS, APR_VKP_DATUM from V_ART_PRGR WHERE APR_MANDANT = 1 AND APR_REFNUMMER = @APR_REFNUMMERY ";
                            anSku.Prices = new List<PricePerCode>();
                            using (SqlCommand pricesCmd = new SqlCommand(priceSql, ersConnection))
                            {
                                Logging.WriteDebug("Price1", dcSetup.Debug);
                                pricesCmd.Parameters.AddWithValue("@APR_REFNUMMERY", anSku.SkuId);
                                using (SqlDataReader priceReader = pricesCmd.ExecuteReader())
                                {
                                    while (priceReader.Read())
                                    {
                                        PricePerCode aprice = new PricePerCode();
                                        anSku.Prices.Add(aprice);
                                        aprice.PriceCode = priceReader.GetInt16(0);
                                        aprice.Price = Logging.strToDoubleDef(priceReader[1].ToString(), 0);
                                        aprice.Date = priceReader.GetInt32(2);
                                    }
                                }
                            }

                            String price9Sql = "SELECT  KRF_FILIALE, KRF_VKPREIS, KRF_FILIAL_PREIS, KRF_MAXRABATT FROM V_KASSREF WHERE KRF_MANDANT = 1 AND KRF_REFNUMMER = @KRF_REFNUMMER ";
                            anSku.PricesPerBranch = new List<PricePerBranch>();
                            using (SqlCommand pricesCmd = new SqlCommand(price9Sql, ersConnection))
                            {
                                pricesCmd.Parameters.AddWithValue("@KRF_REFNUMMER", anSku.SkuId);
                                using (SqlDataReader priceReader = pricesCmd.ExecuteReader())
                                {
                                    while (priceReader.Read())
                                    {
                                        Logging.WriteDebug("Price2", dcSetup.Debug);
                                        PricePerBranch aprice = new PricePerBranch();
                                        anSku.PricesPerBranch.Add(aprice);
                                        aprice.BranchNo = priceReader.GetInt32(0);
                                        aprice.Price = Logging.strToDoubleDef(priceReader[1].ToString(), 0);
                                        aprice.BranchPrice = Logging.strToDoubleDef(priceReader[2].ToString(), 0);
                                        //aprice.MaxDiscount = Logging.strToDoubleDef(priceReader[3].ToString(), 0);
                                    }
                                }
                            }

                        }

                    }


                }

                Logging.WriteDebug("Finishing price", dcSetup.Debug);
                String ajsonStr = SimpleJson.SerializeObject(aroot).ToString();
              //  String md5Contents = Logging.CreateMD5(ajsonStr);
                String md5Contents = Logging.CreateMD5(ajsonStr);
                Logging.WriteDebug("MD5", dcSetup.Debug);
                String storedMd5 = getMd5(aroot.ProductPrice.Item.ProductGroup.ToString(), aroot.ProductPrice.Item.Subgroup.ToString(), aroot.ProductPrice.Item.Type.ToString(),
                   aroot.ProductPrice.Item.GroupNumber.ToString(), "1", "PRICE", Logging.strToInt64Def(dcSetup.ProductUpdate, 0),  ersConnection);
                Logging.WriteDebug("Store MD5", dcSetup.Debug);
                if (!md5Contents.Equals(storedMd5) && aroot.ProductPrice.Item.Skus.Count > 0)
                {
                    Logging.WriteDebug("JSON " + SimpleJson.SerializeObject(aroot).ToString(), dcSetup.Debug);
                    SendNewMessageQueue(SimpleJson.SerializeObject(aroot).ToString(), dcSetup.PricesQueueName);
                    updateDaasExport(aroot.ProductPrice.Item.ProductGroup.ToString(), aroot.ProductPrice.Item.Subgroup.ToString(), aroot.ProductPrice.Item.Type.ToString(),
                        aroot.ProductPrice.Item.GroupNumber.ToString(), "1", "PRICE", md5Contents, ersConnection);
                }




            }
            DateTime anow = DateTime.Now;
            String snow = anow.ToString("yyyyMMddhhmmss");
            dcSetup.PriceUpdate = snow;

        }

        private void getProducts(SqlConnection ersConnection)
        {
            Logging.WriteLog("Starting getProducts");
            String lastUpdate = dcSetup.ProductUpdate ;
            String sqlLastUpdate = Logging.FuturaDateTimeAddMins(lastUpdate, -30);

            String anSql = "SELECT FORMAT(max(modify_date), 'yyyyMMddhhmmss')[MODIFY_DATE] " +
                " FROM sys.tables where name in ( 'ART_KOPF', 'ARTIKEL'   )";
            double ansModify = 0;
            using (SqlCommand cmd = new SqlCommand(anSql, ersConnection))
            {
                var aresult = cmd.ExecuteScalar();
                if (aresult != null)
                {
                    ansModify = Logging.strToDoubleDef(aresult.ToString(), 0);
                }
            }
            String sAnsModify = Logging.doubleDateToString(ansModify);


            if (sAnsModify.CompareTo(sqlLastUpdate) < 0 )
            {
                return;
            }

            String sTop = " ";
            if (dcSetup.ResultSet > 0)
            {
                sTop = " top " + dcSetup.ResultSet.ToString();
            }


            anSql = "select " + sTop + " AGR_WARENGR [ProductGroup], AGR_ABTEILUNG[Subgroup], AGR_TYPE[Type],AGR_GRPNUMMER [GroupNumber], AGR_TEXT [SupplierItemDescription], " +
                " AGR_BONTEXT [ReceiptText], ISNULL(TBL.TEXT, '') [LongDescription], AGR_LIEFERANT[DeliveryType], AGR_LFARTGRP[SupplierItemGroup], " +
                " AGR_LFARTGRP_TCOD[SupplierItemGroupIndex], ISNULL(EKB_NUMMER, 0)[ORIGEN], ISNULL(EKB_TEXT, '')[ORIGEN_TEXT], AGR_SERIENFLAG[SerialNumberEntry] , " +
                " AGR_VK_BEREICH[SalesAreaNo], ISNULL(VKB_TEXT, '')[SalesArea], AGR_ETITYP[LabelType], AGR_ETIZAHL[LabelPerPiece], " +
                " cast(AGR_ULOG_DATE as varchar) + right('000000' + cast(AGR_ULOG_TIME AS VARCHAR), 6) " +
                " from V_ART_KOPF " +
                " LEFT JOIN (SELECT ATX_WARENGR, ATX_ABTEILUNG, ATX_TYPE, ATX_GRPNUMMER, STUFF((SELECT ' ' + T2.ATX_TEXT " +
                "     FROM V_ART_TEXT T2 " +
                "      WHERE T2.ATX_MANDANT = 1 AND T2.ATX_WARENGR = T1.ATX_WARENGR AND T1.ATX_ABTEILUNG = T2.ATX_ABTEILUNG AND T1.ATX_TYPE = T2.ATX_TYPE AND T1.ATX_GRPNUMMER = T2.ATX_GRPNUMMER " +
                "       FOR XML PATH('')), 1, 1, '')[TEXT]  " +
                "       FROM V_ART_TEXT  T1 WHERE T1.ATX_MANDANT = 1 " +
                "        GROUP BY ATX_WARENGR, ATX_ABTEILUNG, ATX_TYPE, ATX_GRPNUMMER)TBL " +
                "        ON TBL.ATX_WARENGR = AGR_WARENGR AND TBL.ATX_ABTEILUNG = AGR_ABTEILUNG AND TBL.ATX_TYPE = AGR_TYPE AND TBL.ATX_GRPNUMMER = AGR_GRPNUMMER " +
                " LEFT JOIN V_EK_BER ON EKB_NUMMER = AGR_EK_BEREICH AND  EKB_MANDANT = 1 " +
                " LEFT JOIN V_VK_BER ON VKB_NUMMER = AGR_VK_BEREICH AND  VKB_MANDANT = 1 " +
                " where AGR_MANDANT = 1 AND  cast(AGR_ULOG_DATE as varchar) + right('000000' + cast(AGR_ULOG_TIME AS VARCHAR), 6) >= '" + sqlLastUpdate + "'";

            DataTable dsItems = null;
            using (SqlDataAdapter daTrans = new SqlDataAdapter(anSql, ersConnection))
            {
                dsItems = new DataTable();
                daTrans.Fill(dsItems);
            }
            if (dsItems == null)
                return;

            String attrSql = "select ARC_CODE, ARC_VALUE, ARC_TEXT from V_ART_CODE WHERE " +
                " ARC_MANDANT = 1 AND ARC_WARENGR = @ARC_WARENGR AND ARC_ABTEILUNG = @ARC_ABTEILUNG AND ARC_TYPE = @ARC_TYPE AND ARC_GRPNUMMER = @ARC_GRPNUMMER";


            for (int k = 0; k < dsItems.Rows.Count; k++)
            {
                DataRow arow = dsItems.Rows[k];
                ItemsJson itemsJson = new ItemsJson();
                StockItem anItem = new StockItem();
                itemsJson.Item = anItem;
                anItem.Skus = null;
                anItem.ProductGroup = Convert.ToInt32(arow["ProductGroup"]);
                anItem.Subgroup = Convert.ToInt32(arow["Subgroup"]);
                anItem.Type = Convert.ToInt32(arow["Type"]);
                anItem.GroupNumber = Convert.ToInt32(arow["GroupNumber"]);
                anItem.SupplierItemDescription = arow["SupplierItemDescription"].ToString();
                anItem.ReceiptText = arow["ReceiptText"].ToString();
                anItem.LongDescription = arow["LongDescription"].ToString();
                anItem.DeliveryType = Convert.ToInt32(arow["DeliveryType"]);
                anItem.SupplierItemGroup = Convert.ToInt32(arow["SupplierItemGroup"]);
                anItem.Origin = Convert.ToInt32(arow["ORIGEN"]);
                anItem.OriginText = arow["ORIGEN_TEXT"].ToString();
                anItem.SerialNumberEntry = Convert.ToInt32(arow["SerialNumberEntry"]);
                anItem.SalesAreaNo = Convert.ToInt32(arow["SalesAreaNo"]);
                anItem.SalesArea = arow["SalesArea"].ToString();

                anItem.ItemAttributes = new List<StockAttribute>();
                using (SqlCommand cmd = new SqlCommand(attrSql, ersConnection))
                {
                    cmd.Parameters.AddWithValue("@ARC_WARENGR", anItem.ProductGroup);
                    cmd.Parameters.AddWithValue("@ARC_ABTEILUNG", anItem.Subgroup);
                    cmd.Parameters.AddWithValue("@ARC_TYPE", anItem.Type);
                    cmd.Parameters.AddWithValue("@ARC_GRPNUMMER", anItem.GroupNumber);
                    using(SqlDataReader areader = cmd.ExecuteReader())
                    {
                        while(areader.Read())
                        {
                            StockAttribute anAttribute = new StockAttribute();
                            anItem.ItemAttributes.Add(anAttribute);
                            anAttribute.Code = areader.GetString(0);
                            anAttribute.Value = areader.GetInt32(1);
                            anAttribute.Text = areader.GetString(2);
                        }

                    }
                }

                String skuSql = "select ART_REFNUMMER[SkuId], ART_SORTIERUNG[Sort], ART_EINHEITTEXT[UnitText], ART_EIGENTEXT[VariantText], ART_LFID_NUMMER[RefNummer], " +
                    " ART_SAISON[StatisticalPeriodNo], SPE_TEXT[StatisticalPeriod], ART_MAXRABATT[MaximumDiscount], ART_KEIN_RABATT[FixedPrice], " +
                    " ART_CMP_TYP[QtyTypeForComparativePrice], ART_CMP_ISTMENGE[ComparativeQtyForComparativePrice], ART_CMP_REFMENGE[QtyForComparativePrice], " +
                    " ART_ZWEIT_LFID[POSupplierItemNumber], ART_VKPREIS[RT_Price], ART_EKWAEHRUNG[Currency] " +
                    " from V_ARTIKEL " +
                    " LEFT JOIN V_STATPERI ON SPE_SAISON = ART_SAISON AND SPE_MANDANT = 1" +
                    " where ART_MANDANT = 1 AND ART_WARENGR = @ART_WARENGR AND ART_ABTEILUNG = @ART_ABTEILUNG AND ART_TYPE = @ART_TYPE AND ART_GRPNUMMER = @ART_GRPNUMMER";


                anItem.Skus = new List<Sku>();

                using(SqlCommand cmd = new SqlCommand(skuSql, ersConnection))
                {
                    cmd.Parameters.AddWithValue("@ART_WARENGR", anItem.ProductGroup);
                    cmd.Parameters.AddWithValue("@ART_ABTEILUNG", anItem.Subgroup);
                    cmd.Parameters.AddWithValue("@ART_TYPE", anItem.Type);
                    cmd.Parameters.AddWithValue("@ART_GRPNUMMER", anItem.GroupNumber);
                    using(SqlDataReader areader = cmd.ExecuteReader())
                    {
                        while(areader.Read())
                        {
                            Sku sku = new Sku();
                            anItem.Skus.Add(sku);
                            sku.SkuId = Logging.strToIntDef(areader["SkuId"].ToString(), 0);
                            sku.Sort = Logging.strToIntDef(areader["Sort"].ToString(), 0);
                            sku.UnitText = areader["UnitText"].ToString();
                            sku.VariantText = areader["VariantText"].ToString();
                            sku.SpltmNo = areader["RefNummer"].ToString();
                            sku.StatisticalPeriodNo = Logging.strToIntDef(areader["StatisticalPeriodNo"].ToString(), 0);
                            sku.StatisticalPeriod = areader["StatisticalPeriod"].ToString();
                            sku.MaximumDiscount = Logging.strToDoubleDef(areader["MaximumDiscount"].ToString(), 0);
                            sku.FixedPrice = Logging.strToDoubleDef(areader["FixedPrice"].ToString(), 0); 
                            sku.POSupplierItemNumber = areader["POSupplierItemNumber"].ToString();
                            sku.QtyTypeForComparativePrice = areader["QtyTypeForComparativePrice"].ToString();
                            sku.ComparativeQtyForComparativePrice = Logging.strToDoubleDef(areader["ComparativeQtyForComparativePrice"].ToString(), 0);
                            sku.QtyForComparativePrice = Logging.strToDoubleDef(areader["QtyForComparativePrice"].ToString(), 0);
                            sku.RT_Price = Logging.strToDoubleDef(areader["RT_Price"].ToString(), 0);
                            sku.Currency = areader["Currency"].ToString();

                            sku.EanCodes = new List<EanCode>();
                            String eanSql = "SELECT AEA_EANCODE, AEA_SORTIERUNG FROM V_ART_EANS WHERE AEA_MANDANT = 1 AND AEA_REFNUMMER = @AEA_REFNUMMER ";

                            using(SqlCommand cmdEan = new SqlCommand(eanSql, ersConnection))
                            {
                                cmdEan.Parameters.AddWithValue("@AEA_REFNUMMER", sku.SkuId);
                                using(SqlDataReader eanReader = cmdEan.ExecuteReader())
                                {
                                    while (eanReader.Read())
                                    {
                                        EanCode eanCode = new EanCode();
                                        sku.EanCodes.Add(eanCode);
                                        eanCode.ECode = eanReader["AEA_EANCODE"].ToString();
                                        eanCode.Sorting = Logging.strToIntDef(eanReader["AEA_SORTIERUNG"].ToString(), 0);
                                    }
                                }
                            }

                            String attrSkuSql = "select ADC_CODE, ADC_VALUE, ADC_TEXT from V_ART_DCOD where ADC_MANDANT = 1 AND ADC_REFNUMMER = @ADC_REFNUMMERY";
                            sku.skuAttributes = new List<StockAttribute>();

                            using(SqlCommand attrCmd = new SqlCommand(attrSkuSql, ersConnection))
                            {
                                
                                try
                                {
                                    Logging.WriteDebug("Here + " + sku.SkuId, dcSetup.Debug);
                                    attrCmd.Parameters.AddWithValue("@ADC_REFNUMMERY", sku.SkuId);
                                    using (SqlDataReader attrReader = attrCmd.ExecuteReader())
                                    {
                                        while (attrReader.Read())
                                        {
                                            StockAttribute anAttribute = new StockAttribute();
                                            sku.skuAttributes.Add(anAttribute);
                                            anAttribute.Code = attrReader["ADC_CODE"].ToString();
                                            anAttribute.Text = attrReader["ADC_TEXT"].ToString();
                                            anAttribute.Value = Logging.strToIntDef(attrReader["ADC_VALUE"].ToString(), 0);
                                        }
                                    }
                                }
                                catch(Exception e)
                                {
                                    Logging.WriteErrorLog("here " + sku.SkuId + " " + e.Message);
                                }

                            }


                        }
                    }
                }

                String ajsonStr = SimpleJson.SerializeObject(itemsJson).ToString();
                String md5Contents = Logging.CreateMD5(ajsonStr);

                String storedMd5 = getMd5(anItem.ProductGroup.ToString(), anItem.Subgroup.ToString(), anItem.Type.ToString(), anItem.GroupNumber.ToString(), "1", "PRODUCT", Logging.strToInt64Def(dcSetup.ProductUpdate, 0),
                    ersConnection);

                if (!md5Contents.Equals(storedMd5))
                {
                    Logging.WriteDebug("JSON " + SimpleJson.SerializeObject(itemsJson).ToString(), dcSetup.Debug);
                    SendNewMessageQueue(SimpleJson.SerializeObject(itemsJson).ToString(), dcSetup.ProductsQueueName);
                    updateDaasExport(anItem.ProductGroup.ToString(), anItem.Subgroup.ToString(), anItem.Type.ToString(), anItem.GroupNumber.ToString(), "1", "PRODUCT", md5Contents, ersConnection);
                }



            }

            DateTime anow = DateTime.Now;
            String snow = anow.ToString("yyyyMMddhhmmss");
            dcSetup.ProductUpdate = snow;


        }

        private void getLocations(SqlConnection ersConnection)
        {
            String lastUpdate = dcSetup.LocationUpdate;
            String sqlLastUpdate = Logging.FuturaDateTimeAddMins(lastUpdate, -30);

            String anSql = "SELECT FORMAT(max(modify_date), 'yyyyMMddhhmmss')[MODIFY_DATE] " +
                " FROM sys.tables where name = 'FILIALEN'";

            String filiaModify = "";
            using(SqlCommand cmd = new SqlCommand(anSql, ersConnection))
            {
                var aresult = cmd.ExecuteScalar();
                if (aresult != null)
                {
                    filiaModify = aresult.ToString();
                }
            }

            anSql = "SELECT " +
                " case " +
                "   when max(A.DB_TIME) > max(F.DB_TIME) then max(A.DB_TIME) " +
                "   else max(F.DB_TIME) " +
                " end[DBTIME] " +
                " FROM ANSCHRIF A " +
                " join FILIALEN F on ANS_TYP = FIL_TYP AND ANS_NUMMER = FIL_NUMMER";
            double ansModify = 0;
            using (SqlCommand cmd = new SqlCommand(anSql, ersConnection))
            {
                var aresult = cmd.ExecuteScalar();
                if (aresult != null)
                {
                    ansModify = Logging.strToDoubleDef(aresult.ToString(), 0);
                }
            }
            String sAnsModify = Logging.doubleDateToString(ansModify);


            // if ( sAnsModify.CompareTo(lastUpdate) < 0 && filiaModify.CompareTo(lastUpdate) < 0)
            // {
            //     return;
            // }


            String sTop = " ";
            if (dcSetup.ResultSet > 0)
            {
                sTop = " top " + dcSetup.ResultSet.ToString();
            }



            String locSql = " select " + sTop + " FIL_TYP, FIL_NUMMER[BRANCH_NO], FIL_WAEHRUNG [CURRENCY], WAE_TEXT [CURRENCY_NAME], FIL_REGION [REGION], FIL_KASSENKTO [POS_ACCOUNT], " +
                " isnull((select max (REG_TEXT) from V_REGION where REG_NUMMER = FIL_REGION AND REG_MANDANT = 1), '') [REGION_NAME], " +
                " ISNULL(K1.KTO_TEXT, '') [POS_ACCOUNT_NAME], FIL_WARENKTO[PRODUCT_ACCOUNT],ISNULL(K2.KTO_TEXT, '')[PRODUCT_ACCOUNT_NAME], FIL_WARENKST[COST_CENTRE], ISNULL(KO.KST_TEXT, '')[COST_CENTRE_NAME], " +
                " FIL_STEUERGRUPPE[TAX_GROUP], ISNULL(SGR_TEXT, '')[TAX_GROUP_NAME], FIL_INDEX, ADD_SPRACHE[LANGUAGE], ANS_NAME1[NAME1], ANS_NAME2[NAME2], ANS_LAND[COUNTRY], LAN_TEXT[COUNTRY_NAME], " +
                " ANS_COUNTY[STATE], ANS_PLZ[POSTCODE], ANS_ORT[SUBURB], rtrim(ANS_STRASSE + ' ' + ANS_STRASSE_2)[ADDRESS], ANS_TELEFON[PHONE1], ANS_TELEFON2[PHONE2], ANS_TELEFAX[FAX], ANS_EMAIL[EMAIL], " +
                " FIL_ZEITZONE[TIMEZONE], ISNULL(ZZO_NAME_DISPLAY, '')[TIMEZONE_NAME], FIL_PRIO, FIL_EGSTEUERNR, FIL_FASTEUERNR, FIL_VERTEILUNG, FIL_KASSENKST [COST_OF_GOODS], ISNULL(KP.KST_TEXT, '')[COST_OF_GOODS_DESCRIPTION]  " +
                " from V_FILIALEN " +
                " JOIN V_ADR_DATA ON FIL_NUMMER = ADD_NUMMER AND ADD_TYP = FIL_TYP AND ADD_MANDANT = 1 " +
                " JOIN V_ANSCHRIF ON ANS_TYP = FIL_TYP AND ANS_NUMMER = FIL_NUMMER AND ANS_COUNT = 1 AND ANS_MANDANT = 1" +
                " JOIN V_LAND ON ANS_LAND = LAN_NUMMER  and LAN_MANDANT = 1 " +
                " LEFT JOIN V_WAEHRUNG ON WAE_CODE = FIL_WAEHRUNG AND WAE_MANDANT = 1 " +
                " LEFT JOIN V_KONTO K1 ON K1.KTO_NUMMER = FIL_KASSENKTO AND K1.KTO_MANDANT = 1 " +
                " LEFT JOIN V_KONTO K2 ON K2.KTO_NUMMER = FIL_WARENKTO  AND K2.KTO_MANDANT = 1 " +
                " LEFT JOIN V_KOSTENST KO ON KO.KST_NUMMER = FIL_WARENKST AND KO.KST_MANDANT = 1 " +
                " LEFT JOIN V_KOSTENST KP ON KP.KST_NUMMER = FIL_KASSENKST AND KO.KST_MANDANT = 1 " +
                " LEFT JOIN V_STEUER_G ON SGR_NUMMER = FIL_STEUERGRUPPE AND SGR_MANDANT = 1 " +
                " LEFT JOIN V_ZEITZONE ON ZZO_INDEX = FIL_ZEITZONE AND ZZO_MANDANT = 1" + 
                " where FIL_MANDANT = 1 " ;

            Logging.WriteDebug(locSql, dcSetup.Debug);

            DataTable dsLocations = null;
            using (SqlDataAdapter daTrans = new SqlDataAdapter(locSql, ersConnection))
            {
                dsLocations = new DataTable();
                daTrans.Fill(dsLocations);
            }
            if (dsLocations == null)
                return;


            String attrSql = "select AAV_CODE, AAV_UNIQUE, AAV_VALUE, AAV_TEXT, ISNULL(AAE_TEXT, '') [DESCRIPTION ]  " + 
                " from V_ADRATVAL " +
                " LEFT JOIN ADRATENU ON AAE_MANDANT = AAV_MANDANT AND AAE_CODE = AAV_CODE AND AAE_ENUM = AAV_VALUE and AAE_ETXT = AAV_TEXT " +
                " WHERE AAV_TYP = @AAV_TYP AND AAV_NUMMER = @AAV_NUMMER AND AAV_MANDANT = 1 ";


            String addrSql = "select ANS_COUNT, ANS_NAME1[NAME1], ANS_NAME2[NAME2], ANS_LAND[COUNTRY], ANS_COUNTY[STATE], ANS_PLZ[POSTCODE], ANS_ORT[SUBURB], " +
                " rtrim(ANS_STRASSE + ' ' + ANS_STRASSE_2)[ADDRESS], ANS_TELEFON[PHONE1], ANS_TELEFON2[PHONE2], ANS_TELEFAX[FAX], ANS_EMAIL[EMAIL], LAN_TEXT[COUNTRY_NAME] " +
                " from V_ANSCHRIF " +
                " JOIN V_LAND ON ANS_LAND = LAN_NUMMER  and LAN_MANDANT = 1 " +
                " where ANS_MANDANT = 1 AND ANS_TYP = @AAV_TYP AND ANS_NUMMER =  @AAV_NUMMER";

            for (int k = 0; k < dsLocations.Rows.Count; k++)
            {
                LocationsJson locationsJson = new LocationsJson();
 
                Location location = new Location();
                locationsJson.Location = location;

                DataRow arow = dsLocations.Rows[k];
                int filTyp = Convert.ToInt32(arow["FIL_TYP"]);

                location.RegionName = arow["REGION_NAME"].ToString();
                location.Region = Convert.ToInt32(arow["REGION"]);

                location.BranchNo = Convert.ToInt32(arow["BRANCH_NO"]);
                location.Currency = arow["CURRENCY"].ToString();
                location.CurrencyName = arow["CURRENCY_NAME"].ToString();
                location.Region = Convert.ToInt32(arow["REGION"]);
                location.RegionName = arow["REGION_NAME"].ToString();
                location.ProductAccount = Convert.ToInt32(arow["PRODUCT_ACCOUNT"]);
                location.ProductAccountName = arow["PRODUCT_ACCOUNT_NAME"].ToString();
                 location.TaxGroup = Convert.ToInt32(arow["TAX_GROUP"]);
                location.TaxGroupName = arow["TAX_GROUP_NAME"].ToString();
                location.Language = arow["LANGUAGE"].ToString();
                location.Timezone = Convert.ToInt32(arow["TIMEZONE"]);
                location.TimezoneName = arow["TIMEZONE_NAME"].ToString();
                location.BranchName  = arow["FIL_INDEX"].ToString();
                location.Priority = Convert.ToInt32(arow["FIL_PRIO"]);

                location.GstId = arow["FIL_EGSTEUERNR"].ToString();
                location.GstReg = arow["FIL_FASTEUERNR"].ToString();
                location.AllocationPossible = Convert.ToInt32(arow["FIL_VERTEILUNG"].ToString());
 

                location.PosAccount = Convert.ToInt32(arow["POS_ACCOUNT"]);
                location.PosAccountName = arow["POS_ACCOUNT_NAME"].ToString();


           //     location.CostOfGoods = Convert.ToInt32(arow["COST_OF_GOODS"].ToString());
           //     location.CostOfGoodsDescription = arow["COST_OF_GOODS_DESCRIPTION"].ToString();
           //     location.CostCentre = Convert.ToInt32(arow["COST_CENTRE"]);
           //     location.CostCentreName = arow["COST_CENTRE_NAME"].ToString();

                location.CostOfGoods = Convert.ToInt32(arow["COST_CENTRE"]);
                location.CostOfGoodsDescription = arow["COST_CENTRE_NAME"].ToString();
                location.CostCentre = Convert.ToInt32(arow["COST_OF_GOODS"].ToString());
                location.CostCentreName = arow["COST_OF_GOODS_DESCRIPTION"].ToString();


                location.Address = arow["ADDRESS"].ToString();
                location.Name1 = arow["NAME1"].ToString();
                location.Name2 = arow["NAME2"].ToString();
                location.Country = Convert.ToInt32(arow["COUNTRY"]);
                location.CountryName = arow["COUNTRY_NAME"].ToString();
                location.State = arow["STATE"].ToString();
                location.Postcode = arow["POSTCODE"].ToString();
                location.Suburb = arow["SUBURB"].ToString();
                location.Address = arow["ADDRESS"].ToString();
                location.Phone1 = arow["PHONE1"].ToString();
                location.Phone2 = arow["PHONE2"].ToString();
                location.Fax = arow["FAX"].ToString();
                location.Email = arow["EMAIL"].ToString();



                location.Attributes = new List<ATTRIBUTE>();

                int attrCounter = 0;
                using (SqlCommand cmd = new SqlCommand(attrSql, ersConnection))
                {
                    cmd.Parameters.AddWithValue("@AAV_TYP", filTyp);
                    cmd.Parameters.AddWithValue("@AAV_NUMMER", location.BranchNo);
                    using(SqlDataReader areader = cmd.ExecuteReader())
                    {
                        while(areader.Read())
                        {
                            attrCounter++;
                            ATTRIBUTE anAttribute = new ATTRIBUTE();
                            location.Attributes.Add(anAttribute);
                            anAttribute.Code = areader.GetString(0);
                            anAttribute.Unique = areader.GetInt16(1);
                            anAttribute.Value = areader.GetInt32(2);
                            anAttribute.Text = areader.GetString(3);
                            anAttribute.Description = areader.GetString(4);

                        }
                    }
                }

 

                String jsonContents = SimpleJson.SerializeObject(locationsJson).ToString();

                String md5Contents = Logging.CreateMD5(jsonContents);

                String storedMd5 = getMd5(location.BranchNo.ToString(), "1", "1", "1", "1", "LOCATION", Logging.strToInt64Def(dcSetup.LocationUpdate, 0),
                    ersConnection);

                if (!md5Contents.Equals(storedMd5) || lastUpdate.Equals(""))
                {

                    Logging.WriteDebug("JSON " + SimpleJson.SerializeObject(locationsJson).ToString(), dcSetup.Debug);
                    SendNewMessageQueue(SimpleJson.SerializeObject(locationsJson).ToString(), dcSetup.LocationsQueueName);
                    updateDaasExport(location.BranchNo.ToString(), "1", "1", "1", "1", "LOCATION", md5Contents, ersConnection);
                }

            }


            DateTime anow = DateTime.Now;
            String snow = anow.ToString("yyyyMMddhhmmss");
            dcSetup.LocationUpdate = snow;




        }

        public void sendEmail(String textBody)
        {
            if (textBody.Equals(""))
                return;
            String errSettings = "";
            if (dcSetup.SMTPUserName == null || dcSetup.SMTPUserName.Equals(""))
                errSettings += " SMTPUserName is not set. ";

            if (dcSetup.SMTPPassword == null || dcSetup.SMTPPassword.Equals(""))
                errSettings += " SMTPPassword is not set. ";

            if (dcSetup.EmailFrom == null || dcSetup.EmailFrom.Equals(""))
                errSettings += " EmailFrom is not set. ";

            if (dcSetup.SMTPRecipients == null || dcSetup.SMTPRecipients.Equals(""))
                errSettings += " SMTPRecipients not set. ";

            if (dcSetup.SMTPHost == null || dcSetup.SMTPHost.Equals(""))
                errSettings += "SMTPHost is not set. ";

            if (!errSettings.Equals(""))
            {
                Logging.WriteLog("Email will not be sent, SMTP settings missing in the config file. " + errSettings);
                return;
            }

            try
            {
                var credentials = new NetworkCredential(dcSetup.SMTPUserName, dcSetup.SMTPPassword);
                var mail = new MailMessage()
                {
                    From = new MailAddress(dcSetup.EmailFrom),
                    Subject = dcSetup.EmailSubject,
                    Body = textBody
                };

                mail.To.Add(dcSetup.SMTPRecipients);

                var client = new SmtpClient()
                {
                    Port = dcSetup.SMTPPort,
                    UseDefaultCredentials = true,
                    Host = dcSetup.SMTPHost,
                    EnableSsl = dcSetup.SMTPUsesTls,
                    Credentials = credentials
                };

                client.Send(mail);
            }

            catch (Exception ex)
            {
                Logging.WriteErrorLog("Could not send email. " + ex.Message);
            }

        }

        private double getWac(int refNo, int branchNo, int kasDate, bool isReturn, SqlConnection aconnection)
        {
            if(refNo == 0)
            {
                return 0;
            }
            double lager_wac = 0;
            bool lagerValid = false;

            double wac = 0;

            String lagerSql = "select LAG_EK_GEW_VALID, LAG_EK_GEWICHTET from V_LAGER WHERE LAG_MANDANT = 1 AND LAG_REFNUMMER = " + refNo + " AND  LAG_FILIALE = " + branchNo; 
            using (SqlCommand cmd = new SqlCommand(lagerSql, aconnection))
            {
                using (SqlDataReader areader = cmd.ExecuteReader())
                {
                    if (areader.Read())
                    {
                        int gemvalis = Logging.strToIntDef(areader["LAG_EK_GEW_VALID"].ToString(), 0);
                        lagerValid = gemvalis != 0;
                        if(lagerValid)
                        {
                            lager_wac = Logging.strToDoubleDef(areader["LAG_EK_GEWICHTET"].ToString(), 0);
                        }
                    }
                }
            }

            double artWac = 0;
            bool artValid = false;
            String artSql = "select  ART_EK_GEW_VALID, ART_EK_GEWICHTET from V_ARTIKEL WHERE ART_REFNUMMER = " + refNo;
            using (SqlCommand cmd = new SqlCommand(artSql, aconnection))
            {
                using (SqlDataReader areader = cmd.ExecuteReader())
                {
                    if (areader.Read())
                    {
                        int arty = Logging.strToIntDef(areader["ART_EK_GEW_VALID"].ToString(), 0);
                        artValid = arty != 0;
                        if (artValid)
                        {
                            artWac = Logging.strToDoubleDef(areader["ART_EK_GEWICHTET"].ToString(), 0);
                        }
                    }
                }
            }

            int kennumb = 6;
            if(isReturn)
            {
                kennumb = 7;
            }

            double statWac = 0;
            bool statValid = false;
            String statSql = "select SAD_DATUM, SAD_EKWERT / SAD_ZAHL [WAC],  SAD_KENNUNG from STATADTA " +
                " JOIN STATAIDX ON SAD_MANDANT = SAI_MANDANT AND SAD_ID = SAI_ID " +
                " WHERE SAD_DATUM > " + kasDate + "  AND SAD_KENNUNG = " + kennumb + " AND SAI_REFNUMMER = " + refNo + " and SAD_ZAHL > 1 AND SAD_FILIALE = " + branchNo + " AND SAD_ZAHL<> 0 " +
                " order by 1 ";

            using (SqlCommand cmd = new SqlCommand(statSql, aconnection))
            {
                using (SqlDataReader areader = cmd.ExecuteReader())
                {
                    if (areader.Read())
                    {
                        statWac = Logging.strToDoubleDef(areader["WAC"].ToString(), 0);
                        statValid = true;
                    }
                }
            }

            if(statValid)
            {
                return statWac;
            }

            if(lagerValid)
            {
                return lager_wac;
            }

            if (artValid)
            {
                return artWac;
            }
            return 0;
        }


        private double getWacNZ(int refNo, int branchNo, int kasDate, bool isReturn, SqlConnection aconnection)
        {
            if (refNo == 0)
            {
                return 0;
            }
            double lager_wac = 0;
            bool lagerValid = false;

            double wac = 0;

            String lagerSql = "select LAG_EK_GEW_VALID, LAG_EK_GEWICHTET from V_LAGER WHERE LAG_REFNUMMER = " + refNo + " AND  LAG_FILIALE = " + branchNo;
            using (SqlCommand cmd = new SqlCommand(lagerSql, aconnection))
            {
                using (SqlDataReader areader = cmd.ExecuteReader())
                {
                    if (areader.Read())
                    {
                        int gemvalis = Logging.strToIntDef(areader["LAG_EK_GEW_VALID"].ToString(), 0);
                        lagerValid = gemvalis != 0;
                        if (lagerValid)
                        {
                            lager_wac = Logging.strToDoubleDef(areader["LAG_EK_GEWICHTET"].ToString(), 0);
                        }
                    }
                }
            }

            double artWac = 0;
            bool artValid = false;
            String artSql = "select  ART_EK_GEW_VALID, ART_EK_GEWICHTET from V_ARTIKEL WHERE ART_REFNUMMER = " + refNo;
            using (SqlCommand cmd = new SqlCommand(artSql, aconnection))
            {
                using (SqlDataReader areader = cmd.ExecuteReader())
                {
                    if (areader.Read())
                    {
                        int arty = Logging.strToIntDef(areader["ART_EK_GEW_VALID"].ToString(), 0);
                        artValid = arty != 0;
                        if (artValid)
                        {
                            artWac = Logging.strToDoubleDef(areader["ART_EK_GEWICHTET"].ToString(), 0);
                        }
                    }
                }
            }

            int kennumb = 6;
            if (isReturn)
            {
                kennumb = 7;
            }

            double statWac = 0;
            bool statValid = false;
            String statSql = "select SAD_DATUM, SAD_EKWERT / SAD_ZAHL [WAC],  SAD_KENNUNG from STATADTA " +
                " JOIN STATAIDX ON SAD_ID = SAI_ID " +
                " WHERE SAD_DATUM > " + kasDate + "  AND SAD_KENNUNG = " + kennumb + " AND SAI_REFNUMMER = " + refNo + " and SAD_ZAHL > 1 AND SAD_FILIALE = " + branchNo + " AND SAD_ZAHL<> 0 " +
                " order by 1 ";

            using (SqlCommand cmd = new SqlCommand(statSql, aconnection))
            {
                using (SqlDataReader areader = cmd.ExecuteReader())
                {
                    if (areader.Read())
                    {
                        statWac = Logging.strToDoubleDef(areader["WAC"].ToString(), 0);
                        statValid = true;
                    }
                }
            }

            if (statValid)
            {
                return statWac;
            }

            if (lagerValid)
            {
                return lager_wac;
            }

            if (artValid)
            {
                return artWac;
            }
            return 0;
        }


        private void houseKeeping(String apath, String afilter)
        {
            if (!Directory.Exists(apath))
                return;

            String[] thefiles = Directory.GetFiles(apath, afilter);
            foreach (String afile in thefiles)
            {
                DateTime atime = File.GetCreationTime(afile);
                DateTime dateNow = DateTime.Now;
                DateTime datestart = dateNow.AddMonths(-3);
                if (datestart.CompareTo(atime) >= 0)
                {
                    try
                    {
                        File.Delete(afile);
                    }
                    catch { }
                }
            }

        }


    }
}
