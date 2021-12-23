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


        private int ACounter { get; set; } = 0;
        public DUtils()
        {
            dcSetup = new DCsetup();
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



        private  void SendNewMessageQueue(string text, string queueName)
        {
            Console.WriteLine($"Adding message to queue topic: {queueName}");

            string brokerUri = $"activemq:tcp://" +  dcSetup.ActiveMQUrl + "?useInactivityMonitor=false&wireFormat.maxInactivityDuration=0";  
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
            String anSql = " select DAAS_MD5 from DAAS_EXPORT where DAAS_KEY1 = @DAAS_KEY1 and DAAS_KEY1 = @DAAS_KEY1 and DAAS_KEY2 = @DAAS_KEY2 and DAAS_KEY3 = @DAAS_KEY3 " +
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
                using(SqlDataReader areader = cmd.ExecuteReader())
                {
                    found = areader.Read();
                }
            }

            DateTime anow = DateTime.Now;
            String snow = anow.ToString("yyyyMMddhhmmss");
            Int64 inow = Logging.strToInt64Def(snow, 0);


            String updateSql = "insert into DAAS_EXPORT (DAAS_KEY1, DAAS_KEY2, DAAS_KEY3, DAAS_KEY4, DAAS_KEY5, DAAS_SET_NAME, DAAS_MD5, DAAS_UPDATE_TIME) values " +
                "(@DAAS_KEY1, @DAAS_KEY2, @DAAS_KEY3, @DAAS_KEY4, @DAAS_KEY5, @DAAS_SET_NAME, @DAAS_MD5, @DAAS_UPDATE_TIME)";

            if(found)
            {
                updateSql = "update DAAS_EXPORT set DAAS_MD5 = @DAAS_MD5, DAAS_UPDATE_TIME = @DAAS_UPDATE_TIME where " +
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
            String anSql = " select DAAS_MD5 from DAAS_EXPORT  WITH(NOLOCK)  where DAAS_KEY1 = @DAAS_KEY1 and DAAS_KEY1 = @DAAS_KEY1 and DAAS_KEY2 = @DAAS_KEY2 and DAAS_KEY3 = @DAAS_KEY3 " +
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
                    if(areader.Read())
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

        public void getSales(SqlConnection ersConnection )
        {
            populateDiscount(ersConnection);
            Logging.WriteLog("Starting geSales");
            String lastUpdate = dcSetup.ProductUpdate;
            String sqlLastUpdate = Logging.FuturaDateTimeAddMins(lastUpdate, -30);
            if(sqlLastUpdate.Equals("0"))
            {
                String anSql = "delete from DAAS_EXPORT where DAAS_SET_NAME = 'SALE' ";
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

            String sqlstr = "select distinct " + top10 + " KAS_MANDANT, K.KAS_DATUM, K.KAS_FILIALE, K.KAS_KASSE, K.KAS_BONNR, KAS_BERICHT, IsNull(F.FIL_INDEX, '') [FIL_INDEX],  " +
                " IsNull(KAS_ZEIT, 0) [KAS_ZEIT], isNull(ZZO_STD_NAME, '') [ZZO_STD_NAME]  " +
                " from V_KASIDLTA K  WITH (NOLOCK) " +
                " left join V_FILIALEN F on K.KAS_FILIALE = F.FIL_NUMMER and F.FIL_MANDANT = K.KAS_MANDANT" +
                " left join V_ZEITZONE Z on FIL_ZEITZONE = Z.ZZO_INDEX and ZZO_MANDANT = K.KAS_MANDANT " +
                " where ((K.KAS_DATUM > " + anInterval + " and  K.KAS_DATUM > " + startingDate + ") or (K.KAS_DATUM >= " + dateFrom + " and K.KAS_DATUM <= " + dateTo + ") ) " +
                " and not exists(select * from DAAS_EXPORT D " +
                "   where DAAS_KEY1 = K.KAS_DATUM and K.KAS_FILIALE = DAAS_KEY2 and K.KAS_MANDANT = DAAS_KEY3 " +
                "   and K.KAS_KASSE = DAAS_KEY4 and K.KAS_BONNR = DAAS_KEY5 AND DAAS_SET_NAME = 'SALE' AND DAAS_UPDATE_TIME < " + sqlLastUpdate + " ) " +

                " and not exists(select * from  V_KASIDLTA D  " +
                " where D.KAS_DATUM = K.KAS_DATUM and K.KAS_FILIALE = D.KAS_FILIALE and KAS_SATZART in ( 10, 12, 20) and K.KAS_MANDANT = D.KAS_MANDANT " +
                " and K.KAS_KASSE = D.KAS_KASSE and K.KAS_BONNR = D.KAS_BONNR) " +

                " union all " +
                " select distinct " + top10 + " KAS_MANDANT, K.KAS_DATUM, K.KAS_FILIALE, K.KAS_KASSE, K.KAS_BONNR, KAS_BERICHT, IsNull(F.FIL_INDEX, '') [FIL_INDEX], " +
                " IsNull(KAS_ZEIT, 0) [KAS_ZEIT], isNull(ZZO_STD_NAME, '') [ZZO_STD_NAME]   " +
                " from V_KASSTRNS K  WITH (NOLOCK) " +
                " left join V_FILIALEN F on K.KAS_FILIALE = F.FIL_NUMMER and F.FIL_MANDANT = K.KAS_MANDANT" +
                " left join V_ZEITZONE Z on FIL_ZEITZONE = Z.ZZO_INDEX and ZZO_MANDANT = K.KAS_MANDANT " +
                " where ((K.KAS_VK_DATUM > " + anInterval + " and  K.KAS_VK_DATUM > " + startingDate + ") or (K.KAS_VK_DATUM >= " + dateFrom + " and K.KAS_VK_DATUM <= " + dateTo + ") ) " +

                " and not exists(select * from DAAS_EXPORT D " +
                "   where DAAS_KEY1 = K.KAS_DATUM and K.KAS_FILIALE = DAAS_KEY2 and K.KAS_MANDANT = DAAS_KEY3 " +
                "   and K.KAS_KASSE = DAAS_KEY4 and K.KAS_BONNR = DAAS_KEY5 AND DAAS_SET_NAME = 'SALE' AND DAAS_UPDATE_TIME<  " + sqlLastUpdate + " ) " +

                " and not exists(select * from  V_KASSTRNS D  " +
                " where D.KAS_DATUM = K.KAS_DATUM and K.KAS_FILIALE = D.KAS_FILIALE and KAS_SATZART in ( 10, 12, 20) and K.KAS_MANDANT = D.KAS_MANDANT " +
                " and K.KAS_KASSE = D.KAS_KASSE and K.KAS_BONNR = D.KAS_BONNR) " +

                " union all " +
                " select distinct " + top10 + " KAS_MANDANT, K.KAS_DATUM, K.KAS_FILIALE, K.KAS_KASSE, K.KAS_BONNR, KAS_BERICHT, IsNull(F.FIL_INDEX, '') [FIL_INDEX], " +
                " IsNull(KAS_ZEIT, 0) [KAS_ZEIT], isNull(ZZO_STD_NAME, '') [ZZO_STD_NAME]   " +
                " from V_KASSE K  WITH (NOLOCK) " +
                " left join V_FILIALEN F on K.KAS_FILIALE = F.FIL_NUMMER and F.FIL_MANDANT = K.KAS_MANDANT" +
                " left join V_ZEITZONE Z on FIL_ZEITZONE = Z.ZZO_INDEX and ZZO_MANDANT = K.KAS_MANDANT " +
                " where ((K.KAS_DATUM > " + anInterval + " and  K.KAS_DATUM > " + startingDate + ") or (K.KAS_DATUM >= " + dateFrom + " and K.KAS_DATUM <= " + dateTo + ") ) " +
                " and not exists(select * from DAAS_EXPORT D " +
                "   where DAAS_KEY1 = K.KAS_DATUM and K.KAS_FILIALE = DAAS_KEY2 and K.KAS_MANDANT = DAAS_KEY3 " +
                "   and K.KAS_KASSE = DAAS_KEY4 and K.KAS_BONNR = DAAS_KEY5 AND DAAS_SET_NAME = 'SALE' AND DAAS_UPDATE_TIME <  " + sqlLastUpdate + " ) " +
                " and not exists(select * from V_KASSE D  " +
                " where D.KAS_DATUM = K.KAS_DATUM and K.KAS_FILIALE = D.KAS_FILIALE and KAS_SATZART in ( 10, 12, 20) and K.KAS_MANDANT = D.KAS_MANDANT" +
                " and K.KAS_KASSE = D.KAS_KASSE and K.KAS_BONNR = D.KAS_BONNR) ";

            sqlstr = "select KAS_MANDANT, KAS_DATUM, KAS_FILIALE, KAS_KASSE, KAS_BONNR, max(ZZO_STD_NAME) [ZZO_STD_NAME] , max(KAS_ZEIT)[KAS_ZEIT], max(FIL_INDEX) [FIL_INDEX] from (" + sqlstr +

                " ) tbl group by KAS_MANDANT, KAS_DATUM, KAS_FILIALE, KAS_KASSE, KAS_BERICHT  order by 2,3,4,5,1   option (force order)";

            using(SqlCommand cmd = new SqlCommand(sqlstr, ersConnection))
            {
                using (SqlDataReader areader = cmd.ExecuteReader())
                {
                    while (areader.Read())
                    {

                    }
                }

            }


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


        public void process()
        {
            Logging.WriteDebug("Starting Process ", dcSetup.Debug);
            SqlConnection ersConnection = null;
            ersConnection = openERSSQLConnection();
            if(ersConnection == null)
            {
                return;
            }

            try
            {
                if (!dcSetup.BlockSales)
                {
                    getSales(ersConnection);
                }

                if (!dcSetup.BlockProducts)
                {
                    getProducts_1(ersConnection);
                }

                if (!dcSetup.BlockPrices)
                {
                    getPrices_1(ersConnection);
                }
                ACounter++;
                Logging.WriteDebug("ACounter: " + ACounter.ToString(), dcSetup.Debug);
                if (ACounter >= 100)
                {
                    ACounter = 0;
                }



                int aRem = ACounter % dcSetup.LocationModule;
                Logging.WriteDebug("aRem: " + aRem.ToString(), dcSetup.Debug);
                if (aRem == 0 && (!dcSetup.BlockLocations) )
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
                    sAnsModify =  aresult.ToString();
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
            if(sqlLastUpdate.ToString().Length == 14)
            {
                supdateDate = sqlLastUpdate.ToString().Substring(0, 8);
                supdateTime = sqlLastUpdate.ToString().Substring(8);
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
                " where AGR_MANDANT = 1 AND  (AGR_ULOG_DATE > " + supdateDate + " or (AGR_ULOG_DATE = " + supdateDate + " and AGR_ULOG_TIME >= 0" + supdateTime + ") )";
            //"cast(AGR_ULOG_DATE as varchar) + right('000000' + cast(AGR_ULOG_TIME AS VARCHAR), 6) >= '" + sqlLastUpdate + "'";

            Logging.WriteDebug(anSql, dcSetup.Debug)
;

            String attrSql = "select ARC_CODE, ARC_VALUE, ARC_TEXT from V_ART_CODE WHERE " +
                " ARC_MANDANT = 1 AND ARC_WARENGR = @ARC_WARENGR AND ARC_ABTEILUNG = @ARC_ABTEILUNG AND ARC_TYPE = @ARC_TYPE AND ARC_GRPNUMMER = @ARC_GRPNUMMER";


            using (SqlCommand cmdmain = new SqlCommand(anSql, ersConnection))
            {
                using (SqlDataReader mainReader = cmdmain.ExecuteReader())
                {
                    while(mainReader.Read())
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
                                }

                            }
                        }

                        String skuSql = "select ART_REFNUMMER[SkuId], ART_SORTIERUNG[Sort], ART_EINHEITTEXT[UnitText], ART_EIGENTEXT[VariantText], ART_LFID_NUMMER[RefNummer], " +
                            " ART_SAISON[StatisticalPeriodNo], SPE_TEXT[StatisticalPeriod], ART_MAXRABATT[MaximumDiscount], ART_KEIN_RABATT[FixedPrice], " +
                            " ART_CMP_TYP[QtyTypeForComparativePrice], ART_CMP_ISTMENGE[ComparativeQtyForComparativePrice], ART_CMP_REFMENGE[QtyForComparativePrice], " +
                            " ART_ZWEIT_LFID[POSupplierItemNumber], ART_VKPREIS[RT_Price], ART_EKWAEHRUNG[Currency], ART_NEUEK_DM [PurchasePrice],ART_ZWEITLIEFERANT [PO_Supplier], " +
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
                                    sku.RefNummer = areader["RefNummer"].ToString();
                                    sku.StatisticalPeriodNo = Logging.strToIntDef(areader["StatisticalPeriodNo"].ToString(), 0);
                                    sku.StatisticalPeriod = areader["StatisticalPeriod"].ToString();
                                    sku.MaximumDiscount = Logging.strToDoubleDef(areader["MaximumDiscount"].ToString(), 0);
                                    sku.FixedPrice = Logging.strToDoubleDef(areader["FixedPrice"].ToString(), 0);
                                    sku.WeightedAverageCost = Logging.strToDoubleDef(areader["WeightedAverageCost"].ToString(), 0);
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

                                    String attrSkuSql = "select ADC_CODE, ADC_VALUE, ADC_TEXT from V_ART_DCOD where ADC_MANDANT = 1 AND ADC_REFNUMMER = @ADC_REFNUMMERY";
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

            String anSql = "select " + sTop + " AGR_WARENGR [ProductGroup], AGR_ABTEILUNG[Subgroup], AGR_TYPE[Type],AGR_GRPNUMMER [GroupNumber] " +
                " from V_ART_KOPF " +
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

                        String skuSql = "select ART_REFNUMMER[SkuId], ART_MAXRABATT[MaximumDiscount], ART_KEIN_RABATT[FixedPrice], ART_NEUEK_DM [PurchasePrice], " +
                            " ART_EKWAEHRUNG[Currency], ART_VKPREIS[RT_Price], " +
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
                                    anSku.Currency = areader["Currency"].ToString();
                                    anSku.PP_Price = Logging.strToDoubleDef(areader["PurchasePrice"].ToString(), 0);
                                    anSku.WeightedAverageCost = Logging.strToDoubleDef(areader["WeightedAverageCost"].ToString(), 0);

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
                            sku.RefNummer = areader["RefNummer"].ToString();
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
                " ISNULL(K1.KTO_TEXT, '') [POS_ACCOUNT_NAME], FIL_WARENKTO[PRODUCT_ACCOUNT],ISNULL(K2.KTO_TEXT, '')[PRODUCT_ACCOUNT_NAME], FIL_WARENKST[COST_CENTRE], ISNULL(KST_TEXT, '')[COST_CENTRE_NAME], " +
                " FIL_STEUERGRUPPE[TAX_GROUP], ISNULL(SGR_TEXT, '')[TAX_GROUP_NAME], FIL_INDEX, ADD_SPRACHE[LANGUAGE], ANS_NAME1[NAME1], ANS_NAME2[NAME2], ANS_LAND[COUNTRY], LAN_TEXT[COUNTRY_NAME], " +
                " ANS_COUNTY[STATE], ANS_PLZ[POSTCODE], ANS_ORT[SUBURB], rtrim(ANS_STRASSE + ' ' + ANS_STRASSE_2)[ADDRESS], ANS_TELEFON[PHONE1], ANS_TELEFON2[PHONE2], ANS_TELEFAX[FAX], ANS_EMAIL[EMAIL], " +
                " FIL_ZEITZONE[TIMEZONE], ISNULL(ZZO_NAME_DISPLAY, '')[TIMEZONE_NAME] " +
                " from V_FILIALEN " +
                " JOIN V_ADR_DATA ON FIL_NUMMER = ADD_NUMMER AND ADD_TYP = FIL_TYP AND ADD_MANDANT = 1 " +
                " JOIN V_ANSCHRIF ON ANS_TYP = FIL_TYP AND ANS_NUMMER = FIL_NUMMER AND ANS_COUNT = 1 AND ANS_MANDANT = 1" +
                " JOIN V_LAND ON ANS_LAND = LAN_NUMMER  and LAN_MANDANT = 1 " +
                " LEFT JOIN V_WAEHRUNG ON WAE_CODE = FIL_WAEHRUNG AND WAE_MANDANT = 1 " +
                " LEFT JOIN V_KONTO K1 ON K1.KTO_NUMMER = FIL_KASSENKTO AND K1.KTO_MANDANT = 1 " +
                " LEFT JOIN V_KONTO K2 ON K2.KTO_NUMMER = FIL_WARENKTO  AND K2.KTO_MANDANT = 1 " +
                " LEFT JOIN V_KOSTENST ON KST_NUMMER = FIL_WARENKST AND KST_MANDANT = 1 " +
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


            String attrSql = "select AAV_CODE, AAV_UNIQUE, AAV_VALUE, AAV_TEXT from V_ADRATVAL WHERE AAV_TYP = @AAV_TYP AND AAV_NUMMER = @AAV_NUMMER AND AAV_MANDANT = 1 ";

            for (int k = 0; k < dsLocations.Rows.Count; k++)
            {
                LocationsJson locationsJson = new LocationsJson();
 
                Location location = new Location();
                locationsJson.Location = location;

                DataRow arow = dsLocations.Rows[k];
                int filTyp = Convert.ToInt32(arow["FIL_TYP"]);

                location.Address = arow["ADDRESS"].ToString();
                location.RegionName = arow["REGION_NAME"].ToString();
                location.Region = Convert.ToInt32(arow["REGION"]);

                location.BranchNo = Convert.ToInt32(arow["BRANCH_NO"]);
                location.Currency = arow["CURRENCY"].ToString();
                location.CurrencyName = arow["CURRENCY_NAME"].ToString();
                location.Region = Convert.ToInt32(arow["REGION"]);
                location.RegionName = arow["REGION_NAME"].ToString();
                location.PosAccount = Convert.ToInt32(arow["POS_ACCOUNT"]);
                location.PosAccountName = arow["POS_ACCOUNT_NAME"].ToString();
                location.ProductAccount = Convert.ToInt32(arow["PRODUCT_ACCOUNT"]);
                location.ProductAccountName = arow["PRODUCT_ACCOUNT_NAME"].ToString();
                location.CostCentre = Convert.ToInt32(arow["COST_CENTRE"]);
                location.CostCentreName = arow["COST_CENTRE_NAME"].ToString();
                location.TaxGroup = Convert.ToInt32(arow["TAX_GROUP"]);
                location.TaxGroupName = arow["TAX_GROUP_NAME"].ToString();
                location.Language = arow["LANGUAGE"].ToString();
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
                location.Timezone = Convert.ToInt32(arow["TIMEZONE"]);
                location.TimezoneName = arow["TIMEZONE_NAME"].ToString();

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
