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

            string brokerUri = $"activemq:tcp://" +  dcSetup.ActiveMQUrl;  
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


        public void process()
        {
            SqlConnection ersConnection = openERSSQLConnection();
            if(ersConnection == null)
            {
                return;
            }
            getLocations(ersConnection);

            try
            {
                houseKeeping(AppDomain.CurrentDomain.BaseDirectory + "\\log", "log*.txt");
                houseKeeping(AppDomain.CurrentDomain.BaseDirectory + "\\log", "errorlog*.txt");
            }
            catch { }


        }

        private void getLocations(SqlConnection ersConnection)
        {
            int lastUpdate = dcSetup.LocationUpdate;

            String anSql = "SELECT FORMAT(max(modify_date), 'yyyyMMdd')[MODIFY_DATE] " +
                " FROM sys.tables where name = 'FILIALEN'";

            int filiaModify = 0;
            using(SqlCommand cmd = new SqlCommand(anSql, ersConnection))
            {
                var aresult = cmd.ExecuteScalar();
                if (aresult != null)
                {
                    filiaModify = Logging.strToIntDef(aresult.ToString(), 0);
                }
            }
            anSql = "SELECT " +
                " case " +
                "   when cast(max(A.DB_TIME) as int) > cast(max(F.DB_TIME) as int) then cast(max(A.DB_TIME) as int) " +
                "   else cast(max(F.DB_TIME) as int) " +
                " end[DBTIME] " +
                " FROM ANSCHRIF A " +
                " join FILIALEN F on ANS_TYP = FIL_TYP AND ANS_NUMMER = FIL_NUMMER";
            int ansModify = 0;
            using (SqlCommand cmd = new SqlCommand(anSql, ersConnection))
            {
                var aresult = cmd.ExecuteScalar();
                if (aresult != null)
                {
                    ansModify = Logging.strToIntDef(aresult.ToString(), 0);
                }
            }

            if(ansModify < lastUpdate && filiaModify < lastUpdate)
            {
                return;
            }
            String locSql = " select  FIL_TYP, FIL_NUMMER[BRANCH_NO], FIL_WAEHRUNG [CURRENCY], WAE_TEXT [CURRENCY_NAME], FIL_REGION [REGION], ISNULL(REG_TEXT, '') [REGION_NAME], FIL_KASSENKTO [POS_ACCOUNT], " +
                " ISNULL(K1.KTO_TEXT, '') [POS_ACCOUNT_NAME], FIL_WARENKTO[PRODUCT_ACCOUNT],ISNULL(K2.KTO_TEXT, '')[PRODUCT_ACCOUNT_NAME], FIL_WARENKST[COST_CENTRE], ISNULL(KST_TEXT, '')[COST_CENTRE_NAME], " +
                " FIL_STEUERGRUPPE[TAX_GROUP], ISNULL(SGR_TEXT, '')[TAX_GROUP_NAME], FIL_INDEX, ADD_SPRACHE[LANGUAGE], ANS_NAME1[NAME1], ANS_NAME2[NAME2], ANS_LAND[COUNTRY], LAN_TEXT[COUNTRY_NAME], " +
                " ANS_COUNTY[STATE], ANS_PLZ[POSTCODE], ANS_ORT[SUBURB], rtrim(ANS_STRASSE + ' ' + ANS_STRASSE_2)[ADDRESS], ANS_TELEFON[PHONE1], ANS_TELEFON2[PHONE2], ANS_TELEFAX[FAX], ANS_EMAIL[EMAIL], " +
                " FIL_ZEITZONE[TIMEZONE], ISNULL(ZZO_NAME_DISPLAY, '')[TIMEZONE_NAME] " +
                " from FILIALEN " +
                " JOIN ADR_DATA ON FIL_NUMMER = ADD_NUMMER AND ADD_TYP = FIL_TYP " +
                " JOIN ANSCHRIF ON ANS_TYP = FIL_TYP AND ANS_NUMMER = FIL_NUMMER AND ANS_COUNT = 1 " +
                " JOIN LAND ON ANS_LAND = LAN_NUMMER " +
                " LEFT JOIN V_WAEHRUNG ON WAE_CODE = FIL_WAEHRUNG " +
                " LEFT JOIN KONTO K1 ON K1.KTO_NUMMER = FIL_KASSENKTO " +
                " LEFT JOIN KONTO K2 ON K2.KTO_NUMMER = FIL_WARENKTO " +
                " LEFT JOIN KOSTENST ON KST_NUMMER = FIL_WARENKST " +
                " LEFT JOIN STEUER_G ON SGR_NUMMER = FIL_STEUERGRUPPE " +
                " LEFT JOIN REGION ON REG_NUMMER = FIL_REGION " +
                " LEFT JOIN ZEITZONE ON ZZO_INDEX = FIL_ZEITZONE";

            DataTable dsLocations = null;
            using (SqlDataAdapter daTrans = new SqlDataAdapter(locSql, ersConnection))
            {
                dsLocations = new DataTable();
                daTrans.Fill(dsLocations);
            }
            if (dsLocations == null)
                return;

            LocationsJson locationsJson = new LocationsJson();
            locationsJson.LOCATIONS = new List<LOCATION>();

            String attrSql = "select AAV_CODE, AAV_UNIQUE, AAV_VALUE, AAV_TEXT from ADRATVAL WHERE AAV_TYP = @AAV_TYP AND AAV_NUMMER = @AAV_NUMMER";

            for (int k = 0; k < dsLocations.Rows.Count; k++)
            {

                LOCATION location = new LOCATION();
                locationsJson.LOCATIONS.Add(location);

                DataRow arow = dsLocations.Rows[k];
                int filTyp = Convert.ToInt32(arow["FIL_TYP"]);

                location.ADDRESS = arow["ADDRESS"].ToString();
                location.REGION_NAME = arow["REGION_NAME"].ToString();
                location.REGION = Convert.ToInt32(arow["REGION"]);

                location.BRANCH_NO = Convert.ToInt32(arow["BRANCH_NO"]);
                location.CURRENCY = arow["CURRENCY"].ToString();
                location.CURRENCY_NAME = arow["CURRENCY_NAME"].ToString();
                location.REGION = Convert.ToInt32(arow["REGION"]);
                location.REGION_NAME = arow["REGION_NAME"].ToString();
                location.POS_ACCOUNT = Convert.ToInt32(arow["POS_ACCOUNT"]);
                location.POS_ACCOUNT_NAME = arow["POS_ACCOUNT_NAME"].ToString();
                location.PRODUCT_ACCOUNT = Convert.ToInt32(arow["PRODUCT_ACCOUNT"]);
                location.PRODUCT_ACCOUNT_NAME = arow["PRODUCT_ACCOUNT_NAME"].ToString();
                location.COST_CENTRE = Convert.ToInt32(arow["COST_CENTRE"]);
                location.COST_CENTRE_NAME = arow["COST_CENTRE_NAME"].ToString();
                location.TAX_GROUP = Convert.ToInt32(arow["TAX_GROUP"]);
                location.TAX_GROUP_NAME = arow["TAX_GROUP_NAME"].ToString();
                location.LANGUAGE = arow["LANGUAGE"].ToString();
                location.NAME1 = arow["NAME1"].ToString();
                location.NAME2 = arow["NAME2"].ToString();
                location.COUNTRY = Convert.ToInt32(arow["COUNTRY"]);
                location.COUNTRY_NAME = arow["COUNTRY_NAME"].ToString();
                location.STATE = arow["STATE"].ToString();
                location.POSTCODE = arow["POSTCODE"].ToString();
                location.SUBURB = arow["SUBURB"].ToString();
                location.ADDRESS = arow["ADDRESS"].ToString();
                location.PHONE1 = arow["PHONE1"].ToString();
                location.PHONE2 = arow["PHONE2"].ToString();
                location.FAX = arow["FAX"].ToString();
                location.EMAIL = arow["EMAIL"].ToString();
                location.TIMEZONE = Convert.ToInt32(arow["TIMEZONE"]);
                location.TIMEZONE_NAME = arow["TIMEZONE_NAME"].ToString();

                location.ATTRIBUTES = new List<ATTRIBUTE>();

                int attrCounter = 0;
                using (SqlCommand cmd = new SqlCommand(attrSql, ersConnection))
                {
                    cmd.Parameters.AddWithValue("@AAV_TYP", filTyp);
                    cmd.Parameters.AddWithValue("@AAV_NUMMER", location.BRANCH_NO);
                    using(SqlDataReader areader = cmd.ExecuteReader())
                    {
                        while(areader.Read())
                        {
                            attrCounter++;
                            ATTRIBUTE anAttribute = new ATTRIBUTE();
                            location.ATTRIBUTES.Add(anAttribute);
                            anAttribute.CODE = areader.GetString(0);
                            anAttribute.UNIQUE = areader.GetInt32(1);
                            anAttribute.VALUE = areader.GetInt32(2);
                            anAttribute.TEXT = areader.GetString(3);

                        }
                    }
                }

                Logging.WriteLog("JSON " + SimpleJson.SerializeObject(location).ToString());

            }

            Logging.WriteLog("JSON " + SimpleJson.SerializeObject(locationsJson).ToString());

            SendNewMessageQueue(SimpleJson.SerializeObject(locationsJson).ToString(), dcSetup.LocationsQueueName);

            DateTime anow = DateTime.Now;
            String snow = anow.ToString("yyyyMMdd");
            dcSetup.LocationUpdate = Logging.strToIntDef(snow, 0);




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
