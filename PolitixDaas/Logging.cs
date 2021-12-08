using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace PolitixDaas
{
    class Logging
    {

        public static string CreateMD5(string input)
        {
            // Use input string to calculate MD5 hash
            using (System.Security.Cryptography.MD5 md5 = System.Security.Cryptography.MD5.Create())
            {
                byte[] inputBytes = System.Text.Encoding.ASCII.GetBytes(input);
                byte[] hashBytes = md5.ComputeHash(inputBytes);

                // Convert the byte array to hexadecimal string
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < hashBytes.Length; i++)
                {
                    sb.Append(hashBytes[i].ToString("X2"));
                }
                return sb.ToString();
            }
        }

        static string generateHash(string input)
        {
            MD5 md5Hasher = MD5.Create();
            byte[] data = md5Hasher.ComputeHash(Encoding.Default.GetBytes(input));
            return BitConverter.ToString(data);
        }



        public static string GetLast(string source, int last)
        {
            return last >= source.Length ? source : source.Substring(source.Length - last);
        }

        //adate as YYYYMMDD.fraction of day
        public static String doubleDateToString(double adate)
        {
            String ares = "";

            int idate = (int)Math.Truncate(adate);

            double fracy = adate - idate;

            int totSecs = (int)Math.Truncate(fracy * 60 * 60 * 24);
            int tHours = (int)(totSecs / (60 * 60));
            int adelta = totSecs - (tHours * 60 * 60);
            int tMins = (int)(adelta / 60);
            int tSecs = adelta % 60;

            ares = idate.ToString() + GetLast("00" + tHours.ToString(), 2) + GetLast("00" + tMins.ToString(), 2) + GetLast("00" + tSecs.ToString(), 2);

            return ares;
        }

        public static int strToIntDef(String avalue, int defvalue)
        {
            int res = 0;
            try
            {
                res = Convert.ToInt32(avalue);
            }
            catch
            {
                res = defvalue;
            }
            return res;
        }

        public static Int64 strToInt64Def(String avalue, Int64 defvalue)
        {
            Int64 res = 0;
            try
            {
                res = Convert.ToInt64(avalue);
            }
            catch
            {
                res = defvalue;
            }
            return res;
        }

        //DateTime in yyyyMMddhhmmss
        public static DateTime futuraDateToDateTime(String aDate)
        {
            DateTime ares = DateTime.MinValue;
            ares = DateTime.ParseExact(aDate, "yyyyMMddhhmmss", System.Globalization.CultureInfo.InvariantCulture);
            return ares;
        }


        public static String FuturaDateTimeAddMins(String FuturaDateTime, int minutes)
        {
            String ares = "0";
            try
            {
                DateTime adate = futuraDateToDateTime(FuturaDateTime);
                DateTime updated = adate.Add(new TimeSpan(0, minutes, 0));
                ares = updated.ToString("yyMMddhhmmss");
            }
            catch { }


            return ares;
        }

        public static double strToDoubleDef(String avalue, double defvalue)
        {
            double res = 0;
            try
            {
                res = Convert.ToDouble(avalue);
            }
            catch
            {
                res = defvalue;
            }
            return res;
        }


        //Date in yyyymmdd returns yyyy-mm-dd
        public static String dateIntToStr(int adate)
        {
            String inputDate = adate.ToString();
            if (inputDate.Length != 8)
            {
                return "";
            }
            return inputDate.Substring(0, 4) + "-" + inputDate.Substring(4, 2) + "-" + inputDate.Substring(6, 2);
        }

        public static List<String> lstErrors = new List<string>();

        public static String getErrors()
        {
            StringBuilder strBuilder = new StringBuilder();
            for (int i = 0; i < lstErrors.Count; i++)
            {
                strBuilder.Append(lstErrors[i] + "\n");
            }
            return strBuilder.ToString();
        }
        public static void CreateLogDirs()
        {
            try
            {
                if (!Directory.Exists(AppDomain.CurrentDomain.BaseDirectory + "\\log"))
                    Directory.CreateDirectory(AppDomain.CurrentDomain.BaseDirectory + "\\log");
            }
            catch { }
        }
        public static void WriteErrorLog(Exception ex)
        {
            String atime = DateTime.Now.ToString();
            lstErrors.Add(atime + " : " + ex.Source.ToString().Trim() + " : " + ex.Message.Trim());
            WriteLog(ex.Source.ToString().Trim() + " : " + ex.Message.Trim(), atime);
            StreamWriter sw = null;
            try
            {
                sw = new StreamWriter(AppDomain.CurrentDomain.BaseDirectory + "\\log\\errorlog" + DateTime.Now.ToString("yyyyMMdd") + ".txt", true);
                sw.WriteLine(atime + " : " + ex.Source.ToString().Trim() + " : " +
                    ex.Message.Trim());
                sw.Flush();
                sw.Close();
            }
            catch
            {

            }

        }

        public static void WriteLog(String aMessage, String strTime)
        {
            CreateLogDirs();
            StreamWriter sw = null;
            try
            {
                sw = new StreamWriter(AppDomain.CurrentDomain.BaseDirectory + "\\log\\log" + DateTime.Now.ToString("yyyyMMdd") + ".txt", true);
                sw.WriteLine(strTime + " : " + aMessage);
                sw.Flush();
                sw.Close();
            }
            catch
            {

            }

        }

        public static void WriteDebug(String aMessage, int debugLevel)
        {
            if (debugLevel > 0)
            {
                WriteLog(aMessage);
            }
        }

        public static void WriteLog(String aMessage)
        {
            CreateLogDirs();
            StreamWriter sw = null;
            try
            {
                sw = new StreamWriter(AppDomain.CurrentDomain.BaseDirectory + "\\log\\log" + DateTime.Now.ToString("yyyyMMdd") + ".txt", true);
                sw.WriteLine(DateTime.Now.ToString() + " : " + aMessage);
                sw.Flush();
                sw.Close();
            }
            catch
            {

            }

        }


        public static void WriteErrorLog(String aMessage)
        {
            String atime = DateTime.Now.ToString();
            lstErrors.Add(atime + " : " + aMessage.Trim());
            WriteLog(aMessage, atime);
            StreamWriter sw = null;
            try
            {
                sw = new StreamWriter(AppDomain.CurrentDomain.BaseDirectory + "\\log\\errorlog" + DateTime.Now.ToString("yyyyMMdd") + ".txt", true);
                sw.WriteLine(atime + " : " + aMessage);
                sw.Flush();
                sw.Close();
            }
            catch
            {

            }

        }


    }
}
