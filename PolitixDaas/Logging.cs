using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PolitixDaas
{
    class Logging
    {
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
