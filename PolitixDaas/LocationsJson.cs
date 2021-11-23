using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PolitixDaas
{
    class LocationsJson
    {
        public List<LOCATION> LOCATIONS { get; set; }
    }



    public class LOCATION
    {
        public int BRANCH_NO { get; set; }
        public string CURRENCY { get; set; }
        public string CURRENCY_NAME { get; set; }
        public int REGION { get; set; }
        public string REGION_NAME { get; set; }
        public int POS_ACCOUNT { get; set; }
        public string POS_ACCOUNT_NAME { get; set; }
        public int PRODUCT_ACCOUNT { get; set; }
        public string PRODUCT_ACCOUNT_NAME { get; set; }
        public int COST_CENTRE { get; set; }
        public string COST_CENTRE_NAME { get; set; }
        public int TAX_GROUP { get; set; }
        public string TAX_GROUP_NAME { get; set; }
        public string FIL_INDEX { get; set; }
        public string LANGUAGE { get; set; }
        public string NAME1 { get; set; }
        public string NAME2 { get; set; }
        public int COUNTRY { get; set; }
        public string COUNTRY_NAME { get; set; }
        public string STATE { get; set; }
        public string POSTCODE { get; set; }
        public string SUBURB { get; set; }
        public string ADDRESS { get; set; }
        public string PHONE1 { get; set; }
        public string PHONE2 { get; set; }
        public string FAX { get; set; }
        public string EMAIL { get; set; }
        public int TIMEZONE { get; set; }
        public string TIMEZONE_NAME { get; set; }
        public List<ATTRIBUTE> ATTRIBUTES { get; set; }
    }

    public class ATTRIBUTE
    {
        public String CODE { get; set; }
        public int UNIQUE { get; set; }
        public int VALUE { get; set; }
        public String TEXT { get; set; }
    }


}
