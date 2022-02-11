using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PolitixDaas
{
    class LocationsJson
    {
        public Location Location { get; set; }
    }



    public class LOCATION
    {
        public int BRANCH_NO { get; set; }
        public String BRANCH_NAME { get; set; }

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
        public String Code { get; set; }
        public int Unique { get; set; }
        public int Value { get; set; }
        public String Text { get; set; }
    }

    public class Location
    {
        public int BranchNo { get; set; }
        public String BranchName { get; set; }
        public string Currency { get; set; }
        public string CurrencyName{ get; set; }
        public int Region { get; set; }
        public string RegionName { get; set; }
        public int PosAccount { get; set; }
        public string PosAccountName { get; set; }
        public int ProductAccount { get; set; }
        public string ProductAccountName { get; set; }
        public int CostCentre { get; set; }
        public string CostCentreName { get; set; }
        public int TaxGroup { get; set; }
        public string TaxGroupName { get; set; }
    //    public string FilIndex { get; set; }
        public string Language { get; set; }
        public string Name1 { get; set; }
        public string Name2 { get; set; }
        public int Country { get; set; }
        public string CountryName { get; set; }
        public string State { get; set; }
        public string Postcode { get; set; }
        public string Suburb { get; set; }
        public string Address { get; set; }
        public string Phone1 { get; set; }
        public string Phone2 { get; set; }
        public string Fax { get; set; }
        public string Email { get; set; }
        public int Timezone { get; set; }
        public string TimezoneName { get; set; }
        public int Priority { get; set; }

        public String GstId { get; set; }
        public String GstReg { get; set; }
        public int AllocationPossible { get; set; }

        public List<ATTRIBUTE> Attributes { get; set; }

    }


}
