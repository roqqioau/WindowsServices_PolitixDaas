using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PolitixDaas
{
    public class POJson
    {
        public POOrder Order { get; set; }
    }

    public class POOrder
    {
        public String OrderId { get; set; }
        public String Supplier { get; set; }
        public int OrderDate {get;set;}
        public int ArrivalDate { get; set; }
        public String Text { get; set; }
        public double TotalOrderQty { get; set; }
        public double TotalPurchaseValue { get; set; }
        public double TotalRetailValue { get; set; }
//        public int TransferDate { get; set; }
        public List<PODetails> POLines { get; set; }
    }

    public class PODetails
    {
        public String OrderId { get; set; }
        public int Line { get; set; }
        public int SkuId { get; set; }
        public String Description { get; set; }
        public double OrderQty { get; set; }
        public double DeliveredQty { get; set; }
        public double PurchaseValue { get; set; }
        public double RetailValue { get; set; }
        public double WAC { get; set; }


    }


}
