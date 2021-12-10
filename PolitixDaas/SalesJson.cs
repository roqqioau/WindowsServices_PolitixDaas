using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PolitixDaas
{
    public class SalesJson
    {
        public Sale FuturaSale {get; set;}
    }

    public class Sale
    {
        public Int64 ReceiptId { get; set; }
        public String ReceiptNo { get; set; }
        public String BranchNo { get; set; }
        public String CashierId { get; set; }
        public String ReceiptState { get; set; }
        public String ReceiptMode { get; set; }
        public String SaleType { get; set; }
        public String CustomerNo { get; set; }
        public String EmployeeId { get; set; }
        public List<SaleLine> SaleLines { get; set; }

    }

    public class SaleLine
    {
        public String SalesMode { get; set; }
        public double Qty { get; set; }
        public double Price { get; set; }
        public double OriginalPrice { get; set; }
        public double RoundingDifference { get; set; }
        public double LineValueNet { get; set; }
        public double LineValueGross { get; set; }
        public String SalesPersonId { get; set; }
        public List<ValDetail> ValDetail { get; set; }
        public int ProductGroupId { get; set; }
        public Int64 SkuId { get; set; }
        public Int64 CustomerId { get; set; }
        public double VatPercent { get; set; }
        public double VatAmount { get; set; }
        public String ReturnReason { get; set; }
        public List<DiscountLine> DiscountLines { get; set; }
        public String VoucherNumber { get; set; }
        public Int64 PaymentType { get; set; }
        public String SaleLineType { get; set; }
        public List<LineTextAttribute> Attributes { get; set; }
        public String OrderNo { get; set; }
        public double Amount { get; set; }


    }

    public class LineTextAttribute
    {
        public String TextType { get; set; }
        public int Seq { get; set; }
        public String TextValue { get; set; }
    }

    public class ValDetail
    {

    }

    public class PaymentLine
    {
        public String PaymentType { get; set; }
        public double Amount { get; set; }
        public String RefNumber { get; set; }
        public List<LineTextAttribute> Attributes { get; set; }
        public double ForeignAmount { get; set; }
    }

    public class DiscountLine
    {
        public String DiscountReason { get; set; }
        public double Amount { get; set; }
        public String RefNumber { get; set; }
        public int LoyaltyPoints { get; set; }

    }


}
