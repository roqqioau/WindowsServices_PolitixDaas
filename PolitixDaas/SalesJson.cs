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
        public String ReceiptId { get; set; }
        public String ReceiptNo { get; set; }
        public String BranchNo { get; set; }
        public String TillNo { get; set; }
        public String CashierId { get; set; }
        public String ReceiptState { get; set; }
        public String ReceiptMode { get; set; }
        public String SaleType { get; set; }
        public String CustomerNo { get; set; }
        public String EmployeeId { get; set; }
        public String Timestamp { get; set; }
        public List<SaleLine> SaleLines { get; set; }
        public List<PaymentLine> PaymentLines { get; set; }


    }

    public class SaleLine
    {
        public String SalesMode { get; set; }
        public double Qty { get; set; }
        public double Price { get; set; }
        public double OriginalPrice { get; set; }
        public double LineValueNet { get; set; }
        public double LineValueGross { get; set; }
        public String SalesPersonId { get; set; }
        public int ProductGroupId { get; set; }
        public int SkuId { get; set; }
        public String VatHeadEntityId { get; set; }
        public double VatPercent { get; set; }
        public double VatAmount { get; set; }  
        public String ReturnReason { get; set; }
        public List<DiscountLine> DiscountLines { get; set; }
        public String VoucherNumber { get; set; }
        public String VoucherPaymentTypeId { get; set; }
        public String VoucherPaymentType { get; set; }
        public String SaleLineType { get; set; }


    }

    public class LineTextAttribute
    {
        public String TextType { get; set; }
        public int Seq { get; set; }
        public String TextValue { get; set; }
    }


    public class PaymentLine
    {
        public int PaymentTypeId { get; set; }
        public String PaymentType { get; set; }
        public double Amount { get; set; }
        public String RefNumber { get; set; }
        public String Currency { get; set; }
    }

    public class DiscountLine
    {
        public String DiscountReasonId { get; set; }
        public String DiscountReason { get; set; }
        public double Amount { get; set; }

    }


}
