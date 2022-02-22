using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PolitixDaas
{
    class ItemsJson
    {
        public StockItem Item { get; set; }
    }



    public class StockItem
    {
        public int ProductGroup { get; set; }
        public String ProductGroupDescription { get; set; }
        public int Subgroup { get; set; }
        public String SubgroupDescription { get; set; }
        public int Type { get; set; }
        public String TypeDescription { get; set; }
        public int GroupNumber { get; set; }
    //    public String GroupNumberDescription { get; set; }
        public string ReceiptText { get; set; }
        public string LongDescription { get; set; }
        public int DeliveryType { get; set; }
        public int SupplierItemGroup { get; set; }
        public int Origin { get; set; }
        public string OriginText { get; set; }
        public int SerialNumberEntry { get; set; }
        public int SalesAreaNo { get; set; }
        public string SalesArea { get; set; }
        public int LabelType { get; set; }
        public int LabelPerPiece { get; set; }
        public String SupplierItemGroupIndex { get; set; }
        public string SupplierItemDescription { get; set; }
        public List<StockAttribute> ItemAttributes;

        public List<Sku> Skus { get; set; }

    }

    public class StockAttribute
    {
        public String Code { get; set; }
        public int Value { get; set; }
        public String Text { get; set; }
        public String Description { get; set; }

    }

    public class Sku
    {
        public int SkuId { get; set; }
        public int Sort { get; set; }
        public string UnitText { get; set; }
        public string VariantText { get; set; }
        public string SpltmNo { get; set; }
        public int StatisticalPeriodNo { get; set; }
        public string StatisticalPeriod { get; set; }
        public double MaximumDiscount { get; set; }
        public double FixedPrice { get; set; }
        public string QtyTypeForComparativePrice { get; set; }
        public double ComparativeQtyForComparativePrice { get; set; }
        public double QtyForComparativePrice { get; set; }
        public String PO_Supplier { get; set; }
        public string POSupplierItemNumber { get; set; }
        public double RT_Price { get; set; }
        public double PP_Price { get; set; }
        public string Currency { get; set; }
     //   public double WeightedAverageCost { get; set; }
        public List<EanCode> EanCodes { get; set; }
   //     public List<PricePerBranch> PricesPerBranch { get; set; }
   //     public List<PricePerCode> Prices { get; set; }
        public List<StockAttribute> skuAttributes { get; set; }
    }



    public class EanCode
    {
        public String ECode { get; set; }
        public int Sorting { get; set; }
    }

    public class PricePerBranch
    {
        public int BranchNo { get; set; }
        public double Price { get; set; }
        public double BranchPrice { get; set; }
       // public double MaxDiscount { get; set; }
    }

    public class PricePerCode
    {
        public int PriceCode { get; set; }
        public double Price { get; set; }
        public int Date { get; set; }
    }


    public class PriceRoots
    {
       public  ProductPrices ProductPrice { get; set; }
    }

    public class ProductPrices
    {
        public PricesItem Item { get; set; }
    }

    public class PricesItem
    {
        public int ProductGroup { get; set; }
        public int Subgroup { get; set; }
        public int Type { get; set; }
        public int GroupNumber { get; set; }
    //    public String ProductGroupDescription { get; set; }
    //    public String SubgroupDescription { get; set; }
    //    public String TypeDescription { get; set; }
    //    public String GroupNumberDescription { get; set; }
        public List<PricesSku> Skus { get; set; }

    }

    public class PricesSku
    {
        public int SkuId { get; set; }
        public double RT_Price { get; set; }
        public double PP_Price { get; set; }
        public double WS_Price { get; set; }
        public string Currency { get; set; }
        public double WeightedAverageCost { get; set; }
        public List<PricePerBranch> PricesPerBranch { get; set; }
        public List<PricePerCode> Prices { get; set; }


    }



}
