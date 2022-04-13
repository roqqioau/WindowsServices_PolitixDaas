using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PolitixDaas
{
    public class InventoryAdjustmentJson
    {
        public InventoryAdjustment InventoryAdjustmentLine { get; set; }
    }

    public class InventoryAdjustment
    {
   
        public String AdjustmentNumber { get; set; }
        
        public int AdjustmentDate { get; set; }
        public int SkuId { get; set; }
        public int BranchNo { get; set; }
        public int UniqueNo { get; set; }
        public double AdjustmentQty { get; set; }
        public double WeightedAverageCost { get; set; }
        public double RetailPrice { get; set; }
        public int AdjustmentReasonId { get; set; }
        public String AdjustmentReason { get; set; }
        public int EmployeeId { get; set; }


    }
}
