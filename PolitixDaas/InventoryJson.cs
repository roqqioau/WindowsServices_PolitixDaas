using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PolitixDaas
{
    public class InventoryJson
    {
        public InventoryItem InventoryLine { get; set; }
    }

    public class InventoryItem
    {
        public int BranchNo { get; set; }
        public int Sku { get; set; }
        public double StockOnHand { get; set; }
        //public double StockonTransit { get; set; }
        public double WeightedAverageCost { get; set; }
        public double StockInTransit { get; set; }
        public int Inventorydate { get; set; }

    }

    public class StockTransferLine
    {
        public int BranchNo { get; set; }
        public int Sku { get; set; }
        public int TransferDate { get; set; }
        public double Qty { get; set; }
        public bool Taken { get; set; }

        public StockTransferLine()
        {
            Taken = false;
        }


    }


}
