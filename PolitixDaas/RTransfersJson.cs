using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PolitixDaas
{
    public class RTransfersJson
    {
        public TransferHeader TransferFromHO { get; set; }
    }

    public class TransferHeader
    {
        public String TrasferNo { get; set; }
        public int FromStore { get; set; }
        public int ToStore { get; set; }
        public int CreationDate { get; set; }
        public int DespatchDate { get; set; }
        public int RequestUser { get; set; }
        public List<TransferDetails> Details { get; set; }
    }

    public class TransferDetails
    {
        public String TransferNo { get; set; }
        public int LineNo { get;  set; }
        public int SkuId { get; set; }
        public double Cost { get; set; }
        public double RetailPrice { get; set; }
        public double Qty { get; set; }

    }

}
