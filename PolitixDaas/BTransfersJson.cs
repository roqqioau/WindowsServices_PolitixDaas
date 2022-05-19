using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PolitixDaas
{
    public class BTransfersJson
    {
        public TransferBHeader TransferFromBranch { get; set; }
    }

    public class TransferBHeader
    {
        public String TrasferNo { get; set; }
        public int FromStore { get; set; }
        public int ToStore { get; set; }
        public int CreationDate { get; set; }
        public int AcknowledgeDate { get; set; }
        public int DespatchDate { get; set; }

        public String DeliveryNoteNumber { get; set; }
        public String DeliveryNote { get; set; }

        public String Type { get; set; }
        public int RequestUser { get; set; }
        public List<TransferBDetails> Details { get; set; }
        public String Text { get; set; }
        public String DNDate { get; set; }

    }

    public class TransferBDetails
    {
        public int LineNo { get; set; }
        public int SkuId { get; set; }
        public double UnitPrice { get; set; }
        public double Qty { get; set; }

    }


}



