using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PolitixDaas
{
    public class ShipmentJson
    {
        public GoodsIn Shipment { get; set; }
    }

    public class GoodsIn
    {
        public String Id { get; set; }
        public int ShipmentDate { get; set; }
        public String Text { get; set; }
        public String Currency { get; set; }
        public List<GoodsInLine> Lines { get; set; }
    }

    public class GoodsInLine
    {
        public int LineNo { get; set; }
        public int SkuId { get; set; }
        public int OrderNo { get; set; }
        public int OrderPosition { get; set; }
        public double QtyOrderd { get; set; }
        public double DeliveryNoteQty { get; set; }
        public double QtyInvoiced { get; set; }
        public double QtyDelivered { get; set; }
        public double PP_Price { get; set; }


    }


}
