using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PolitixDaas
{
    public class SalesOLJson
    {
        public SalesOLHeader SalesOnLineHeader { get; set; }
    }

    public class SalesOLHeader
    {
        public String TrasferNo { get; set; }
        public int FromStore { get; set; }
        public int ToStore { get; set; }
        public int CreationDate { get; set; }
        public int DespatchDate { get; set; }
        public String ODShipmentId { get; set; }
        public String ODOrderId { get; set; }
        public String ODExternalOrderId { get; set; }
        public int Status_Id { get; set; }
        public String Status { get; set; }
        public List<SalesOLDetails> Details { get; set; }

    }

    public class SalesOLDetails
    {
        public String TransferNo { get; set; }
        public int LineNo { get; set; }
        public int SkuId { get; set; }
        public double WAC { get; set; }
        public double RetailPrice { get; set; }
        public double Qty { get; set; }

    }

}