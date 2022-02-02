using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PolitixDaas
{
    public class PermanentMarkdownsJson
    {
        List<PrmanentMarkdown> Matkdown { get; set; }
    }



    public class PrmanentMarkdown
    {
        public int SkuId { get; set; }

        public List<PricePerCode> Prices { get; set; }
    }
}
