using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PolitixDaas
{
    public class Markdown
    {
        public int PdtDatum { get; set; }
        public int PdtEffectiveDate { get; set; }
        public int PdtNummer { get; set; }
        public String PdtText { get; set; }
    }

    public class MarkdownJson
    {
        public PermanentMarkdown PermanentMD { get; set; }
    }

    public class PermanentMarkdown
    {
        public String Date { get; set; }
        public String ID { get; set; }
        public String Description { get; set; }
        public String ProductGroup { get; set; }
        public String SubGroup { get; set; }
        public String GroupNo { get; set; }
        public String SKU { get; set; }
        public String Branch { get; set; }
        public String SadId { get; set; }
        public double Qty { get; set; }
        public double Value { get; set; }
        public String EffectiveDate { get; set; }

    }

    public class MarkdownH
    {
        public String Date { get; set; }
        public String ID { get; set; }
        public String Description { get; set; }
        public List<MarkdownDetails> Details { get; set; }
    }

    public class MarkdownDetails
    {
        public String ProductGroup { get; set; }
        public String SubGroup { get; set; }
        public String GroupNo { get; set; }
        public String SKU { get; set; }
        public String Branch { get; set; }
        public double Qty { get; set; }
        public double Value { get; set; }

    }


}
