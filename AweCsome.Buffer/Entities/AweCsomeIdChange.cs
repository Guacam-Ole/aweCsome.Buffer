using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AweCsome.Buffer.Entities
{
    public class AweCsomeIdChange
    {
        public string ListName { get; set; }
        public DateTime Changed { get; set; } = DateTime.Now;
        public int OldId { get; set; }
        public int NewId { get; set; }
    }
}
