using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AweCsome.Buffer
{
    public static class Configuration
    {
        private static long GetSizeFromConfig(string configName)
        {
            string config = ConfigurationManager.AppSettings[configName];
            if (config == null) return long.MaxValue;   // No Limit
            var parts = config.Split(' ');
            if (parts.Length > 2) return -1; // Unexpected value
            if (parts.Length == 0)
            {
                if (!long.TryParse(config, out long justTheNumber)) return -2; // NaN
                return justTheNumber;
            }
            if (!long.TryParse(parts[0], out long configNumber)) return -2; // NaN
            switch (parts[1].ToLower().Trim())
            {
                case "b":
                case "byte":
                case "bytes":
                    return configNumber;

                case "kb":
                case "kbyte":
                case "kbytes":
                    return configNumber * 1024;

                case "mb":
                case "mbyte":
                case "mbytes":
                    return configNumber * 1024 * 1024;

                case "gb":
                case "gbyte":
                case "gbytes":
                    return configNumber * 1024 * 1024 * 1024;

                default:
                    return -1; // unexpected value
            }
        }

        public static long MaxLocalDocLibSize
        {
            get
            {
                return GetSizeFromConfig("AweCsome.Limit.DocLib");
            }
        }

        public static long MaxLocalAttachmentSize
        {
            get
            {
                return GetSizeFromConfig("AweCsome.Limit.Attachment");
            }
        }
    }
}
