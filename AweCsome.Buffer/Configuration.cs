using System.Collections.Specialized;
using System.Configuration;
using System.Linq;

namespace AweCsome.Buffer
{
    public static class Configuration
    {
        private const string ConfigSectionName = "awecsome";

        private static long GetSizeFromConfig(string configName)
        {
            var section = (NameValueCollection)ConfigurationManager.GetSection(ConfigSectionName);
            if (section == null) return long.MaxValue;
            if (!section.AllKeys.Contains(configName)) return long.MaxValue;

            string config = section[configName];
            if (string.IsNullOrWhiteSpace(config)) return long.MaxValue;   // No Limit

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
                return GetSizeFromConfig("BufferLimitDocLib");
            }
        }

        public static long MaxLocalAttachmentSize
        {
            get
            {
                return GetSizeFromConfig("BufferLimitAttachment");
            }
        }
    }
}