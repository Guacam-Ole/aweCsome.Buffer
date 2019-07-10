using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System;
using System.Collections.Generic;

namespace AweCsome.Buffer.Entities
{
    public class Command
    {
        public enum Actions { DeleteTable, CreateTable, Insert, Update, Delete, Empty, AttachFileToItem, RemoveAttachmentFromItem, AttachFileToLibrary, RemoveFileFromLibrary, Like, Unlike }

        public enum States { Pending, Failed, Succeeded, Delayed, Disabled }

        [JsonConverter(typeof(StringEnumConverter))]
        public Actions Action { get; set; }
        public Dictionary<string, object> Parameters { get; set; }
        public int Id { get; set; }
        public string TableName { get; set; }
        [JsonConverter(typeof(StringEnumConverter))]
        public States State { get; set; } = States.Pending;
        public int? ItemId { get; set; }
        public DateTime Created { get; } = DateTime.Now;
        public string FullyQualifiedName { get; set; }
        public int Priority { get; set; } = 1;

        public override string ToString()
        {
            return $"{Id} [Action:{Action}, Table:{TableName}, State: {State}, ItemId:{ItemId}, Created: {Created}, parametercount: {Parameters?.Count}]";
        }
    }
}
