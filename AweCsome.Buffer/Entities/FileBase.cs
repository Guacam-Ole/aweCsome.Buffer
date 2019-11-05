using LiteDB;

namespace AweCsome.Buffer.Entities
{
    public class FileBase

    {
        [BsonId]
        public string FileId { get; set; }

        public enum AllowedStates { Upload, Local, Server }

        public string List { get; set; }
        public AllowedStates State { get; set; }
        public string Filename { get; set; }

        public int? ReferenceId { get; set; }
    }
}