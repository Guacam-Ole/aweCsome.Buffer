using AweCsome.Entities;
using LiteDB;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace AweCsome.Buffer.Interfaces
{
    public interface ILiteDb
    {
        MemoryStream GetAttachmentStreamById(string id, out string filename, out BufferFileMeta meta);
        List<AweCsomeLibraryFile> GetFilesFromDocLib<T>(string folder, bool retrieveData=true) where T : new();
        string AddAttachment(BufferFileMeta meta, Stream fileStream);
        int Insert<T>(T item, string listname);
        LiteDB.LiteCollection<T> GetCollection<T>();
        IEnumerable<string> GetCollectionNames();
        Dictionary<string, Stream> GetAttachmentsFromItem<T>(int id);
        List<string> GetFilenamesFromLibrary<T>(string folder);
        List<string> GetAttachmentNamesFromItem<T>(int id);
        void RemoveAttachment(BufferFileMeta meta);
        LiteCollection<BsonDocument> GetCollection(string name);
        void DeleteTable(string name);
        void ReadAllFromList<T>() where T : new();
        void ReadAllLists(Type baseType, string forbiddenNamespace=null);
        MethodInfo GetMethod<T>(Expression<Action<T>> expr);
        object CallGenericMethodByName(object baseObject, MethodInfo method, Type baseType, string fullyQualifiedName, object[] parameters);
        object CallGenericMethod(object baseObject, MethodInfo method, Type entityType, object[] parameters);
        void EmptyStorage();
    }
}
