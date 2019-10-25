using AweCsome.Buffer.Interfaces;
using AweCsome.Entities;
using AweCsome.Interfaces;

using LiteDB;

using log4net;

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.RegularExpressions;

namespace AweCsome.Buffer
{
    public class LiteDb : ILiteDb
    {
        private readonly ILog _log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private const string PrefixAttachment = "UploadAttachment_";
        private const string PrefixFile = "UploadFile_";

        private enum DbModes { Memory, File, Undefined };

        private DbModes _dbMode = DbModes.Undefined;
        private static List<MemoryDatabase> _memoryDb = new List<MemoryDatabase>();
        private static readonly object _dbLock = new object();
        protected IAweCsomeHelpers _helpers;
        protected string _connectionString;
        protected IAweCsomeTable _aweCsomeTable;
        private LiteDatabase _database;

   

        public LiteDb(IAweCsomeHelpers helpers, IAweCsomeTable aweCsomeTable, string connectionString, bool queue = false)
        {
            _connectionString = connectionString;
            _aweCsomeTable = aweCsomeTable;
            _helpers = helpers;
            RegisterMappers();
            _database = GetDatabase(connectionString, queue);
        }

        private string SerializePair<T, U>(KeyValuePair<T, U> pair)
        {
            try
            {
                return $"{pair.Key}[-]{pair.Value}";
            }
            catch (Exception)
            {
                _log.Error("Cannot serialize pair");
                throw;
            }
        }

        private KeyValuePair<T, U> DeserializePair<T, U>(string serialized)
        {
            try
            {
                var splitted = serialized.Split(new string[] { "[-]" }, StringSplitOptions.None);
                T key = (T)Convert.ChangeType(splitted[0], typeof(T));
                U value = (U)Convert.ChangeType(splitted[1], typeof(U));
                return new KeyValuePair<T, U>(key, value);
            }
            catch (Exception)
            {
                _log.Error($"Cannot deserialize pair '{serialized}'");
                throw;
            }
        }

        private string SerializeList<T, U>(List<KeyValuePair<T, U>> list)
        {
            return string.Join("~\n", list.Select(q => SerializePair(q)));
        }

        private List<KeyValuePair<T, U>> DeserializeList<T, U>(string serialized)
        {
            var list = new List<KeyValuePair<T, U>>();
            var elements = serialized.Split(new string[] { "~\n" }, StringSplitOptions.RemoveEmptyEntries);
            foreach (string element in elements)
            {

                list.Add(DeserializePair<T, U>(element));
            }
            return list;
        }

        private string SerializeDictionary<T, U>(Dictionary<T, U> list)
        {
            return string.Join("~\n", list.Select(q => SerializePair(q)));
        }

        private Dictionary<T, U> DeserializeDictionary<T, U>(string serialized)
        {
            return DeserializeList<T, U>(serialized).ToDictionary(q => q.Key, q => q.Value);
        }

        private void RegisterMappers()
        {
            BsonMapper.Global.RegisterType(
                serialize: (pair) => SerializePair(pair),
                deserialize: (bson) => DeserializePair<int, string>(bson.AsString)
                );
            BsonMapper.Global.RegisterType(
                serialize: (pair) => SerializePair(pair),
                deserialize: (bson) => DeserializePair<long, string>(bson.AsString)
                );
            BsonMapper.Global.RegisterType(
                serialize: (list) => SerializeList(list),
                deserialize: (bson) => DeserializeList<long, string>(bson.AsString)
                );
            BsonMapper.Global.RegisterType(
                serialize: (list) => SerializeList(list),
                deserialize: (bson) => DeserializeList<int, string>(bson.AsString)
                );
            BsonMapper.Global.RegisterType(
                serialize: (list) => SerializeDictionary(list),
                deserialize: (bson) => DeserializeDictionary<long, string>(bson.AsString)
                );
            BsonMapper.Global.RegisterType(
                serialize: (list) => SerializeDictionary(list),
                deserialize: (bson) => DeserializeDictionary<int, string>(bson.AsString)
                );
        }

        public void DeleteTable(string name)
        {
                _database.DropCollection(name);
        }

        private string CleanUpLiteDbId(string dirtyName)
        {
            dirtyName = dirtyName.Replace("-", "_");
            Regex rx = new Regex("[^a-zA-Z0-9_]");
            return rx.Replace(dirtyName, "");
        }

        private string GetStringIdFromFilename(BufferFileMeta meta, bool pathOnly = false)
        {
            string stringId = $"{meta.AttachmentType}_{meta.Listname}_{meta.Folder}";
            if (!pathOnly) stringId += $"_{meta.ParentId}_{meta.Filename}";
            return CleanUpLiteDbId(stringId);
        }

        protected void DropCollection<T>(string name)
        {
            name = name ?? typeof(T).Name;
                _database.DropCollection(name);
        }

        protected LiteCollection<T> GetCollection<T>(string name, bool useLocal=false)
        {
            name = name ?? typeof(T).Name;
                return _database.GetCollection<T>(name);
        }

        public LiteCollection<BsonDocument> GetCollection(string name)
        {
                return _database.GetCollection(name);
        }

        private LiteStorage GetStorage()
        {
                return _database.FileStorage;
        }

        public void EmptyStorage()
        {
            var storageids = GetStorage()?.FindAll()?.Select(q => q.Id)?.ToList();
            if (storageids == null) return;

            foreach (var itemId in storageids) GetStorage().Delete(itemId);
        }

        public void RemoveAttachment(BufferFileMeta meta)
        {
                var existingFile = _database.FileStorage.Find(GetStringIdFromFilename(meta, true)).FirstOrDefault(q => q.Filename == meta.Filename);
                if (existingFile == null) return;
                _database.FileStorage.Delete(existingFile.Id);
        }

        public void UpdateFileMeta(BufferFileMeta oldMeta, BufferFileMeta newMeta)
        {
                string id = GetStringIdFromFilename(oldMeta);
                var existingFile = _database.FileStorage.Find(id).FirstOrDefault(q => GetMetadataFromAttachment(q.Metadata).ParentId == oldMeta.ParentId);
                if (existingFile == null)
                {
                    _log.Warn($"Cannot change meta for {id}. File cannot be found");
                    return;
                }
                existingFile.Metadata = GetMetadataFromAttachment(newMeta);
        }

        public List<KeyValuePair<DateTime, string>> GetAttachmentNamesFromItem<T>(int id)
        {
                var matches = new List<KeyValuePair<DateTime, string>>();
                string prefix = GetStringIdFromFilename(new BufferFileMeta { AttachmentType = BufferFileMeta.AttachmentTypes.Attachment, ParentId = id, Listname = _helpers.GetListName<T>() }, true);
                var files = _database.FileStorage.Find(prefix);
                if (matches == null) return null;
                foreach (var file in files)
                {
                    if (GetMetadataFromAttachment(file.Metadata).ParentId == id) matches.Add(new KeyValuePair<DateTime, string>(file.UploadDate, file.Filename));
                }
                return matches;
        }

        public List<string> GetFilenamesFromLibrary<T>(string folder)
        {
                var matches = new List<string>();

                string prefix = GetStringIdFromFilename(new BufferFileMeta { AttachmentType = BufferFileMeta.AttachmentTypes.DocLib, Folder = folder, Listname = _helpers.GetListName<T>() }, true);

                var files = _database.FileStorage.Find(prefix);
                foreach (var file in files)
                {
                    matches.Add(file.Filename);
                }
                return matches;
        }

        public void UpdateMetadata( string id, BsonDocument metadata)
        {
            _database.FileStorage.SetMetadata(id, metadata);
        }

        public Dictionary<string, Stream> GetAttachmentsFromItem<T>(int id)
        {
                var matches = new Dictionary<string, Stream>();
                string prefix = GetStringIdFromFilename(new BufferFileMeta { AttachmentType = BufferFileMeta.AttachmentTypes.Attachment, ParentId = id, Listname = _helpers.GetListName<T>() }, true);
                var files = _database.FileStorage.Find(prefix);
                if (matches == null) return null;
                foreach (var file in files)
                {
                    if (GetMetadataFromAttachment(file.Metadata).ParentId != id) continue;
                    MemoryStream fileStream = new MemoryStream((int)file.Length);
                    file.CopyTo(fileStream);
                    matches.Add(file.Filename, fileStream);
                }
                return matches;
        }

        public MemoryStream GetAttachmentStreamById(string id, out string filename, out BufferFileMeta meta)
        {
                var fileInfo = _database.FileStorage.FindById(id);
                filename = fileInfo.Filename;
                MemoryStream fileStream = new MemoryStream((int)fileInfo.Length);
                fileInfo.CopyTo(fileStream);
                meta = GetMetadataFromAttachment(fileInfo.Metadata);
                return fileStream;
        }

        public IEnumerable<LiteFileInfo> GetAllFiles()
        {
            return _database.FileStorage.FindAll();
        }

        public List<AweCsomeLibraryFile> GetFilesFromDocLib<T>(string folder, bool retrieveContent = true) where T : new()
        {
                var matches = new List<AweCsomeLibraryFile>();

                string prefix = GetStringIdFromFilename(new BufferFileMeta { AttachmentType = BufferFileMeta.AttachmentTypes.DocLib, Folder = folder, Listname = _helpers.GetListName<T>() }, true);

                var files = _database.FileStorage.Find(prefix);
                foreach (var file in files)
                {
                    var meta = GetMetadataFromAttachment(file.Metadata);

                    var libFile = new AweCsomeLibraryFile
                    {
                        Filename = file.Filename,
                        Entity = meta
                    };

                    if (retrieveContent)
                    {
                        MemoryStream fileStream = new MemoryStream((int)file.Length);
                        file.CopyTo(fileStream);
                        libFile.Stream = fileStream;
                    }

                    matches.Add(libFile);
                }
                return matches;
        }

        public BsonDocument GetMetadataFromAttachment(BufferFileMeta meta)
        {
            var doc = new BsonDocument();
            foreach (var property in typeof(BufferFileMeta).GetProperties())
            {
                //  doc.Set(property.Name,  property.GetValue(meta));
                if (property.CanRead) doc[property.Name] = property.GetValue(meta)?.ToString();
            }
            return doc;
        }

        public BufferFileMeta GetMetadataFromAttachment(LiteDB.BsonDocument doc)
        {
            var meta = new BufferFileMeta();
            foreach (var property in typeof(BufferFileMeta).GetProperties())
            {
                if (property.CanWrite && doc.ContainsKey(property.Name))
                {
                    var converter = TypeDescriptor.GetConverter(property.PropertyType);
                    property.SetValue(meta, converter.ConvertFromString(doc[property.Name]));
                }
            }

            meta.SetId(int.Parse(doc[nameof(BufferFileMeta.Id)].AsString));
            return meta;
        }

        public T GetMetadataFromAttachment<T>(BsonDocument doc) where T : new()
        {
            var meta = new T();
            foreach (var property in typeof(T).GetProperties())
            {
                if (property.CanWrite && doc.ContainsKey(property.Name))
                {
                    var converter = TypeDescriptor.GetConverter(property.PropertyType);
                    property.SetValue(meta, converter.ConvertFromString(doc[property.Name]));
                }
            }
            return meta;
        }

        public string AddAttachment(BufferFileMeta meta, Stream fileStream)
        {
                int calculatedIndex = 0;
                string prefix = GetStringIdFromFilename(meta, true);
                var existingFiles = _database.FileStorage.Find(prefix);
                if (existingFiles.Count() > 0)
                {
                    calculatedIndex = existingFiles.Min(q => int.Parse(q.Metadata["Id"].AsString ?? "0"));
                    if (calculatedIndex > 0) calculatedIndex = 0;
                }
                calculatedIndex--;
                meta.SetId(calculatedIndex);
                var uploadedFile = _database.FileStorage.Upload(GetStringIdFromFilename(meta), meta.Filename, fileStream);
                _database.FileStorage.SetMetadata(uploadedFile.Id, GetMetadataFromAttachment(meta));
                return uploadedFile.Id;
        }

        public void Delete<T>(int id, string listname)
        {
            var collection = GetCollection<T>(listname);
            collection.Delete(id);
        }

        public int Insert<T>(T item, string listname)
        {
            var collection = GetCollection<T>(listname);
            collection.EnsureIndex("Id");
            collection.EnsureIndex(nameof(Entities.AweCsomeListItemBuffered.BufferId));

            int minIdBuffer = collection.Min(nameof(Entities.AweCsomeListItemBuffered.BufferId)).AsInt32;
            int minId = collection.Min().AsInt32;
            if (minId < minIdBuffer) minIdBuffer = minId;
            if (minIdBuffer > 0) minIdBuffer = 0;
            minIdBuffer--;
            _helpers.SetId(item, minIdBuffer);
            SetBufferId(item, minIdBuffer);

            return collection.Insert(item);
        }

        private void SetBufferId<T>(T item, int id)
        {
            var property = typeof(T).GetProperty(nameof(Entities.AweCsomeListItemBuffered.BufferId));
            if (property == null) return;
            property.SetValue(item, id);
        }

        public void Update<T>(int id, T item, string listname)
        {
            var collection = GetCollection<T>(listname);
            var oldItem = collection.FindById(id);
            collection.Update(id, item);
        }

        public LiteCollection<T> GetCollection<T>()
        {
                return _database.GetCollection<T>();
        }

        public IEnumerable<string> GetCollectionNames()
        {
                return _database.GetCollectionNames();
        }

        private string AddPasswordToDbName(string dbName, string cleanedUrl)
        {
            string setting_name = $"password_{cleanedUrl}";
            string password = ConfigurationManager.AppSettings[setting_name];
            if (password == null) return "Filename=" + dbName;

            return $"Filename=\"{dbName}\"; Password=\"{password}\"";
        }

        private LiteDatabase GetDatabase(string connectionString, bool isQueue)
        {
            LiteDatabase database = null;
            try
            {
                if (_dbMode == DbModes.Undefined)
                {
                    string dbModeSetting = ConfigurationManager.AppSettings["DbMode"];
                    _dbMode = dbModeSetting == null ? DbModes.File : DbModes.Memory;
                }

                //_log.Debug($"Retrieving Database for '{connectionString}' ");
                if (_dbMode == DbModes.Memory)
                {
                    _log.Debug("FROM MEMORY");
                    var oldDb = _memoryDb.FirstOrDefault(q => q.Filename == connectionString);
                    if (oldDb == null) _memoryDb.Add(new MemoryDatabase { Filename = connectionString, IsQueue = isQueue, Database = new LiteDatabase(new MemoryStream()) });
                    return _memoryDb.First(q => q.Filename == connectionString).Database;
                }
                database = new LiteDatabase(connectionString);
                //_log.Debug("Database retrieved");
            }
            catch (Exception ex)
            {
                _log.Error("Error retrieving Database", ex);
                throw;
            }
            if (database==null)
            {
                _log.Warn($"Database is null (but shouldn't) '{connectionString}' {isQueue} ");
            }
            return database;

        }

        public object CallGenericMethodByName(object baseObject, MethodInfo method, Type baseType, string fullyQualifiedName, object[] parameters)
        {
            return CallGenericMethod(baseObject, method, baseType.Assembly.GetType(fullyQualifiedName, false, true), parameters);
        }

        public object CallGenericMethod(object baseObject, MethodInfo method, Type entityType, object[] parameters)
        {
            MethodInfo genericMethod = method.MakeGenericMethod(entityType);
            var paams = genericMethod.GetParameters();
            try
            {
                var retVal = genericMethod.Invoke(baseObject, parameters);
                return retVal;
            }
            catch (Exception ex)
            {
                if (ex.InnerException != null && ex.InnerException.GetType() != typeof(Exception)) throw ex.InnerException;
                throw;
            }
        }

        public void ReadAllFromList(Type entityType)
        {
            MethodInfo method = GetMethod<LiteDbQueue>(q => q.ReadAllFromList<object>());
            CallGenericMethod(this, method, entityType, null);
        }

        public void GetChangesFromList<T>(DateTime compareDate) where T : new()
        {
            foreach (var modification in _aweCsomeTable.ModifiedItemsSince<T>(compareDate))
            {
                switch (modification.Key.ChangeType)
                {
                    case AweCsomeListUpdate.ChangeTypes.Add:
                        Insert(modification.Value, _helpers.GetListName<T>());
                        break;

                    case AweCsomeListUpdate.ChangeTypes.Delete:
                        Delete<T>(modification.Key.Id, _helpers.GetListName<T>());
                        break;

                    case AweCsomeListUpdate.ChangeTypes.Update:
                        Update(modification.Key.Id, modification.Value, _helpers.GetListName<T>());
                        break;
                }
            }
        }

        public void GetChangesFromAllLists(Type baseType)
        {
            DateTime compareDate = DateTime.Now; // TODO: From the settings
            foreach (var type in baseType.Assembly.GetTypes())
            {
                string fullyQualifiedName = type.FullName;
                MethodInfo method = GetMethod<LiteDbQueue>(q => q.GetChangesFromList<object>(compareDate));
                CallGenericMethodByName(this, method, baseType, fullyQualifiedName, new object[] { compareDate });
            }
        }

        public void ReadAllFromList<T>() where T : new()
        {
            if (!_aweCsomeTable.Exists<T>()) return;
            var spItems = _aweCsomeTable.SelectAllItems<T>();
            _log.Debug($"Replacing Data in LiteDB for {typeof(T).Name} ({spItems.Count} items)");

            DropCollection<T>(null);
            if (spItems.Count == 0) return;
            if (typeof(T).GetProperty("Id") == null)
            {
                _log.Warn($"Collection {typeof(T).Name} has no ID-Field. Cannot insert");
                return;
            }
            var targetCollection = GetCollection<T>();
            foreach (var item in spItems)
            {
                targetCollection.Insert(item);
            }
        }

        public MethodInfo GetMethod<T>(Expression<Action<T>> expr)
        {
            return ((MethodCallExpression)expr.Body)
                .Method
                .GetGenericMethodDefinition();
        }

        public void ReadAllLists(Type baseType, string forbiddenNamespace = null)
        {
            foreach (var type in baseType.Assembly.GetTypes())
            {
                var constructor = type.GetConstructor(Type.EmptyTypes);
                if (constructor == null)
                    continue;
                if (forbiddenNamespace != null && type.Namespace.Contains(forbiddenNamespace)) continue;
                MethodInfo method = GetMethod<LiteDbQueue>(q => q.ReadAllFromList<object>());
                CallGenericMethod(this, method, type, null);
            }
        }
    }
}