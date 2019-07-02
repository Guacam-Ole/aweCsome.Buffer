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
using System.Web.Hosting;

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
        private static object _dbLock = new object();
        private LiteDB.LiteDatabase _database;
        protected IAweCsomeHelpers _helpers;
        protected string _databaseName;
        protected IAweCsomeTable _aweCsomeTable;

        public LiteDb(IAweCsomeHelpers helpers, IAweCsomeTable aweCsomeTable, string databaseName, bool queue = false)
        {
            _databaseName = databaseName;
            _aweCsomeTable = aweCsomeTable;
            if (queue) databaseName += ".QUEUE";
            _database = GetDatabase(databaseName, queue);
            _helpers = helpers;
            RegisterMappers();
        }

        private void RegisterMappers()
        {
            BsonMapper.Global.RegisterType<KeyValuePair<int, string>>(
                serialize: (pair) => $"{pair.Key}[-]{pair.Value}",
                deserialize: (bson) => new KeyValuePair<int, string>(int.Parse(bson.AsString.Split(new string[] { "[-]" }, StringSplitOptions.None)[0]), bson.AsString.Split(new string[] { "[-]" }, StringSplitOptions.None)[1])
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

        protected LiteCollection<T> GetCollection<T>(string name)
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
            foreach (var itemId in GetStorage().FindAll().Select(q => q.Id).ToList())
            {
                GetStorage().Delete(itemId);
            }
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

        public List<string> GetAttachmentNamesFromItem<T>(int id)
        {
            var matches = new List<string>();
            string prefix = GetStringIdFromFilename(new BufferFileMeta { AttachmentType = BufferFileMeta.AttachmentTypes.Attachment, ParentId = id, Listname = _helpers.GetListName<T>() }, true);
            var files = _database.FileStorage.Find(prefix);
            if (matches == null) return null;
            foreach (var file in files)
            {
                if (GetMetadataFromAttachment(file.Metadata).ParentId == id) matches.Add(file.Filename);
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

        public void UpdateMetadata(string id, BsonDocument metadata)
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

        public List<AweCsomeLibraryFile> GetFilesFromDocLib<T>(string folder, bool retrieveContent=true) where T:new()
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

        public T GetMetadataFromAttachment<T>(BsonDocument doc) where T:new()
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

            //meta.SetId(int.Parse(doc[nameof(BufferFileMeta.Id)].AsString));
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
            int minId = collection.Min().AsInt32;
            if (minId > 0) minId = 0;
            minId--;
            _helpers.SetId(item, minId);

            return collection.Insert(item);
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

        private string CreateConnectionString(string databasename)
        {
            string path = null;
            string absolutePath = ConfigurationManager.AppSettings["AweCsomeLiteDbPath"];
            if (absolutePath != null) path = Path.Combine(Environment.ExpandEnvironmentVariables(absolutePath), databasename);
            path = path ?? HostingEnvironment.MapPath(databasename);    // No AbsolutePath
            path = path ?? Environment.CurrentDirectory + "\\" + databasename;// No Web environment
         //   _log.Debug($"DB-Path: {path}");

            return "Filename=" + path;
        }

        private LiteDatabase GetDatabase(string databaseName, bool isQueue)
        {
            if (_dbMode == DbModes.Undefined)
            {
                string dbModeSetting = ConfigurationManager.AppSettings["DbMode"];
                if (dbModeSetting == null)
                {
                    _dbMode = DbModes.File;
                }
                else
                {
                    _dbMode = DbModes.Memory;
                }
            }
            lock (_dbLock)
            {
                if (_dbMode == DbModes.Memory)
                {
                    var oldDb = _memoryDb.FirstOrDefault(q => q.Filename == databaseName);
                    if (oldDb == null) _memoryDb.Add(new MemoryDatabase { Filename = databaseName, IsQueue = isQueue, Database = new LiteDB.LiteDatabase(new MemoryStream()) });
                    return _memoryDb.First(q => q.Filename == databaseName).Database;
                }
                else
                {
                    return new LiteDatabase(CreateConnectionString(databaseName));
                }
            }
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

        public void ReadAllLists(Type baseType)
        {
            foreach (var type in baseType.Assembly.GetTypes())
            {
                var constructor = type.GetConstructor(Type.EmptyTypes);
                if (constructor == null)
                    continue;
                MethodInfo method = GetMethod<LiteDbQueue>(q => q.ReadAllFromList<object>());
                CallGenericMethod(this, method, type, null);
            }
        }
    }
}
