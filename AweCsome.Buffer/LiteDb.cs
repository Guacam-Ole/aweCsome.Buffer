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
            return dirtyName.Replace("/", "").Replace("\\", "").Replace("-", "_").Replace(" ", "");
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

        protected LiteDB.LiteCollection<T> GetCollection<T>(string name)
        {

            name = name ?? typeof(T).Name;
            return _database.GetCollection<T>(name);
        }

        public LiteCollection<BsonDocument> GetCollection(string name)
        {
            return _database.GetCollection(name);
        }

        private LiteDB.LiteStorage GetStorage()
        {
            return _database.FileStorage;
        }

        public void RemoveAttachment(BufferFileMeta meta)
        {
            var existingFile = _database.FileStorage.Find(GetStringIdFromFilename(meta,true)).FirstOrDefault(q=>q.Filename==meta.Filename);
            if (existingFile == null) return;
            _database.FileStorage.Delete(existingFile.Id);
        }

        public void UpdateFileMeta(BufferFileMeta oldMeta, BufferFileMeta newMeta )
        {
            string id = GetStringIdFromFilename(oldMeta);
            var existingFile = _database.FileStorage.Find(id).FirstOrDefault(q => GetMetadataFromAttachment(q.Metadata).ParentId == oldMeta.ParentId);
            if (existingFile==null)
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

        public List<AweCsomeLibraryFile> GetFilesFromDocLib<T>(string folder)
        {
            var matches = new List<AweCsomeLibraryFile>();

            string prefix = GetStringIdFromFilename(new BufferFileMeta { AttachmentType = BufferFileMeta.AttachmentTypes.DocLib, Folder = folder, Listname = _helpers.GetListName<T>() }, true);

            var files = _database.FileStorage.Find(prefix);
            foreach (var file in files)
            {
                MemoryStream fileStream = new MemoryStream((int)file.Length);
                file.CopyTo(fileStream);
                matches.Add(new AweCsomeLibraryFile
                {
                    Stream = fileStream,
                    Filename = file.Filename,
                    Entity = file.Metadata
                });
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
                if (property.CanWrite && doc.ContainsKey(property.Name)) {
                    var converter = TypeDescriptor.GetConverter(property.PropertyType);
                    property.SetValue(meta, converter.ConvertFromString(doc[property.Name]));
                } 
            }
                //AttachmentType = (BufferFileMeta.AttachmentTypes)Enum.Parse(typeof(BufferFileMeta.AttachmentTypes), doc[nameof(BufferFileMeta.AttachmentType)]),
                //Filename = doc[nameof(BufferFileMeta.Filename)],
                //Folder = doc[nameof(BufferFileMeta.Folder)],
                //Listname = doc[nameof(BufferFileMeta.Listname)],
                //ParentId = doc[nameof(BufferFileMeta.ParentId)],
                //AdditionalInformation = doc[nameof(BufferFileMeta.AdditionalInformation)]

            meta.SetId(int.Parse(doc[nameof(BufferFileMeta.Id)].AsString));
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

        public int Insert<T>(T item, string listname)
        {

            var collection = GetCollection<T>(listname);
            collection.EnsureIndex("Id");
            int minId = collection.Min().AsInt32;
            if (minId > 0) minId = 0;
            minId--;
            _helpers.SetId<T>(item, minId);

            return collection.Insert(item);
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
            string localPath = HostingEnvironment.MapPath(databasename);
            if (localPath == null)
            {
                // No Web environment
                localPath = System.Environment.CurrentDirectory + "\\" + databasename;
            }
            return "Filename=" + localPath;
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

        public void ReadAllFromList<T>() where T : new()
        {
            if (!_aweCsomeTable.Exists<T>()) return;
            var spItems = _aweCsomeTable.SelectAllItems<T>();
            _log.Debug($"Replacing Data in localDB for {typeof(T).Name} ({spItems.Count} items)");

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
