using AweCsome.Attributes.FieldAttributes;
using AweCsome.Buffer.Entities;
using AweCsome.Buffer.Interfaces;
using AweCsome.Entities;
using AweCsome.Interfaces;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;

namespace AweCsome.Buffer
{
    public class AweCsomeTable : IAweCsomeTable, IBufferTable
    {
        private IAweCsomeTable _baseTable;
        private IAweCsomeHelpers _helpers;
        private LiteDb _db;
        public ILiteDbQueue Queue { get; }

        public AweCsomeTable(IAweCsomeTable baseTable, IAweCsomeHelpers helpers, string databasename)
        {
            _baseTable = baseTable;
            _helpers = helpers;
            _db = new LiteDb(helpers, baseTable, databasename);
            Queue = new LiteDbQueue(helpers, baseTable, databasename);
        }

        public void EmptyStorage()
        {
            _db.EmptyStorage();
        }

        public string AddFolderToLibrary<T>(string folder)
        {
            return _baseTable.AddFolderToLibrary<T>(folder);   // NOT buffered
        }

        public void AttachFileToItem<T>(int id, string filename, Stream filestream)
        {
            string liteAttachmentId = _db.AddAttachment(new BufferFileMeta
            {
                AttachmentType = BufferFileMeta.AttachmentTypes.Attachment,
                Filename = filename,
                Listname = _helpers.GetListName<T>(),
                ParentId = id
            }, filestream);

            Queue.AddCommand<T>(new Command
            {
                Action = Command.Actions.AttachFileToItem,
                ItemId = id,
                TableName = _helpers.GetListName<T>(),
                Parameters = new Dictionary<string, object> { { "AttachmentId", liteAttachmentId } }
            });
        }

        public void GetChangesFromAllLists(Type baseType)
        {
            _db.GetChangesFromAllLists(baseType);
        }

        public string AttachFileToLibrary<T>(string folder, string filename, Stream filestream, T entity)
        {
            string liteAttachmentId = _db.AddAttachment(new BufferFileMeta
            {
                AttachmentType = BufferFileMeta.AttachmentTypes.DocLib,
                Filename = filename,
                Listname = _helpers.GetListName<T>(),
                FullyQualifiedName = typeof(T).FullName,
                Folder = folder,
                AdditionalInformation = JsonConvert.SerializeObject(entity, Formatting.Indented)
            }, filestream);

            Queue.AddCommand<T>(new Command
            {
                Action = Command.Actions.AttachFileToLibrary,
                TableName = _helpers.GetListName<T>(),
                Parameters = new Dictionary<string, object> { { "AttachmentId", liteAttachmentId }, { "Folder", folder } }
            });

            return $"{folder}/{filename}";
        }

        public int CountItems<T>()
        {
            return _db.GetCollection<T>().Count();
        }

        public int CountItemsByFieldValue<T>(string fieldname, object value)
        {
            int counter = 0;
            var collection = _db.GetCollection<T>();
            PropertyInfo property = null;

            foreach (var item in collection.FindAll())
            {
                property = property ?? typeof(T).GetProperty(fieldname);
                if (property == null) throw new MissingFieldException($"Field '{fieldname}' cannot be found");
                if (!property.CanRead) throw new FieldAccessException($"Field '{fieldname}' cannot be queried");
                if (property.GetValue(item) == value) counter++;
            }
            return counter;
        }

        public int CountItemsByMultipleFieldValues<T>(Dictionary<string, object> conditions, bool isAndCondition = true)
        {
            int counter = 0;
            var collection = _db.GetCollection<T>();
            var properties = new Dictionary<string, PropertyInfo>();

            if (collection.Count() == 0) return 0;
            var allItems = collection.FindAll();
            foreach (var condition in conditions)
            {
                var property = typeof(T).GetProperty(condition.Key);
                if (property == null) throw new MissingFieldException($"Field '{condition.Key}' cannot be found");
                if (!property.CanRead) throw new FieldAccessException($"Field '{condition.Key}' cannot be queried");
                properties.Add(condition.Key, property);
            }

            foreach (var item in allItems)
            {
                int matchesCount = 0;
                foreach (var property in properties)
                {
                    if (property.Value.GetValue(item) == conditions[property.Key]) matchesCount++;
                }
                if (matchesCount == properties.Count || matchesCount > 0 && !isAndCondition) counter++;
            }
            return counter;
        }

        public int CountItemsByQuery<T>(string query)
        {
            throw new NotImplementedException(); // Cannot be converted to LiteDB
        }

        public void DeleteFileFromItem<T>(int id, string filename)
        {
            _db.RemoveAttachment(new BufferFileMeta
            {
                ParentId = id,
                Listname = _helpers.GetListName<T>(),
                AttachmentType = BufferFileMeta.AttachmentTypes.Attachment,
                Filename = filename
            });
            Queue.AddCommand<object>(new Command
            {
                Action = Command.Actions.RemoveAttachmentFromItem,
                ItemId = id,
                TableName = _helpers.GetListName<T>(),
                Parameters = new Dictionary<string, object> { { "Filename", filename } }
            });
        }

        public void DeleteFilesFromDocumentLibrary<T>(string path, List<string> filenames)
        {
            foreach (var filename in filenames)
            {
                _db.RemoveAttachment(new BufferFileMeta
                {
                    Listname = _helpers.GetListName<T>(),
                    AttachmentType = BufferFileMeta.AttachmentTypes.DocLib,
                    FullyQualifiedName = typeof(T).FullName,
                    Filename = filename,
                    Folder = path
                });
                Queue.AddCommand<object>(new Command
                {
                    Action = Command.Actions.RemoveFileFromLibrary,
                    Parameters = new Dictionary<string, object> { { "Filename", filename }, { "Folder", path } },
                    TableName = _helpers.GetListName<T>()
                });
            }
        }

        public void DeleteFolderFromDocumentLibrary<T>(string path, string folder)
        {
            // not buffered
            _baseTable.DeleteFolderFromDocumentLibrary<T>(path, folder);
        }

        public void DeleteItemById<T>(int id)
        {
            _db.GetCollection<T>().Delete(id);
            Queue.AddCommand<object>(new Command
            {
                Action = Command.Actions.Delete,
                ItemId = id,
                TableName = _helpers.GetListName<T>()
            });
        }

        public void DeleteTable<T>()
        {
            // Not buffered
            _baseTable.DeleteTable<T>();
            BufferState.RemoveTable(_helpers.GetListName<T>());
        }

        public void DeleteTableIfExisting<T>()
        {
            // Not buffered
            _baseTable.DeleteTableIfExisting<T>();
            BufferState.RemoveTable(_helpers.GetListName<T>());
        }

        public void Empty<T>()
        {
            _db.GetCollection<T>().Delete(LiteDB.Query.All());
            Queue.AddCommand<T>(new Command
            {
                Action = Command.Actions.Empty,
                TableName = _helpers.GetListName<T>()
            });
        }

        public string[] GetAvailableChoicesFromField<T>(string propertyname)
        {
            return _baseTable.GetAvailableChoicesFromField<T>(propertyname); // unbuffered
        }

        public int InsertItem<T>(T entity)
        {
            string listname = _helpers.GetListName<T>();
            int itemId = _db.Insert(entity, listname);
            Queue.AddCommand<T>(new Command
            {
                Action = Command.Actions.Insert,
                ItemId = itemId,
                TableName = listname
            });
            return itemId;
        }

        public T Like<T>(int id, int userId) where T : new()
        {
            var item = _db.GetCollection<T>().FindById(id);
            if (item == null) throw new Exceptions.ItemNotFoundException();
            PropertyInfo likesCountProperty = typeof(T).GetProperty("LikesCount");
            PropertyInfo likedByProperty = typeof(T).GetProperty("LikedBy");

            if (likedByProperty == null || likesCountProperty == null) throw new Exceptions.FieldMissingException("Like-Fields missing", "LikedBy,LikesCount");
            var likedBy = (Dictionary<int, string>)likedByProperty.GetValue(item);
            var likesCount = (int)likesCountProperty.GetValue(item);
            if (likedBy.ContainsKey(userId))
            {
                // already liked
                return default(T);
            }

            likedBy.Add(userId, null);
            likesCount++;

            _db.GetCollection<T>().Update(item);
            Queue.AddCommand<T>(new Command
            {
                Action = Command.Actions.Update,
                ItemId = id,
                TableName = _helpers.GetListName<T>()
            });
            return item;
        }

        public List<T> SelectAllItems<T>() where T : new()
        {
            return _db.GetCollection<T>().FindAll().ToList();
        }

        public AweCsomeLibraryFile SelectFileFromLibrary<T>(string foldername, string filename) where T : new()
        {
            var localFiles = _db.GetFilesFromDocLib<T>(foldername);
            if (filename != null) localFiles = localFiles.Where(q => q.Filename == filename).ToList();
            if (localFiles.FirstOrDefault() != null)
            {
                return localFiles.FirstOrDefault();
            }
            var spFile = _baseTable.SelectFileFromLibrary<T>(foldername, filename);
            return spFile;
        }

        public List<string> SelectFileNamesFromItem<T>(int id)
        {
            var localFiles = _db.GetAttachmentNamesFromItem<T>(id);
            var remoteFiles = _baseTable.SelectFileNamesFromItem<T>(id);
            localFiles.ForEach(q => remoteFiles.Add(q));
            return remoteFiles;
        }

        public List<string> SelectFileNamesFromLibrary<T>(string foldername)
        {
            var localFiles = _db.GetFilenamesFromLibrary<T>(foldername);
            var remoteFiles = _baseTable.SelectFileNamesFromLibrary<T>(foldername);
            localFiles.ForEach(q => remoteFiles.Add(q));
            return remoteFiles;
        }

        public List<AweCsomeLibraryFile> SelectFilesFromLibrary<T>(string foldername, bool retrieveContent = true) where T : new()
        {
            var localFiles = _db.GetFilesFromDocLib<T>(foldername);
            var spFiles = _baseTable.SelectFilesFromLibrary<T>(foldername, retrieveContent);
            localFiles.ForEach(q => spFiles.Add(q));
            return spFiles;
        }

        public T SelectItemById<T>(int id) where T : new()
        {
            var item = _db.GetCollection<T>().FindById(id);
            if (item == null && id < 0)
            {
                var changes = _db.GetCollection<AweCsomeIdChange>();
                var idChange = changes.FindOne(q => q.OldId == id && q.ListName == typeof(T).Name);
                if (idChange != null) item = _db.GetCollection<T>().FindById(idChange.NewId);
            }
            return item;
        }

        public List<T> SelectItemsByFieldValue<T>(string fieldname, object value) where T : new()
        {
            var matches = new List<T>();
            var collection = _db.GetCollection<T>();
            PropertyInfo property = null;

            foreach (var item in collection.FindAll())
            {
                property = property ?? typeof(T).GetProperty(fieldname);
                if (property == null) throw new MissingFieldException($"Field '{fieldname}' cannot be found");
                if (!property.CanRead) throw new FieldAccessException($"Field '{fieldname}' cannot be queried");
                var propertyValue = property.GetValue(item);
                var lookupAttribute = property.GetCustomAttribute<LookupAttribute>();
                var peopleAttribute = property.GetCustomAttribute<UserAttribute>();
                if (property.PropertyType == typeof(KeyValuePair<int, string>)) propertyValue = ((KeyValuePair<int, string>)propertyValue).Key;
                if (property.PropertyType == typeof(Dictionary<int, string>)) propertyValue = ((Dictionary<int, string>)propertyValue).Keys.ToArray();
                if (propertyValue.Equals(value))
                {
                    matches.Add(item);
                    continue;
                }
                if (propertyValue is int[] && value is int)
                {
                    if (((int[])propertyValue).Contains((int)value))
                    {
                        matches.Add(item);
                        continue;
                    }
                }
            }
            return matches;
        }

        public List<T> SelectItemsByMultipleFieldValues<T>(Dictionary<string, object> conditions, bool isAndCondition = true) where T : new()
        {
            var matches = new List<T>();
            var collection = _db.GetCollection<T>();
            var properties = new Dictionary<string, PropertyInfo>();

            if (collection.Count() == 0) return new List<T>();
            var allItems = collection.FindAll();
            foreach (var condition in conditions)
            {
                var property = typeof(T).GetProperty(condition.Key);
                if (property == null) throw new MissingFieldException($"Field '{condition.Key}' cannot be found");
                if (!property.CanRead) throw new FieldAccessException($"Field '{condition.Key}' cannot be queried");
                properties.Add(condition.Key, property);
            }

            foreach (var item in allItems)
            {
                int matchesCount = 0;
                foreach (var property in properties)
                {
                    if (property.Value.GetValue(item) == conditions[property.Key]) matchesCount++;
                }
                if (matchesCount == properties.Count || matchesCount > 0 && !isAndCondition) matches.Add(item);
            }
            return matches;
        }

        public List<T> SelectItemsByQuery<T>(string query) where T : new()
        {
            throw new NotImplementedException();    // Does not work with buffer
        }

        public List<T> SelectItemsByTitle<T>(string title) where T : new()
        {
            return SelectItemsByFieldValue<T>("Title", title);
        }

        public T Unlike<T>(int id, int userId) where T : new()
        {
            var item = _db.GetCollection<T>().FindById(id);
            if (item == null) throw new Exceptions.ItemNotFoundException();
            PropertyInfo likesCountProperty = typeof(T).GetProperty("LikesCount");
            PropertyInfo likedByProperty = typeof(T).GetProperty("LikedBy");

            if (likedByProperty == null || likesCountProperty == null) throw new Exceptions.FieldMissingException("Like-Fields missing", "LikedBy,LikesCount");
            var likedBy = (Dictionary<int, string>)likedByProperty.GetValue(item);
            var likesCount = (int)likesCountProperty.GetValue(item);
            if (!likedBy.ContainsKey(userId))
            {
                // didn't like 
                return default(T);
            }

            likedBy = likedBy.Where(q => q.Key != userId).ToDictionary(q => q.Key, q => q.Value);
            likesCount--;

            _db.GetCollection<T>().Update(item);
            Queue.AddCommand<T>(new Command
            {
                Action = Command.Actions.Update,
                ItemId = id,
                TableName = _helpers.GetListName<T>()
            });
            return item;
        }

        public void UpdateItem<T>(T entity)
        {
            _db.GetCollection<T>().Update(entity);
            Queue.AddCommand<T>(new Command
            {
                Action = Command.Actions.Update,
                ItemId = _helpers.GetId(entity),
                TableName = _helpers.GetListName<T>()
            });
        }

        public Guid CreateTable<T>()
        {
            Guid newId = _baseTable.CreateTable<T>();
            BufferState.AddTable(_helpers.GetListName<T>(), newId);
            return newId;
        }

        public Dictionary<string, Stream> SelectFilesFromItem<T>(int id, string filename = null)
        {
            var localFiles = _db.GetAttachmentsFromItem<T>(id);
            if (filename != null) localFiles = localFiles.Where(q => q.Key == filename).ToDictionary(q => q.Key, q => q.Value);
            var spFiles = _baseTable.SelectFilesFromItem<T>(id, filename) ?? new Dictionary<string, Stream>();
            localFiles.ToList().ForEach(q => spFiles.Add(q.Key, q.Value));
            return spFiles;
        }

        public bool HasChangesSince<T>(DateTime compareDate) where T : new()
        {
            throw new NotImplementedException();
        }

        public List<KeyValuePair<AweCsomeListUpdate, T>> ModifiedItemsSince<T>(DateTime compareDate) where T : new()
        {
            throw new NotImplementedException();
        }

        public bool Exists<T>()
        {
            return _db.GetCollectionNames().Contains(typeof(T).Name);
        }

        public void ReadAllLists(Type baseType)
        {
            _db.ReadAllLists(baseType);
        }

        public void ReadAllFromList<T>() where T : new()
        {
            _db.ReadAllFromList<T>();
        }

        public void ReadAllFromList(Type entityType)
        {
            _db.ReadAllFromList(entityType);
        }
    }
}
