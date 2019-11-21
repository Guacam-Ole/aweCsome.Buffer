using AweCsome.Attributes.FieldAttributes;
using AweCsome.Buffer.Entities;
using AweCsome.Buffer.Interfaces;
using AweCsome.Entities;
using AweCsome.Interfaces;

using log4net;

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
        private readonly ILog _log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);
        private IAweCsomeTable _baseTable;
        private IAweCsomeHelpers _helpers;
        private LiteDb _db;
        public ILiteDbQueue Queue { get; }
        private Dictionary<Guid, DateTime> _measurements = new Dictionary<Guid, DateTime>();




        private Guid StartMeasurement()
        {
            var guid = Guid.NewGuid();
            _measurements.Add(guid, DateTime.Now);
            return guid;
        }

        private void StopMeasurement(Guid guid, string message = null)
        {
            if (!_measurements.ContainsKey(guid)) return;
            var started = _measurements[guid];
            var totalSeconds = DateTime.Now.Subtract(started).TotalSeconds;
            if (totalSeconds > 1) _log.Debug($"[MEASUREMENT] Total seconds: {totalSeconds} '{message}'");
            _measurements.Remove(guid);
        }

        public AweCsomeTable(IAweCsomeTable baseTable, IAweCsomeHelpers helpers, string connectionString)
        {
            _baseTable = baseTable;
            _helpers = helpers;
            _db = new LiteDb(helpers, baseTable, connectionString);
            Queue = new LiteDbQueue(helpers, baseTable, connectionString);
        }

        public void EmptyStorage()
        {
            var guid = StartMeasurement();
            _db.EmptyStorage();
            StopMeasurement(guid, "EmptyStorage (LiteDB)");
        }

        public string AddFolderToLibrary<T>(string folder)
        {
            var guid = StartMeasurement();
            var result = _baseTable.AddFolderToLibrary<T>(folder);   // NOT buffered
            StopMeasurement(guid, "AddFolderToLinbrary (SharePoint)");
            return result;
        }

        public void AttachFileToItem<T>(int id, string filename, Stream filestream)
        {
            AttachFileToItem<T>(id, filename, filestream, false, true);
        }

        private BufferFileMeta GetMetaForFile<T>(BufferFileMeta.AttachmentTypes attachmentType, string filename, int parentId)
        {
            return new BufferFileMeta
            {
                AttachmentType = attachmentType,
                Filename = filename,
                Listname = _helpers.GetListName<T>(),
                ParentId = parentId
            };
        }

        private void AttachFileToItem<T>(int id, string filename, Stream filestream, bool storeNameOnly, bool addToQueue)
        {
            if (filestream != null) filestream.Seek(0, SeekOrigin.Begin);
            var guid = StartMeasurement();
            var state = FileBase.AllowedStates.Server;
            if (!storeNameOnly)
            {
                state = addToQueue ? FileBase.AllowedStates.Upload : FileBase.AllowedStates.Local;
            }

            string liteAttachmentId = _db.AddAttachment(
                GetMetaForFile<T>(BufferFileMeta.AttachmentTypes.Attachment, filename, id),
                filestream, state);
            if (addToQueue && !storeNameOnly)
            {
                Queue.AddCommand<T>(new Command
                {
                    Action = Command.Actions.AttachFileToItem,
                    ItemId = id,
                    TableName = _helpers.GetListName<T>(),
                    Parameters = new Dictionary<string, object> { { "AttachmentId", liteAttachmentId } }
                });
            }
            StopMeasurement(guid, "AttachFileToItem (LiteDB)");
        }

        public void GetChangesFromAllLists(Type baseType)
        {
            var guid = StartMeasurement();
            _db.GetChangesFromAllLists(baseType);
            StopMeasurement(guid, "GetChangesFromAllLists (LiteDB)");
        }

        public string AttachFileToLibrary<T>(string folder, string filename, Stream filestream, T entity)
        {
            return AttachFileToLibrary<T>(folder, filename, filestream, entity, false, true);
        }

        private string AttachFileToLibrary<T>(string folder, string filename, Stream filestream, T entity, bool nameOnly, bool addToQueue)
        {
            var updateState = FileBase.AllowedStates.Server;
            if (!nameOnly)
            {
                updateState = addToQueue ? FileBase.AllowedStates.Upload : FileBase.AllowedStates.Local;
            }

            if (filestream != null) filestream.Seek(0, SeekOrigin.Begin);
            var guid = StartMeasurement();
            string liteAttachmentId = _db.AddAttachment(new BufferFileMeta
            {
                AttachmentType = BufferFileMeta.AttachmentTypes.DocLib,
                Filename = filename,
                Listname = _helpers.GetListName<T>(),
                FullyQualifiedName = typeof(T).FullName,
                Folder = folder,
                AdditionalInformation = JsonConvert.SerializeObject(entity, Formatting.Indented)
            }, filestream, updateState);

            if (addToQueue && !nameOnly)
            {
                Queue.AddCommand<T>(new Command
                {
                    Action = Command.Actions.AttachFileToLibrary,
                    TableName = _helpers.GetListName<T>(),
                    Parameters = new Dictionary<string, object> { { "AttachmentId", liteAttachmentId }, { "Folder", folder } }
                });
            }

            StopMeasurement(guid, "AttachFileToLibrary (LiteDB)");
            return $"{folder}/{filename}";
        }

        public int CountItems<T>()
        {
            var guid = StartMeasurement();
            var result = _db.GetCollection<T>().Count();
            StopMeasurement(guid, "CountItems (LiteDB)");
            return result;
        }

        public int CountItemsByFieldValue<T>(string fieldname, object value) where T : new()
        {
            var guid = StartMeasurement();
            var result = SelectItemsByFieldValue<T>(fieldname, value).Count();
            StopMeasurement(guid, "CountItemsByFieldValue (LiteDB)");
            return result;
        }

        private bool ItemMatchesConditions<T>(T item, Dictionary<string, object> conditions, bool isAndCondition)
        {
            var guid = StartMeasurement();
            Dictionary<string, PropertyInfo> properties = CreatePropertiesFromConditions<T>(conditions);
            int matchesCount = 0;
            foreach (var property in properties)
            {
                if (conditions.ContainsKey(property.Key))
                {
                    var propertyValue = property.Value.GetValue(item);
                    var conditionValue = conditions[property.Key];
                    if (propertyValue == null)
                    {
                        if (conditionValue == null) matchesCount++;
                        continue;
                    }

                    if (property.Value.PropertyType == conditionValue.GetType())
                    {
                        if (propertyValue.Equals(conditionValue)) matchesCount++;
                        continue;
                    }
                    else if (property.Value.PropertyType == typeof(KeyValuePair<int, string>) && conditionValue.GetType() == typeof(int))
                    {
                        var pair = (KeyValuePair<int, string>)propertyValue;
                        if (pair.Key == (int)conditionValue) matchesCount++;
                        continue;
                    }
                    else if (property.Value.PropertyType == typeof(Dictionary<int, string>) && conditionValue.GetType() == typeof(int))
                    {
                        var pair = (Dictionary<int, string>)propertyValue;
                        if (pair.Keys.Contains((int)conditionValue)) matchesCount++;
                        continue;
                    }
                }
            }
            StopMeasurement(guid, "ItemsMatchesConditions (LiteDB)");
            return (matchesCount == properties.Count || matchesCount > 0 && !isAndCondition);
        }

        public int CountItemsByMultipleFieldValues<T>(Dictionary<string, object> conditions, bool isAndCondition = true)
        {
            var guid = StartMeasurement();
            int counter = 0;
            var collection = _db.GetCollection<T>();

            if (collection.Count() == 0) return 0;
            var allItems = collection.FindAll();

            foreach (var item in allItems)
            {
                if (ItemMatchesConditions(item, conditions, isAndCondition)) counter++;
            }
            StopMeasurement(guid, "CountItemsByMultipleFieldValues (LiteDB)");
            return counter;
        }

        public int CountItemsByQuery<T>(string query)
        {
            throw new NotImplementedException(); // Cannot be converted to LiteDB
        }

        public void DeleteFileFromItem<T>(int id, string filename)
        {
            var guid = StartMeasurement();
            _db.RemoveAttachment(new BufferFileMeta
            {
                ParentId = id,
                Listname = _helpers.GetListName<T>(),
                AttachmentType = BufferFileMeta.AttachmentTypes.Attachment,
                Filename = filename
            });
            Queue.AddCommand<T>(new Command
            {
                Action = Command.Actions.RemoveAttachmentFromItem,
                ItemId = id,
                TableName = _helpers.GetListName<T>(),
                Parameters = new Dictionary<string, object> { { "Filename", filename } }
            });
            StopMeasurement(guid, "DeleteFileFromItem (LiteDB)");
        }

        public void DeleteFilesFromDocumentLibrary<T>(string path, List<string> filenames)
        {
            var guid = StartMeasurement();
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
                Queue.AddCommand<T>(new Command
                {
                    Action = Command.Actions.RemoveFileFromLibrary,
                    Parameters = new Dictionary<string, object> { { "Filename", filename }, { "Folder", path } },
                    TableName = _helpers.GetListName<T>()
                });
            }
            StopMeasurement(guid, "DeleteFilesFromDocumentLibrary (LiteDB)");
        }

        public void DeleteFolderFromDocumentLibrary<T>(string path, string folder)
        {
            var guid = StartMeasurement();
            // not buffered
            _baseTable.DeleteFolderFromDocumentLibrary<T>(path, folder);
            StopMeasurement(guid, "DeleteFolderFromDocumentLibrary (SharePoint)");
        }

        public void DeleteItemById<T>(int id)
        {
            var guid = StartMeasurement();
            _db.GetCollection<T>().Delete(id);
            Queue.AddCommand<T>(new Command
            {
                Action = Command.Actions.Delete,
                ItemId = id,
                //  FullyQualifiedName = typeof(T).FullName,
                TableName = _helpers.GetListName<T>()
            });
            StopMeasurement(guid, "DeleteItemById (LiteDB)");
        }

        public void DeleteTable<T>()
        {
            var guid = StartMeasurement();
            // Not buffered
            _baseTable.DeleteTable<T>();
            BufferState.RemoveTable(_helpers.GetListName<T>());
            StopMeasurement(guid, "DeleteTable (LiteDB)");
        }

        public void DeleteTableIfExisting<T>()
        {
            var guid = StartMeasurement();
            // Not buffered
            _baseTable.DeleteTableIfExisting<T>();
            BufferState.RemoveTable(_helpers.GetListName<T>());
            StopMeasurement(guid, "DeleteTableIfExisting (LiteDB)");
        }

        public void Empty<T>()
        {
            var guid = StartMeasurement();
            _db.GetCollection<T>().Delete(LiteDB.Query.All());
            Queue.AddCommand<T>(new Command
            {
                Action = Command.Actions.Empty,
                TableName = _helpers.GetListName<T>()
            });
            StopMeasurement(guid, "Empty (LiteDB)");
        }

        public string[] GetAvailableChoicesFromField<T>(string propertyname)
        {
            var guid = StartMeasurement();
            var result = _baseTable.GetAvailableChoicesFromField<T>(propertyname); // unbuffered
            StopMeasurement(guid, "GetAvailableChoices (SharePoint)");
            return result;
        }

        public int InsertItem<T>(T entity)
        {
            var guid = StartMeasurement();
            AutosetCreated(entity);
            string listname = _helpers.GetListName<T>();
            int itemId = _db.Insert(entity, listname);

            Queue.AddCommand<T>(new Command
            {
                Action = Command.Actions.Insert,
                ItemId = itemId,
                TableName = listname
            });
            StopMeasurement(guid, "InsertItem (LiteDB)");
            return itemId;
        }

        private void AutosetCreated<T>(T item)
        {
            AutosetDateTimeField(item, typeof(T).GetProperty(nameof(AweCsomeListItem.Created)));
        }

        private void AutosetModified<T>(T item)
        {
            AutosetDateTimeField(item, typeof(T).GetProperty(nameof(AweCsomeListItem.Modified)));
        }

        private void AutosetDateTimeField<T>(T item, PropertyInfo field)
        {
            if (field == null) return;
            var oldValue = (DateTime?)field.GetValue(item);
            if (oldValue != null && oldValue > new DateTime(1900, 1, 1)) return;
            field.SetValue(item, DateTime.Now);
        }

        public T Like<T>(int id, int userId) where T : new()
        {
            var guid = StartMeasurement();
            var item = SelectItemById<T>(id);
            if (item == null) throw new Exceptions.ItemNotFoundException();

            GetLikeData(item, out int likesCount, out Dictionary<int, string> likedBy);

            if (likedBy.ContainsKey(userId))
            {
                return default(T);
            }

            likedBy.Add(userId, null);
            likesCount++;

            UpdateLikeData(item, likesCount, likedBy);

            _db.GetCollection<T>().Update(item);
            Queue.AddCommand<T>(new Command
            {
                Action = Command.Actions.Like,
                ItemId = id,
                TableName = _helpers.GetListName<T>(),
                Parameters = new Dictionary<string, object> { { "User", userId } }
            });
            StopMeasurement(guid, "Like (LiteDB)");
            return item;
        }

        public List<T> SelectAllItems<T>() where T : new()
        {
            var guid = StartMeasurement();
            var result = _db.GetCollection<T>().FindAll().ToList();
            StopMeasurement(guid, "SelectAllItems (LiteDB)");
            return result;
        }

        public AweCsomeFile SelectFileFromLibrary<T>(string foldername, string filename) where T : new()
        {
            var guid = StartMeasurement();
            var localFiles = _db.GetFilesFromDocLib<T>(foldername);
            if (filename != null) localFiles = localFiles.Where(q => q.Filename == filename).ToList();
            if (localFiles.FirstOrDefault() != null)
            {
                return localFiles.FirstOrDefault();
            }
            var spFile = _baseTable.SelectFileFromLibrary<T>(foldername, filename);
            StopMeasurement(guid, "SelectFileFromLibrary (LiteDB + SharePoint)");
            return spFile;
        }

        public List<KeyValuePair<DateTime, string>> GetLocalFiles<T>(int id)
        {
            var guid = StartMeasurement();
            var result = _db.GetAttachmentNamesFromItem<T>(id);
            StopMeasurement(guid, "GetLocalFiles (LiteDB)");
            return result;
        }

        public List<KeyValuePair<DateTime, string>> SelectFileNamesFromItem<T>(int id)
        {
            var guid = StartMeasurement();
            var localFiles = GetLocalFiles<T>(id);
            //var remoteFiles = _baseTable.SelectFileNamesFromItem<T>(id);
            //localFiles.ForEach(q => remoteFiles.Add(q));
            StopMeasurement(guid, "SelectFileNamesFromItem (LiteDB)");
            return localFiles;
        }

        public List<string> SelectFileNamesFromLibrary<T>(string foldername)
        {
            var guid = StartMeasurement();




            var localFiles = _db.GetFilenamesFromLibrary<T>(foldername);
            //var remoteFiles = _baseTable.SelectFileNamesFromLibrary<T>(foldername);
            //localFiles.ForEach(q => remoteFiles.Add(q));
            StopMeasurement(guid, "SelectFileNamesFromLibrary (LiteDB)");
            return localFiles;
        }

        public List<AweCsomeFile> SelectLocalFilesFromLibrary<T>(string foldername, bool retrieveContent = true) where T : new()
        {
            var guid = StartMeasurement();
            var localFiles = _db.GetFilesFromDocLib<T>(foldername, retrieveContent);
            StopMeasurement(guid, "SeletLocalFilesFromLibrary (LiteDB)");
            return localFiles;
        }

        public List<AweCsomeFile> SelectFilesFromLibrary<T>(string foldername, bool retrieveContent = true) where T : new()
        {
            var guid = StartMeasurement();

            var localFiles = _db.GetFilesFromDocLib<T>(foldername, retrieveContent);
            if (!localFiles.Any(q => q.Stream == null) || !retrieveContent)
            {
                StopMeasurement(guid, "SelectFilesFromDocLib (Local)");
                return localFiles;
            }
            var spFiles = _baseTable.SelectFilesFromLibrary<T>(foldername, retrieveContent) ?? new List<AweCsomeFile>();
            foreach (var localFile in localFiles)
            {
                if (localFile.Stream == null)
                {
                    var match = spFiles.FirstOrDefault(q => q.Filename == localFile.Filename);
                    if (match != null) localFile.Stream = match.Stream;
                }
            }
            StopMeasurement(guid, "SelectFilesFromItem (Local + SharePoint)");
            return localFiles;
        }

        public T SelectItemById<T>(int id) where T : new()
        {
            var guid = StartMeasurement();
            var collection = _db.GetCollection<T>();
            var item = collection.FindById(id);
            if (item == null && id < 0)
            {
                var oldIdProperty = typeof(T).GetProperty(nameof(AweCsomeListItemBuffered.BufferId));
                if (oldIdProperty == null) return default(T);

                var allItems = collection.FindAll();
                foreach (var subItem in allItems)
                {
                    if (((int)oldIdProperty.GetValue(subItem)) == id)
                    {
                        return subItem;
                    }
                }
            }
            StopMeasurement(guid, "SelectItemById (LiteDB)");
            return item;
        }

        public List<T> SelectItemsByFieldValue<T>(string fieldname, object value) where T : new()
        {
            var guid = StartMeasurement();
            var matches = new List<T>();
            var collection = _db.GetCollection<T>();
            PropertyInfo property = null;

            foreach (var item in collection.FindAll())
            {
                property = property ?? typeof(T).GetProperty(fieldname);
                if (property == null) throw new MissingFieldException($"Field '{fieldname}' cannot be found");
                if (!property.CanRead) throw new FieldAccessException($"Field '{fieldname}' cannot be queried");
                var propertyValue = property.GetValue(item);
                if (propertyValue == null) continue;
                var lookupAttribute = property.GetCustomAttribute<LookupAttribute>();
                var peopleAttribute = property.GetCustomAttribute<UserAttribute>();
                if (property.PropertyType == typeof(KeyValuePair<int, string>)) propertyValue = ((KeyValuePair<int, string>)propertyValue).Key;
                if (property.PropertyType == typeof(Dictionary<int, string>)) propertyValue = ((Dictionary<int, string>)propertyValue).Keys.ToArray();
                if (property.PropertyType.IsClass)
                {
                    var idProperty = property.PropertyType.GetProperty("Id");
                    if (idProperty != null)
                    {
                        var id = idProperty.GetValue(propertyValue);
                        if (id.Equals(value)) matches.Add(item);
                        continue;
                    }
                }
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
            StopMeasurement(guid, "SelectitemsbyFieldValue (LiteDB)");
            return matches;
        }

        private Dictionary<string, PropertyInfo> CreatePropertiesFromConditions<T>(Dictionary<string, object> conditions)
        {
            var properties = new Dictionary<string, PropertyInfo>();
            foreach (var condition in conditions)
            {
                var property = typeof(T).GetProperty(condition.Key);
                if (property == null) throw new MissingFieldException($"Field '{condition.Key}' cannot be found");
                if (!property.CanRead) throw new FieldAccessException($"Field '{condition.Key}' cannot be queried");
                properties.Add(condition.Key, property);
            }
            return properties;
        }

        public List<T> SelectItemsByMultipleFieldValues<T>(Dictionary<string, object> conditions, bool isAndCondition = true) where T : new()
        {
            var guid = StartMeasurement();
            var matches = new List<T>();
            var collection = _db.GetCollection<T>();
            if (collection.Count() == 0) return new List<T>();
            var allItems = collection.FindAll();

            foreach (var item in allItems)
            {
                if (ItemMatchesConditions(item, conditions, isAndCondition)) matches.Add(item);
            }
            StopMeasurement(guid, "SelectItemsByMultipleFieldValues (LiteDB)");
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

        private void GetLikeData<T>(T item, out int likesCount, out Dictionary<int, string> likedBy)
        {
            var guid = StartMeasurement();
            PropertyInfo likesCountProperty = typeof(T).GetProperty("LikesCount");
            PropertyInfo likedByProperty = typeof(T).GetProperty("LikedBy");

            if (likedByProperty == null || likesCountProperty == null) throw new Exceptions.FieldMissingException("Like-Fields missing", "LikedBy,LikesCount");
            likedBy = (Dictionary<int, string>)likedByProperty.GetValue(item) ?? new Dictionary<int, string>();
            likesCount = (int)likesCountProperty.GetValue(item);
            StopMeasurement(guid, "GetLikeData (LiteDB)");
        }

        private void UpdateLikeData<T>(T item, int likesCount, Dictionary<int, string> likedBy)
        {
            var guid = StartMeasurement();
            PropertyInfo likesCountProperty = typeof(T).GetProperty("LikesCount");
            PropertyInfo likedByProperty = typeof(T).GetProperty("LikedBy");

            likesCountProperty.SetValue(item, likesCount);
            likedByProperty.SetValue(item, likedBy);
            StopMeasurement(guid, "UpdateLikeData (LiteDB)");
        }

        public T Unlike<T>(int id, int userId) where T : new()
        {
            var guid = StartMeasurement();
            var item = SelectItemById<T>(id);
            if (item == null) throw new Exceptions.ItemNotFoundException();
            GetLikeData(item, out int likesCount, out Dictionary<int, string> likedBy);
            if (!likedBy.ContainsKey(userId))
            {
                // didn't like
                return default(T);
            }

            likedBy = likedBy.Where(q => q.Key != userId).ToDictionary(q => q.Key, q => q.Value);
            likesCount--;
            UpdateLikeData(item, likesCount, likedBy);
            _db.GetCollection<T>().Update(item);
            Queue.AddCommand<T>(new Command
            {
                Action = Command.Actions.Unlike,
                ItemId = id,
                TableName = _helpers.GetListName<T>(),
                //FullyQualifiedName = typeof(T).FullName,
                Parameters = new Dictionary<string, object> { { "User", userId } }
            });
            StopMeasurement(guid, "UnLike (LiteDB)");
            return item;
        }

        public void UpdateItem<T>(T entity)
        {
            var guid = StartMeasurement();
            AutosetModified(entity);
            _db.GetCollection<T>().Update(entity);
            Queue.AddCommand<T>(new Command
            {
                Action = Command.Actions.Update,
                ItemId = _helpers.GetId(entity),
                TableName = _helpers.GetListName<T>()
            });
            StopMeasurement(guid, "UpdateItem (LiteDB)");
        }

        public Guid CreateTable<T>()
        {
            var guid = StartMeasurement();
            Guid newId = _baseTable.CreateTable<T>();
            BufferState.AddTable(_helpers.GetListName<T>(), newId);
            StopMeasurement(guid, "CreateTable (SharePoint)");
            return newId;
        }

        public List<AweCsomeFile> SelectFilesFromItem<T>(int id, string filename = null)
        {
            var guid = StartMeasurement();

            var localFiles = _db.GetAttachmentsFromItem<T>(id);
            if (filename != null) localFiles = localFiles.Where(q => q.Filename == filename).ToList();
            if (!localFiles.Any(q => q.Stream == null))
            {
                StopMeasurement(guid, "SelectFilesFromItem (Local)");
                return localFiles;
            }

            var spFiles = _baseTable.SelectFilesFromItem<T>(id, filename) ?? new List<AweCsomeFile>();
            foreach (var localFile in localFiles)
            {
                if (localFile.Stream == null)
                {
                    var match = spFiles.FirstOrDefault(q => q.Filename == localFile.Filename);
                    if (match != null) localFile.Stream = match.Stream;
                }
            }
            StopMeasurement(guid, "SelectFilesFromItem (Local + SharePoint)");
            return localFiles;
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
            var guid = StartMeasurement();
            var result = _db.GetCollectionNames().Contains(typeof(T).Name);
            StopMeasurement(guid, "Exists (LiteDB)");
            return result;
        }

        public void ReadAllLists(Type baseType, string forbiddenNamespace = null)
        {
            var guid = StartMeasurement();
            _db.ReadAllLists(baseType, forbiddenNamespace);
            StopMeasurement(guid, "ReadAllLists (SharePoint)");
        }

        public void ReadAllFromList<T>() where T : new()
        {
            var guid = StartMeasurement();
            _db.ReadAllFromList<T>();
            StopMeasurement(guid, "ReadAllFromList (LiteDB)");
        }

        public void ReadAllFromList(Type entityType)
        {
            var guid = StartMeasurement();
            _db.ReadAllFromList(entityType);
            StopMeasurement(guid, "ReadAllFromList (LiteDB)");
        }

        public void UpdateTableStructure<T>()
        {
            var guid = StartMeasurement();
            _baseTable.UpdateTableStructure<T>();
            StopMeasurement(guid, "UpdateTableStructure (SharePoint)");
        }

        public void StoreAttachmentsInLiteDb<T>(long maxFilesize) where T : AweCsomeListItem, new()
        {
            var guid = StartMeasurement();
            ClearAttachmentsInLiteDB<T>();
            var allItems = _baseTable.SelectAllItems<T>();
            foreach (var itemId in allItems.Select(q => q.Id))
            {
                var files = _baseTable.SelectFilesFromItem<T>(itemId);
                if (files == null) continue;
                foreach (var file in files)
                {
                    long fileSize = file.Length;
                    if (fileSize > maxFilesize)
                    {
                        AttachFileToItem<T>(itemId, file.Filename, file.Stream, true, false);
                        _log.Debug($"Attachment '{file.Filename}' NOT stored into LiteDB (too big: {EntityHelper.PrettyLong(fileSize)})");
                    }
                    else
                    {
                        AttachFileToItem<T>(itemId, file.Filename, file.Stream, false, false);
                        _log.Debug($"Stored Attachment '{file.Filename}' into LiteDB ({EntityHelper.PrettyLong(fileSize)})");
                    }
                }
            }
            StopMeasurement(guid, "StoreAttachmentsInLiteDb (SharePoint)");
        }

        private void ClearDocLibInLiteDB<T>(string folder) where T : AweCsomeListItem, new()
        {
            folder = folder.Replace("\\", "");
            folder = folder.Replace("/", "");

            var collection = _db.GetCollection<FileDoclib>();
            collection.Delete(q => q.List == typeof(T).Name && q.Folder.Replace("\\", "").Replace("/", "") == folder);
        }

        private void ClearAttachmentsInLiteDB<T>() where T : AweCsomeListItem, new()
        {
            var collection = _db.GetCollection<FileAttachment>();
            collection.Delete(q => q.List == typeof(T).Name);
        }

        public void StoreDocLibInLiteDb<T>(long maxFilesize, string folder) where T : AweCsomeListItem, new()
        {
            var guid = StartMeasurement();
            ClearDocLibInLiteDB<T>(folder);
            var allAttachments = _baseTable.SelectFilesFromLibrary<T>(folder, true);
            if (allAttachments == null) return;
            int totalFileCount = allAttachments.Count;
            long totalSize = 0;
            int count = 0;
            foreach (var file in allAttachments)
            {
                long fileSize = file.Length;
                totalSize += fileSize;
                count++;
                if (fileSize > maxFilesize)
                {
                    int? id = null;
                    if (file.Entity != null)
                    {
                        var idProperty = file.Entity.GetType().GetProperty("Id");
                        if (idProperty != null)
                        {
                            id = (int)idProperty.GetValue(file.Entity);
                        }
                    }
                    AttachFileToLibrary<T>(file.Folder, file.Filename, file.Stream, (T)file.Entity, true, false);
                    _log.Debug($"File from DocLib '{file.Filename}' NOT stored into LiteDB (too big: {EntityHelper.PrettyLong(fileSize)})");
                }
                else
                {
                    AttachFileToLibrary<T>(file.Folder, file.Filename, file.Stream, (T)file.Entity, false, false);
                    _log.Debug($"Stored Attachment '{file.Filename}' into LiteDB ({EntityHelper.PrettyLong(fileSize)}) [File {count} of {totalFileCount}, {EntityHelper.PrettyLong(totalSize)} total]");
                }
            }
            StopMeasurement(guid, "StoreDocLibInLiteDb (SharePoint)");
        }

        public bool IsLikedBy<T>(int id, int userId) where T : new()
        {
            return GetLikes<T>(id).ContainsKey(userId);
        }

        public Dictionary<int, string> GetLikes<T>(int id) where T : new()
        {
            var item = SelectItemById<T>(id);
            if (item == null) throw new Exceptions.ItemNotFoundException();

            GetLikeData(item, out int likesCount, out Dictionary<int, string> likedBy);
            return likedBy;
        }






    }
}