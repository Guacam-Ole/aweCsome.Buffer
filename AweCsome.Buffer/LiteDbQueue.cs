using AweCsome.Attributes.FieldAttributes;
using AweCsome.Buffer.Attributes;
using AweCsome.Buffer.Entities;
using AweCsome.Buffer.Interfaces;
using AweCsome.Interfaces;
using log4net;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace AweCsome.Buffer
{
    public class LiteDbQueue : LiteDb, ILiteDbQueue, ILiteDb
    {
        private static readonly object _queueLock = new object();
        private readonly ILog _log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);


        public LiteDbQueue(IAweCsomeHelpers helpers, IAweCsomeTable aweCsomeTable, string connectionString) : base(helpers, aweCsomeTable, connectionString, true)
        {
        }

        public void Empty()
        {
            lock (_queueLock)
            {
                DeleteTable(nameof(Command));
            }
        }

        public void AddCommand<T>(Command command)
        {
            lock (_queueLock)
            {
                var commandCollection = GetCollection<Command>();
                var maxId = commandCollection.Max(q => q.Id).AsInt32;
                command.Id = maxId + 1;
                command.FullyQualifiedName = typeof(T).FullName;
                commandCollection.Insert(command);
            }
        }

        public List<Command> Read()
        {
            return GetCollection<Command>(null).FindAll().OrderBy(q => q.Created).ToList();
        }

        public void Update(Command command)
        {
            GetCollection<Command>(null).Update(command);
        }

        public void Delete(Command command)
        {
            GetCollection<Command>().Delete(command.Id);
        }

        private string GetListNameFromFullyQualifiedName(Type baseType, string fullyQualifiedName)
        {
            return baseType.Assembly.GetType(fullyQualifiedName, false, true).Name;
        }

        public object GetFromDbById(Type baseType, string fullyQualifiedName, int id)
        {
            var db = new LiteDb(_helpers, _aweCsomeTable, _connectionString);
            MethodInfo method = GetMethod<LiteDb>(q => q.GetCollection<object>());
            dynamic collection = CallGenericMethodByName(db, method, baseType, fullyQualifiedName, null);

            return collection.FindById(id);
        }

        public System.IO.MemoryStream GetAttachmentStreamFromDbById(string id, out string filename, out BufferFileMeta meta)
        {
            var db = new LiteDb(_helpers, _aweCsomeTable, _connectionString);
            return db.GetAttachmentStreamById(id, out filename, out meta);
        }

        public void DeleteAttachmentFromDbWithoutSyncing(BufferFileMeta meta)
        {
            var db = new LiteDb(_helpers, _aweCsomeTable, _connectionString);
            db.RemoveAttachment(meta);
        }

        public void UpdateId(Type baseType, string fullyQualifiedName, int oldId, int newId)
        {
            var db = new LiteDb(_helpers, _aweCsomeTable, _connectionString);
            MethodInfo method = GetMethod<LiteDb>(q => q.GetCollection<object>());


            dynamic collection = CallGenericMethodByName(db, method, baseType, fullyQualifiedName, null);
            var entity = collection.FindById(oldId);

            if (entity == null)
            {
                _log.Error($"Cannot find {fullyQualifiedName} from id {oldId} to change to {newId}");
                throw new KeyNotFoundException();
            }
            entity.Id = newId;

            // Id CANNOT be updated in LiteDB. We have to delete and recreate instead:
            collection.Delete(oldId);
            collection.Insert(entity);

            UpdateLookups(baseType, GetListNameFromFullyQualifiedName(baseType, fullyQualifiedName), oldId, newId);
            UpdateFileLookups(baseType, GetListNameFromFullyQualifiedName(baseType, fullyQualifiedName), oldId, newId);
            UpdateQueueIds(fullyQualifiedName, oldId, newId);
        }

        private void UpdateQueueIds(string fullyQualifiedName, int oldId, int newId)
        {
            var commandCollection = GetCollection<Command>();
            var commands = commandCollection.Find(q => q.ItemId == oldId && q.FullyQualifiedName == fullyQualifiedName);
            foreach (var command in commands)
            {
                command.ItemId = newId;
                commandCollection.Update(command);
            }
        }

        private void UpdateFileLookups(Type baseType, string changedListname, int oldId, int newId)
        {
            var db = new LiteDb(_helpers, _aweCsomeTable, _connectionString);
            foreach (var file in db.GetAllFiles())
            {
                var meta = db.GetMetadataFromAttachment(file.Metadata);
                if (meta.AttachmentType == BufferFileMeta.AttachmentTypes.Attachment)
                {
                    if (meta.Listname == changedListname && meta.ParentId == oldId)
                    {
                        meta.ParentId = newId;
                        db.UpdateMetadata(file.Id, db.GetMetadataFromAttachment(meta));

                    }
                }
                else
                {
                    if (meta.AdditionalInformation != null)
                    {
                        Type targetType = baseType.Assembly.GetTypes().FirstOrDefault(q => q.FullName == meta.FullyQualifiedName);
                        if (targetType == null) continue;
                        var entity = JsonConvert.DeserializeObject(meta.AdditionalInformation, targetType);
                        if (FindLookupProperties(targetType, changedListname, out List<PropertyInfo> lookupProperties, out List<PropertyInfo> virtualStaticProperties, out List<PropertyInfo> virtualDynamicProperties))
                        {
                            bool elementChanged = false;
                            foreach (var lookupProperty in lookupProperties)
                            {
                                if ((int?)lookupProperty.GetValue(entity) == oldId)
                                {
                                    lookupProperty.SetValue(entity, newId);
                                    elementChanged = true;
                                }
                            }
                            foreach (var virtualStaticPropery in virtualStaticProperties)
                            {
                                if ((int?)virtualStaticPropery.GetValue(entity) == oldId)
                                {
                                    virtualStaticPropery.SetValue(entity, newId);
                                    elementChanged = true;
                                }
                            }
                            foreach (var virtualDynamicProperty in virtualDynamicProperties)
                            {
                                var attribute = virtualDynamicProperty.GetCustomAttribute<VirtualLookupAttribute>();
                                var targetList = (string)targetType.GetProperty(attribute.DynamicTargetProperty).GetValue(entity);
                                if (targetList == changedListname)
                                {
                                    if ((int?)virtualDynamicProperty.GetValue(entity) == oldId)
                                    {
                                        virtualDynamicProperty.SetValue(entity, newId);
                                        elementChanged = true;
                                    }
                                }
                            }
                            if (elementChanged)
                            {
                                meta.AdditionalInformation = JsonConvert.SerializeObject(entity, Formatting.Indented);
                                db.UpdateMetadata(file.Id, db.GetMetadataFromAttachment(meta));
                            }
                        }
                    }
                }
            }
        }

        private bool FindLookupProperties(Type subType, string changedListname, out List<PropertyInfo> lookupProperties, out List<PropertyInfo> virtualStaticProperties, out List<PropertyInfo> virtualDynamicProperties)
        {
            PropertyInfo dynamicTargetProperty = null;
            bool modifyId = false;
            lookupProperties = new List<PropertyInfo>();
            virtualStaticProperties = new List<PropertyInfo>();
            virtualDynamicProperties = new List<PropertyInfo>();

            foreach (var property in subType.GetProperties())
            {
                bool propertyHasLookups = false;
                var virtualLookupAttribute = property.GetCustomAttribute<VirtualLookupAttribute>();
                var lookupAttribute = property.GetCustomAttribute<LookupAttribute>();
                if (virtualLookupAttribute != null || lookupAttribute != null) propertyHasLookups = true;

                if (propertyHasLookups)
                {
                    if (virtualLookupAttribute != null)
                    {
                        if (virtualLookupAttribute.StaticTarget != null)
                        {
                            if (virtualLookupAttribute.StaticTarget != changedListname) continue;

                            modifyId = true;
                            virtualStaticProperties.Add(property);
                        }
                        else
                        {
                            if (virtualLookupAttribute.DynamicTargetProperty == null) continue;
                            dynamicTargetProperty = subType.GetProperty(virtualLookupAttribute.DynamicTargetProperty);
                            if (dynamicTargetProperty == null) continue;
                            modifyId = true;    // MIGHT be
                            virtualDynamicProperties.Add(property);
                        }
                    }
                    else if (lookupAttribute != null)
                    {
                        if ((lookupAttribute.List ?? property.Name) != changedListname) continue;
                        lookupProperties.Add(property);
                        modifyId = true;
                    }
                }
                if (modifyId) break;
            }
            return modifyId;
        }

        private void UpdateLookups(Type baseType, string changedListname, int oldId, int newId)
        {
            var db = new LiteDb(_helpers, _aweCsomeTable, _connectionString);
            List<string> collectionNames = db.GetCollectionNames().ToList();
            var subTypes = baseType.Assembly.GetTypes();
            foreach (var subType in subTypes)
            {
                if (!collectionNames.Contains(subType.Name)) continue;

                bool modifyId = FindLookupProperties(subType, changedListname, out List<PropertyInfo> lookupProperties, out List<PropertyInfo> virtualStaticProperties, out List<PropertyInfo> virtualDynamicProperties);
                if (modifyId)
                {
                    var collection = db.GetCollection(subType.Name);
                    var elements = collection.FindAll();
                    bool elementChanged = false;
                    foreach (var element in elements)
                    {
                        foreach (var lookupProperty in lookupProperties)
                        {
                            var targetType = element[lookupProperty.Name].GetType();
                            if (element[lookupProperty.Name] is LiteDB.BsonDocument)
                            {
                                var bson = (LiteDB.BsonDocument)element[lookupProperty.Name];
                                var id = bson["_id"];
                                if (id==oldId)
                                {
                                    bson["_id"] = newId;
                                    element[lookupProperty.Name] = bson;
                                    elementChanged = true;
                                }
                            }
                            else if (targetType.IsClass)
                            {
                                var idProperty = targetType.GetProperty("Id");
                                if (idProperty == null) throw new Exception("Unexpected LookupType");
                                var id = idProperty.GetValue(element[lookupProperty.Name]);
                                if (id.Equals(oldId))
                                {
                                    idProperty.SetValue(element[lookupProperty.Name], newId);
                                    elementChanged = true;
                                }
                            } else { 
                                if ((int?)element[lookupProperty.Name] == oldId)
                                {
                                    element[lookupProperty.Name] = newId;
                                    elementChanged = true;
                                }
                            }

                        }
                        foreach (var virtualStaticProperty in virtualStaticProperties)
                        {
                            if (!element.ContainsKey(virtualStaticProperty.Name)) continue;
                            if ((int?)element[virtualStaticProperty.Name] == oldId)
                            {
                                element[virtualStaticProperty.Name] = newId;
                                elementChanged = true;
                            }
                        }
                        foreach (var virtualDynamicProperty in virtualDynamicProperties)
                        {
                            var attribute = virtualDynamicProperty.GetCustomAttribute<VirtualLookupAttribute>();
                            if (!element.ContainsKey(attribute.DynamicTargetProperty)) continue;

                            if (element[attribute.DynamicTargetProperty] == changedListname)
                            {
                                if ((int?)element[virtualDynamicProperty.Name] == oldId)
                                {
                                    element[virtualDynamicProperty.Name] = newId;
                                    elementChanged = true;
                                }
                            }
                        }
                        if (elementChanged) collection.Update(element);
                    }
                }
            }
        }

        public void Sync(Type baseType)
        {
            // Delete old Entries:
            var oldEntries = Read().Where(q => q.State == Command.States.Succeeded);
            _log.Debug($"Deleting {oldEntries.Count()} old entries from queue");
            foreach (var oldEntry in oldEntries)
            {
                Delete(oldEntry);
            }

            var execution = new QueueCommandExecution(this, _aweCsomeTable, baseType);
            var queueCount = Read().Where(q => q.State == Command.States.Pending).Count();

            _log.Info($"Working with queue ({queueCount} pending commands)");
            Command command;
            int realCount = 0;
            while ((command = Read().Where(q => q.State == Command.States.Pending || q.State == Command.States.Failed).OrderBy(q => q.Id).ToList().FirstOrDefault()) != null)
            {
                realCount++;
                _log.Debug($"storing command {command}");
                string commandAction = $"{command.Action}";
                try
                {
                    MethodInfo method = typeof(QueueCommandExecution).GetMethod(commandAction);
                    bool success = (bool)method.Invoke(execution, new object[] { command });
                    if (success)
                    {
                        command.State = Command.States.Succeeded;
                        Update(command);
                    }
                    else
                    {
                        _log.Error("Command failed");
                        command.State = Command.States.Failed;
                        Update(command);
                        break;
                    }
                }
                catch (Exception ex)
                {
                    _log.Error($"Cannot find method for action '{commandAction}'", ex);
                    break;
                }
            }
            _log.Debug($"{realCount} of {queueCount} items synced (can be higher if new items have been added while in loop)");
        }
    }
}
