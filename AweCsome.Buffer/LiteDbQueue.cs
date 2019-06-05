using AweCsome.Attributes.FieldAttributes;
using AweCsome.Buffer.Attributes;
using AweCsome.Buffer.Interfaces;
using AweCsome.Interfaces;
using log4net;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace AweCsome.Buffer
{
    public class LiteDbQueue : LiteDb, ILiteDbQueue, ILiteDb
    {
        private static object _queueLock = new object();
        private readonly ILog _log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);


        public LiteDbQueue(IAweCsomeHelpers helpers, IAweCsomeTable aweCsomeTable, string databaseName) : base(helpers, aweCsomeTable, databaseName, true)
        {
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

        private void Delete(Command command)
        {
            GetCollection<Command>().Delete(command.Id);
        }

        private string GetListNameFromFullyQualifiedName(Type baseType, string fullyQualifiedName)
        {
            return baseType.Assembly.GetType(fullyQualifiedName, false, true).Name;
        }

        public object GetFromDbById(Type baseType, string fullyQualifiedName, int id)
        {
            var db = new LiteDb(_helpers, _aweCsomeTable, _databaseName);
            MethodInfo method = GetMethod<LiteDb>(q => q.GetCollection<object>());
            dynamic collection = CallGenericMethodByName(db, method, baseType, fullyQualifiedName, null);

            return collection.FindById(id);
        }

        public System.IO.MemoryStream GetAttachmentStreamFromDbById(string id, out string filename, out BufferFileMeta meta)
        {
            var db = new LiteDb(_helpers, _aweCsomeTable, _databaseName);
            return db.GetAttachmentStreamById(id, out filename, out meta);
        }

        public void DeleteAttachmentFromDbWithoutSyncing(BufferFileMeta meta)
        {
            var db = new LiteDb(_helpers, _aweCsomeTable, _databaseName);
            db.RemoveAttachment(meta);
        }

        public void UpdateId(Type baseType, string fullyQualifiedName, int oldId, int newId)
        {
            var db = new LiteDb(_helpers, _aweCsomeTable, _databaseName);
            MethodInfo method = GetMethod<LiteDb>(q => q.GetCollection<object>());
            dynamic collection = CallGenericMethodByName(db, method, baseType, fullyQualifiedName, null);
            var entity = collection.FindById(oldId);
            entity.Id = newId;

            // Id CANNOT be updated in LiteDB. We have to delete and recreate instead:
            collection.Delete(oldId);
            collection.Insert(entity);

            UpdateLookups(baseType, GetListNameFromFullyQualifiedName(baseType, fullyQualifiedName), oldId, newId);
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

        private void UpdateLookups(Type baseType, string changedListname, int oldId, int newId)
        {
            // TODO: Update Lookups after changing Id
            var db = new LiteDb(_helpers, _aweCsomeTable, _databaseName);
            List<string> collectionNames = db.GetCollectionNames().ToList();
            var subTypes = baseType.Assembly.GetTypes();
            foreach (var subType in subTypes)
            {
                if (!collectionNames.Contains(subType.Name)) continue;

                PropertyInfo dynamicTargetProperty = null;
                bool modifyId = false;
                var lookupProperties = new List<PropertyInfo>();
                var virtualStaticProperties = new List<PropertyInfo>();
                var virtualDynamicProperties = new List<PropertyInfo>();

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
                            if (lookupAttribute.List != changedListname) continue;
                            lookupProperties.Add(property);
                            modifyId = true;
                        }
                    }
                    if (modifyId) break;
                }
                if (modifyId)
                {
                    var collection = db.GetCollection(subType.Name);
                    var elements = collection.FindAll();
                    bool elementChanged = false;
                    foreach (var element in elements)
                    {
                        foreach (var lookupProperty in lookupProperties)
                        {
                            if ((int?)element[lookupProperty.Name] == oldId)
                            {
                                element[lookupProperty.Name] = newId;
                                elementChanged = true;
                            }
                        }
                        foreach (var virtualStaticPropery in virtualStaticProperties)
                        {
                            if ((int?)element[virtualStaticPropery.Name] == oldId)
                            {
                                element[virtualStaticPropery.Name] = newId;
                                elementChanged = true;
                            }
                        }
                        foreach (var virtualDynamicProperty in virtualDynamicProperties)
                        {
                            var attribute = virtualDynamicProperty.GetCustomAttribute<VirtualLookupAttribute>();
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

        public void GetChangesFromList<T>() where T : new()
        {
            return;
        }

        public void GetAllChanges()
        {
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

            _log.Info($"Working with queue ({queueCount} pending commands");
            Command command;
            int realCount = 0;
            while ((command = Read().Where(q => q.State == Command.States.Pending).OrderBy(q => q.Id).ToList().FirstOrDefault()) != null)
            {
                realCount++;
                _log.Debug($"storing command {command}");
                string commandAction = $"{command.Action}";
                try
                {
                    MethodInfo method = typeof(QueueCommandExecution).GetMethod(commandAction);
                    method.Invoke(execution, new object[] { command });
                    command.State = Command.States.Succeeded;
                    Update(command);
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
