using AweCsome.Buffer.Entities;
using AweCsome.Interfaces;
using Newtonsoft.Json;
using System;
using System.IO;
using System.Linq;
using System.Reflection;

namespace AweCsome.Buffer
{
    public class QueueCommandExecution
    {
        private LiteDbQueue _queue;
        //private LiteDb _db;
        private readonly IAweCsomeTable _aweCsomeTable;
        private Type _baseType;
        public QueueCommandExecution(LiteDbQueue queue, IAweCsomeTable awecsomeTable, Type baseType)
        {
            _queue = queue;
            _aweCsomeTable = awecsomeTable;
            _baseType = baseType;
        }

        public void DeleteTable(Command command)
        {
            MethodInfo method = _queue.GetMethod<IAweCsomeTable>(q => q.DeleteTable<object>());
            _queue.CallGenericMethodByName(_aweCsomeTable, method, _baseType, command.FullyQualifiedName, null);
        }

        public void CreateTable(Command command)
        {
            MethodInfo method = _queue.GetMethod<IAweCsomeTable>(q => q.CreateTable<object>());
            _queue.CallGenericMethodByName(_aweCsomeTable, method, _baseType, command.FullyQualifiedName, null);
        }

        public void Insert(Command command)
        {
            object element = _queue.GetFromDbById(_baseType, command.FullyQualifiedName, command.ItemId.Value);
            MethodInfo method = _queue.GetMethod<IAweCsomeTable>(q => q.InsertItem<object>(element));
            int newId = (int)_queue.CallGenericMethodByName(_aweCsomeTable, method, _baseType, command.FullyQualifiedName, new object[] { element });
            _queue.UpdateId(_baseType, command.FullyQualifiedName, command.ItemId.Value, newId);
        }

        public void Delete(Command command)
        {
            MethodInfo method = _queue.GetMethod<IAweCsomeTable>(q => q.DeleteItemById<object>(command.ItemId.Value));
            _queue.CallGenericMethodByName(_aweCsomeTable, method, _baseType, command.FullyQualifiedName, new object[] { command.ItemId.Value });
        }

        public void Update(Command command)
        {
            object element = _queue.GetFromDbById(_baseType, command.FullyQualifiedName, command.ItemId.Value);
            MethodInfo method = _queue.GetMethod<IAweCsomeTable>(q => q.UpdateItem(element));
            _queue.CallGenericMethodByName(_aweCsomeTable, method, _baseType, command.FullyQualifiedName, new object[] { element });
        }
        public void Like(Command command)
        {
            object element = _queue.GetFromDbById(_baseType, command.FullyQualifiedName, command.ItemId.Value);
            MethodInfo method = _queue.GetMethod<IAweCsomeTable>(q => q.Like<object>(0,0));
            _queue.CallGenericMethodByName(_aweCsomeTable, method, _baseType, command.FullyQualifiedName, new object[] { command.ItemId.Value, (int)command.Parameters.First().Value });
        }

        public void Unlike(Command  command)
        {
            object element = _queue.GetFromDbById(_baseType, command.FullyQualifiedName, command.ItemId.Value);
            MethodInfo method = _queue.GetMethod<IAweCsomeTable>(q => q.Unlike<object>(0, 0));
            _queue.CallGenericMethodByName(_aweCsomeTable, method, _baseType, command.FullyQualifiedName, new object[] { command.ItemId.Value, (int)command.Parameters.First().Value });
        }

        public void Empty(Command command)
        {
            MethodInfo method = _queue.GetMethod<IAweCsomeTable>(q => q.Empty<object>());
            _queue.CallGenericMethodByName(_aweCsomeTable, method, _baseType, command.FullyQualifiedName, null);
        }

        public void AttachFileToItem(Command command)
        {
            object element = _queue.GetFromDbById(_baseType, command.FullyQualifiedName, command.ItemId.Value);
            var attachmentStream = _queue.GetAttachmentStreamFromDbById((string)command.Parameters["AttachmentId"], out string filename, out BufferFileMeta meta);
            attachmentStream.Seek(0, SeekOrigin.Begin);

            MethodInfo method = _queue.GetMethod<IAweCsomeTable>(q => q.AttachFileToItem<object>(command.ItemId.Value, filename, new MemoryStream()));
            _queue.CallGenericMethodByName(_aweCsomeTable, method, _baseType, command.FullyQualifiedName, new object[] { command.ItemId.Value, filename, attachmentStream });
            _queue.DeleteAttachmentFromDbWithoutSyncing(meta);
        }

        public void RemoveAttachmentFromItem(Command command)
        {
            object element = _queue.GetFromDbById(_baseType, command.FullyQualifiedName, command.ItemId.Value);
            string filename = (string)command.Parameters["Filename"];
            MethodInfo method = _queue.GetMethod<IAweCsomeTable>(q => q.DeleteFileFromItem<object>(command.ItemId.Value, filename));
            _queue.CallGenericMethodByName(_aweCsomeTable, method, _baseType, command.FullyQualifiedName, new object[] { command.ItemId.Value, filename });
        }

        public void AttachFileToLibrary(Command command)
        {
            var attachmentStream = _queue.GetAttachmentStreamFromDbById((string)command.Parameters["AttachmentId"], out string filename, out BufferFileMeta meta);
            string folder = meta.Folder;
            object element = null;
            if (!string.IsNullOrEmpty(meta.AdditionalInformation))
            {
                Type targetType = _baseType.Assembly.GetType(command.FullyQualifiedName);
                element = JsonConvert.DeserializeObject(meta.AdditionalInformation, targetType);
            }

            using (var saveStream = new MemoryStream(attachmentStream.ToArray()))
            {
                MethodInfo method = _queue.GetMethod<IAweCsomeTable>(q => q.AttachFileToLibrary(folder, filename, saveStream, element));
                _queue.CallGenericMethodByName(_aweCsomeTable, method, _baseType, command.FullyQualifiedName, new object[] { folder, filename, saveStream, element });
            }
            _queue.DeleteAttachmentFromDbWithoutSyncing(meta);
        }

        public void RemoveFileFromLibrary(Command command)
        {
            object element = _queue.GetFromDbById(_baseType, command.FullyQualifiedName, command.ItemId.Value);
            string folder = (string)command.Parameters["Folder"];
            string filename = (string)command.Parameters["Filename"];

            MethodInfo method = _queue.GetMethod<IAweCsomeTable>(q => q.DeleteFilesFromDocumentLibrary<object>(folder, new System.Collections.Generic.List<string> { filename }));
            _queue.CallGenericMethodByName(_aweCsomeTable, method, _baseType, command.FullyQualifiedName, new object[] { folder, new System.Collections.Generic.List<string> { filename } });
        }
    }
}
