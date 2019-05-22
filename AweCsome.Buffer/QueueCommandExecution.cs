using AweCsome.Interfaces;
using System;
using System.Reflection;

namespace AweCsome.Buffer
{
    public class QueueCommandExecution
    {
        private LiteDbQueue _queue;
        private IAweCsomeTable _aweCsomeTable;
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
            _queue.CallGenericMethod(_aweCsomeTable, method, _baseType, command.FullyQualifiedName, null);
        }

        public void CreateTable(Command command)
        {
            MethodInfo method = _queue.GetMethod<IAweCsomeTable>(q => q.CreateTable<object>());
            _queue.CallGenericMethod(_aweCsomeTable, method, _baseType, command.FullyQualifiedName, null);
        }

        public void Insert(Command command)
        {
            object element = _queue.GetFromDbById(_baseType, command.FullyQualifiedName, command.ItemId.Value);
            MethodInfo method = _queue.GetMethod<IAweCsomeTable>(q => q.InsertItem<object>(element));
            int newId = (int)_queue.CallGenericMethod(_aweCsomeTable, method, _baseType, command.FullyQualifiedName, new object[] { element });
            _queue.UpdateId(_baseType, command.FullyQualifiedName, command.ItemId.Value, newId);
        }

        public void Update(Command command)
        {
            object element = _queue.GetFromDbById(_baseType, command.FullyQualifiedName, command.ItemId.Value);
            MethodInfo method = _queue.GetMethod<IAweCsomeTable>(q => q.UpdateItem(element));
            _queue.CallGenericMethod(_aweCsomeTable, method, _baseType, command.FullyQualifiedName, new object[] { element });
        }

        public void Empty(Command command)
        {
            MethodInfo method = _queue.GetMethod<IAweCsomeTable>(q => q.Empty<object>());
            _queue.CallGenericMethod(_aweCsomeTable, method, _baseType, command.FullyQualifiedName, null);
        }

        public void AttachFileToItem(Command command)
        {
            object element = _queue.GetFromDbById(_baseType, command.FullyQualifiedName, command.ItemId.Value);
            var attachmentStream = _queue.GetAttachmentStreamById((string)command.Parameters[0], out string filename, out BufferFileMeta meta);

            MethodInfo method = _queue.GetMethod<IAweCsomeTable>(q => q.AttachFileToItem<object>(command.ItemId.Value, filename, attachmentStream));
            _queue.CallGenericMethod(_aweCsomeTable, method, _baseType, command.FullyQualifiedName, new object[] { command.ItemId.Value, filename, attachmentStream });
        }

        public void RemoveAttachmentFromItem(Command command)
        {
            object element = _queue.GetFromDbById(_baseType, command.FullyQualifiedName, command.ItemId.Value);
            string filename = (string)command.Parameters[0];
            MethodInfo method = _queue.GetMethod<IAweCsomeTable>(q => q.DeleteFileFromItem<object>(command.ItemId.Value, filename));
            _queue.CallGenericMethod(_aweCsomeTable, method, _baseType, command.FullyQualifiedName, new object[] { command.ItemId.Value, filename });
        }

        public void AttachFileToLibrary(Command command)
        {
            object element = _queue.GetFromDbById(_baseType, command.FullyQualifiedName, command.ItemId.Value);
            var attachmentStream = _queue.GetAttachmentStreamById((string)command.Parameters[0], out string filename, out BufferFileMeta meta);
            string folder = meta.Folder;

            MethodInfo method = _queue.GetMethod<IAweCsomeTable>(q => q.AttachFileToLibrary<object>(folder, filename, attachmentStream, element));
            _queue.CallGenericMethod(_aweCsomeTable, method, _baseType, command.FullyQualifiedName, new object[] { folder, filename, attachmentStream, element });
        }

        public void RemoveFileFromLibrary(Command command)
        {
            object element = _queue.GetFromDbById(_baseType, command.FullyQualifiedName, command.ItemId.Value);
            var attachmentStream = _queue.GetAttachmentStreamById((string)command.Parameters[0], out string filename, out BufferFileMeta meta);
            string folder = meta.Folder;

            MethodInfo method = _queue.GetMethod<IAweCsomeTable>(q => q.DeleteFilesFromDocumentLibrary<object>(folder, new System.Collections.Generic.List<string> { filename }));
            _queue.CallGenericMethod(_aweCsomeTable, method, _baseType, command.FullyQualifiedName, new object[] { folder, new System.Collections.Generic.List<string> { filename } });
        }
    }
}
