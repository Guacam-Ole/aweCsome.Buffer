using AweCsome.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

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
            MethodInfo method =_queue.GetMethod<IAweCsomeTable>(q => q.CreateTable<object>());
            _queue.CallGenericMethod(_aweCsomeTable, method, _baseType, command.FullyQualifiedName, null);
        }

        public void Insert(Command command)
        {
            object insertData = _queue.GetFromDbById(_baseType, command.FullyQualifiedName, command.ItemId.Value);
            MethodInfo method = _queue.GetMethod<IAweCsomeTable>(q => q.InsertItem<object>(insertData));
            int newId = (int)_queue.CallGenericMethod(_aweCsomeTable, method, _baseType, command.FullyQualifiedName, new object[] { insertData });
            _queue.UpdateId(_baseType, command.FullyQualifiedName, command.ItemId.Value, newId);
        }

        public void Update(Command command)
        {

            throw new NotImplementedException();
        }

        public void SendMail(Command command)
        {
            throw new NotImplementedException();
        }

        public void Empty(Command command)
        {
            MethodInfo method = _queue.GetMethod<IAweCsomeTable>(q => q.Empty<object>());
            _queue.CallGenericMethod(_aweCsomeTable, method, _baseType, command.FullyQualifiedName, null);
        }

        public void UploadAttachment(Command command)
        {
            throw new NotImplementedException();
        }

        public void RemoveAttachment(Command command)
        {
            throw new NotImplementedException();
        }

        public void UploadFile(Command command)
        {
            throw new NotImplementedException();
        }

        public void RemoveFile(Command command)
        {
            throw new NotImplementedException();
        }
    }
}
