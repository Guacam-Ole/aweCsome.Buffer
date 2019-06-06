using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace AweCsome.Buffer.Interfaces
{
    public interface ILiteDbQueue
    {
        void AddCommand<T>(Command command);
        List<Command> Read();
        void Update(Command command);


        object GetFromDbById(Type baseType, string fullyQualifiedName, int id);
        void UpdateId(Type baseType, string fullyQualifiedName, int oldId, int newId);
    
        void Sync(Type baseType);
    }
}
