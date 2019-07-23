using AweCsome.Buffer.Entities;
using System;
using System.Collections.Generic;

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
        void Empty();
        void Delete(Command command);
    }
}
