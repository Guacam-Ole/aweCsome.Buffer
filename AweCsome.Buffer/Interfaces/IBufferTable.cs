using System;

namespace AweCsome.Buffer.Interfaces
{
    public interface IBufferTable
    {
        void ReadAllLists(Type baseType);
        void ReadAllFromList<T>() where T : new();
        void ReadAllFromList(Type entityType);
    }
}
