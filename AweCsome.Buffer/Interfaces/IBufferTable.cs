﻿using System;

namespace AweCsome.Buffer.Interfaces
{
    public interface IBufferTable
    {
        void ReadAllLists(Type baseType, string forbiddenNamespace = null);
        void ReadAllFromList<T>() where T : new();
        void ReadAllFromList(Type entityType);
        void GetChangesFromAllLists(Type baseType);
        void EmptyStorage();
    }
}
