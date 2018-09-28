package com.proofpoint.dataaccess.cassandra;

public interface SimpleCqlProvider<T>
{
    T get();
}
