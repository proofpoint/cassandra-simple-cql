package com.proofpoint.dataaccess.cassandra;

import com.datastax.driver.core.ConsistencyLevel;

public interface SimpleCqlStatement<T>
{
    T withConsistencyLevel(ConsistencyLevel consistencyLevel);

    T withSerialConsistencyLevel(ConsistencyLevel consistencyLevel);
}
