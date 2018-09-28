package com.proofpoint.dataaccess.cassandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.google.common.reflect.TypeToken;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public interface SimpleCqlMapper<T>
{
    BoundStatement bind();

    /**
     * Execute synchronously.
     *
     * @return DataStax driver's ResultSet for streaming.
     */
    ResultSet execute();

    CompletableFuture<ResultSet> executeAsync();

    CompletableFuture<Collection<T>> executeAsyncAndMap();

    CompletableFuture<Optional<T>> executeAsyncAndMapOne();

    CompletableFuture<Optional<T>> executeAsyncAndMapAtMostOne();

    Object[] getParamValues();

    default Class<? super T> getTypeParameterClass()
    {
        return new TypeToken<T>(getClass())
        {
        }.getRawType();
    }
}
