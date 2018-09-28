package com.proofpoint.dataaccess.cassandra;

import java.util.function.BiFunction;

public interface SimpleCqlPrettyPrintable
{
    /**
     * equivalent of running Object#toString on the object, but passing the custom formatter for SimpleCqlPrettyPrint#custom fields.
     */
    <E> String prettyPrint(BiFunction<E, String, String> prettyPrinter);
}
