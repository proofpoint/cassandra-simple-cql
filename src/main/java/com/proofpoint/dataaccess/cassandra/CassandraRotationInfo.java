package com.proofpoint.dataaccess.cassandra;

import java.util.ArrayList;
import java.util.List;


public class CassandraRotationInfo
{
    private final List<Integer> queryOrder;
    private final long timeRemaining;
    private final long timeElapsed;

    public List<Integer> getQueryTids()
    {
        return queryOrder;
    }

    public CassandraRotationInfo(List<Integer> q, long timeRemaining, long timeElapsed)
    {
        queryOrder = q;
        this.timeRemaining = timeRemaining;
        this.timeElapsed = timeElapsed;
    }

    public int getCurrTableIndex()
    {
        return queryOrder.get(0);
    }

    public int getPrevTableIndex()
    {
        return queryOrder.get(1);
    }

    public long getTimeRemainingInCurrent()
    {
        return timeRemaining;
    }

    public long getTimeElapsedInCurrent()
    {
        return timeElapsed;
    }

    public static CassandraRotationInfo getRotationInfo(long period, long numTables, long clockSkew)
    {
        long cassandraTimestamp = System.currentTimeMillis() + clockSkew;
        long runningPeriodIndex = cassandraTimestamp / period;
        long timeElapsed = cassandraTimestamp % period;

        List<Integer> queryOrder = new ArrayList<>();
        for (int n = 0; n < numTables - 1; n++) {
            int index = (int) ((runningPeriodIndex - n) % numTables);
            queryOrder.add(index);
        }

        return new CassandraRotationInfo(queryOrder, period - timeElapsed, timeElapsed);
    }
}
