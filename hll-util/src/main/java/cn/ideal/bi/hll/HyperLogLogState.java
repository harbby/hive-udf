package cn.ideal.bi.hll;

import io.airlift.stats.cardinality.HyperLogLog;

import javax.validation.constraints.NotNull;

public interface HyperLogLogState
{
    int NUMBER_OF_BUCKETS = 4096;

    @NotNull
    public HyperLogLog getHyperLogLog();

    public void setHyperLogLog(HyperLogLog var1);

    public void addMemoryUsage(int var1);
}
