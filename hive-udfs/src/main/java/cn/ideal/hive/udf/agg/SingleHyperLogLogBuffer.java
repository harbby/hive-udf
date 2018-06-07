package cn.ideal.hive.udf.agg;

import cn.ideal.bi.hll.HyperLogLogState;
import cn.ideal.bi.hll.SingleHyperLogLogState;
import io.airlift.stats.cardinality.HyperLogLog;

public class SingleHyperLogLogBuffer extends HyperLogLogBuffer
{

    private final HyperLogLogState agg = new SingleHyperLogLogState();

    @Override
    public HyperLogLog getHyperLogLog()
    {
        return agg.getHyperLogLog();
    }

    @Override
    public void setHyperLogLog(HyperLogLog var1)
    {
        agg.setHyperLogLog(var1);
    }

    @Override
    public void addMemoryUsage(int var1)
    {
        agg.addMemoryUsage(var1);
    }

}
