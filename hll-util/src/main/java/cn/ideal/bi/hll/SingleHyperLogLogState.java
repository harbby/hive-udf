package cn.ideal.bi.hll;

import io.airlift.stats.cardinality.HyperLogLog;
import org.openjdk.jol.info.ClassLayout;

public class SingleHyperLogLogState
        implements HyperLogLogState
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SingleHyperLogLogState.class).instanceSize();
    private HyperLogLog hll;

    @Override
    public HyperLogLog getHyperLogLog()
    {
        return hll;
    }

    @Override
    public void setHyperLogLog(HyperLogLog value)
    {
        hll = value;
    }

    @Override
    public void addMemoryUsage(int value)
    {
        // noop
    }

    public long getEstimatedSize()
    {
        long estimatedSize = INSTANCE_SIZE;
        if (hll != null) {
            estimatedSize += hll.estimatedInMemorySize();
        }
        return estimatedSize;
    }
}
