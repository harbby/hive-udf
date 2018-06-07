package cn.ideal.bi.hll;

import com.google.common.annotations.VisibleForTesting;
import io.airlift.stats.cardinality.HyperLogLog;

public final class HllUtil
{
    private HllUtil(){}

    //----------重复代码块---------------
    //--------------来自presto-----------
    private static final double DEFAULT_STANDARD_ERROR = 0.023;
    private static final double LOWEST_MAX_STANDARD_ERROR = 0.01150;
    private static final double HIGHEST_MAX_STANDARD_ERROR = 0.26000;
    private static final double maxStandardError = 0.023;  //该参数与presto默认一致 需要查询presto文档研究

    public static HyperLogLog getOrCreateHyperLogLog(HyperLogLogState state, double maxStandardError)
    {
        HyperLogLog hll = state.getHyperLogLog();
        if (hll == null) {
            hll = HyperLogLog.newInstance(standardErrorToBuckets(maxStandardError));
            state.setHyperLogLog(hll);
            state.addMemoryUsage(hll.estimatedInMemorySize());
        }
        return hll;
    }

    public static HyperLogLog getNewHyperLogLog(final double maxStandardError){
        return HyperLogLog.newInstance(standardErrorToBuckets(maxStandardError));
    }

    @VisibleForTesting
    private static int standardErrorToBuckets(double maxStandardError)
    {
        checkCondition(maxStandardError >= LOWEST_MAX_STANDARD_ERROR && maxStandardError <= HIGHEST_MAX_STANDARD_ERROR,
                "Max standard error must be in [%s, %s]: %s", LOWEST_MAX_STANDARD_ERROR, HIGHEST_MAX_STANDARD_ERROR, maxStandardError);
        return log2Ceiling((int) Math.ceil(1.0816 / (maxStandardError * maxStandardError)));
    }

    private static int log2Ceiling(int value)
    {
        return Integer.highestOneBit(value - 1) << 1;
    }

    public static void checkCondition(boolean condition, String formatString, Object... args)
    {
        if (!condition) {
            throw new RuntimeException(String.format(formatString, args));
        }
    }
}
