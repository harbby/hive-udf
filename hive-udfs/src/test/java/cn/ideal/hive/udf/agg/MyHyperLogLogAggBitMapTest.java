package cn.ideal.hive.udf.agg;

import com.google.common.annotations.VisibleForTesting;
import cn.ideal.bi.hll.HyperLogLogState;
import io.airlift.slice.Slices;
import io.airlift.stats.cardinality.HyperLogLog;
import junit.framework.TestCase;
import org.junit.Test;

public class MyHyperLogLogAggBitMapTest
        extends TestCase
{


    @Test
    public void test1(){
        Class clazz = shaded.com.google.common.base.Preconditions.class;
        System.out.println(clazz.getResource(clazz.getSimpleName() + ".class")); //判断该类来自哪个jar包

        HyperLogLog hll = HyperLogLog.newInstance(standardErrorToBuckets(maxStandardError));
        hll.add(Slices.utf8Slice("test_001"));
        hll.add(Slices.utf8Slice("test_001"));
        hll.add(Slices.utf8Slice("test_002"));
        System.out.println("num:"+hll.cardinality());

        byte[] a1 = hll.serialize().getBytes();
        HyperLogLog a2 = HyperLogLog.newInstance(Slices.wrappedBuffer(a1));

        System.out.println("num:"+a2.cardinality());
    }


    //----------重复代码块---------------
    //--------------来自presto-----------
    private static final double DEFAULT_STANDARD_ERROR = 0.023;
    private static final double LOWEST_MAX_STANDARD_ERROR = 0.01150;
    private static final double HIGHEST_MAX_STANDARD_ERROR = 0.26000;
    private static final double maxStandardError = 0.023;  //该参数与presto默认一致 需要查询presto文档研究

    private static HyperLogLog getOrCreateHyperLogLog(HyperLogLogState state, double maxStandardError)
    {
        HyperLogLog hll = state.getHyperLogLog();
        if (hll == null) {
            hll = HyperLogLog.newInstance(standardErrorToBuckets(maxStandardError));
            state.setHyperLogLog(hll);
            state.addMemoryUsage(hll.estimatedInMemorySize());
        }
        return hll;
    }

    @VisibleForTesting
    static int standardErrorToBuckets(double maxStandardError)
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