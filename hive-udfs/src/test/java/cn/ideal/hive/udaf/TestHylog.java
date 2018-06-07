package cn.ideal.hive.udaf;

import cn.ideal.hive.udf.agg.MyHyperLogLogAggBitMap;
import cn.ideal.bi.hll.HllUtil;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.stats.cardinality.HyperLogLog;
import junit.framework.Assert;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.ISupportStreamingModeForWindowing;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class TestHylog
{
    public static <T, TW> void _agg(GenericUDAFResolver fnR,
            TypeInfo[] inputTypes, Iterator<T> inVals,
            TestStreamingSum.TypeHandler<T, TW> typeHandler, TW[] in, ObjectInspector[] inputOIs,
            int inSz, int numPreceding, int numFollowing, Iterator<T> outVals)
            throws HiveException
    {

        GenericUDAFEvaluator fn = fnR.getEvaluator(inputTypes);
        fn.init(GenericUDAFEvaluator.Mode.FINAL, inputOIs);
        GenericUDAFEvaluator.AggregationBuffer agg = fn.getNewAggregationBuffer();

        int outSz = 0;
        while (inVals.hasNext()) {
            typeHandler.set(inVals.next(), in[0]);
            fn.aggregate(agg, in);

            Object out = fn.terminate(agg);
            if (out != null) {
                if ( out == ISupportStreamingModeForWindowing.NULL_RESULT ) {
                    out = null;
                } else {
                    try {
                        out = typeHandler.get((TW) out);
                    } catch(ClassCastException ce) {
                    }
                }
                Object a1 = outVals.next();
                Assert.assertEquals(out, a1);
                outSz++;
            }
        }

        fn.terminate(agg);

        while (outSz < inSz) {
            Object out = fn.terminate(agg);
            if ( out == ISupportStreamingModeForWindowing.NULL_RESULT ) {
                out = null;
            } else {
                try {
                    out = typeHandler.get((TW) out);
                } catch(ClassCastException ce) {
                }
            }
            Assert.assertEquals(out, outVals.next());
            outSz++;
        }

    }

    public void maxLong(Iterator<Slice> inVals, int inSz, int numPreceding,
            int numFollowing, Iterator<Slice> outVals) throws HiveException {

        MyHyperLogLogAggBitMap fnR = new MyHyperLogLogAggBitMap();
        TypeInfo[] inputTypes = { TypeInfoFactory.stringTypeInfo };
        ObjectInspector[] inputOIs = { PrimitiveObjectInspectorFactory.writableBinaryObjectInspector };

        BytesWritable[] in = new BytesWritable[1];
        in[0] = new BytesWritable();

        _agg(fnR, inputTypes, inVals, StringHandler, in,
                inputOIs, inSz, numPreceding, numFollowing, outVals);

    }

    TestStreamingSum.TypeHandler<Slice, BytesWritable> StringHandler = new TestStreamingSum.TypeHandler<Slice, BytesWritable>() {
        public void set(Slice d, BytesWritable iw) {
            //BytesWritable binary =  new BytesWritable(d.getBytes());
            iw.set(d.getBytes(),0,d.length());
            //iw.set(binary);
            System.out.println(iw.getLength());
        }

        public Slice get(BytesWritable iw) {
            return Slices.wrappedBuffer(iw.getBytes());
        }
    };

    private static final double maxStandardError = 0.023;  //该参数与presto默认一致 需要查询presto文档研究
    @Test
    public void testHylog_3_4() throws HiveException
    {

        HyperLogLog hll = HllUtil.getNewHyperLogLog(maxStandardError);
        hll.add(Slices.utf8Slice("test_001"));
        hll.add(Slices.utf8Slice("test_001"));
        hll.add(Slices.utf8Slice("test_002"));

        final Slice slice = hll.serialize();

        List<Slice> inVals = Arrays.asList(slice);
        List<Slice> outVals = Arrays.asList(Slices.utf8Slice("a1"));
        maxLong(inVals.iterator(), 10, 3, 0, outVals.iterator());
    }

}
