package cn.ideal.hive.udf.agg;

import cn.ideal.bi.hll.HllUtil;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.stats.cardinality.HyperLogLog;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

@Description(name = "agg_bitmap", value = "_FUNC_(x) - Returns the bitmap")
public class MyHyperLogLogAggBitMap
        extends AbstractGenericUDAFResolver
{
    static final Logger LOG = LoggerFactory.getLogger(MyHyperLogLogAggBitMap.class.getName());

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException
    {
        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Exactly one argument is expected.");
        }

        if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0,
                    "Only primitive type arguments are accepted but "
                            + parameters[0].getTypeName() + " is passed.");
        }
        switch (((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory()) {
            case STRING:
            case VARCHAR:
                return new GenericUDAFAggBitmap();
            default:
                throw new UDFArgumentTypeException(0,
                        "Only string type arguments are accepted but "
                                + parameters[0].getTypeName() + " is passed.");
        }
    }

    public static class GenericUDAFAggBitmap
            extends GenericUDAFEvaluator
    {
        private BytesWritable result;
        private PrimitiveObjectInspector inputOI;
        protected ObjectInspector outputOI;
        //------------------------------
        private static final double maxStandardError = 0.023;  //该参数与presto默认一致 需要查询presto文档研究

        @Override
        public ObjectInspector init(Mode mode, ObjectInspector[] parameters)
                throws HiveException
        {
            assert (parameters.length == 1);
            super.init(mode, parameters);
            result = new BytesWritable();
            inputOI = (PrimitiveObjectInspector) parameters[0];
            outputOI = ObjectInspectorUtils.getStandardObjectInspector(inputOI,
                    ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA);
            return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer()
                throws HiveException
        {
            HyperLogLogBuffer agg = new SingleHyperLogLogBuffer();
            reset(agg);
            return agg;
        }

        @Override
        public void reset(AggregationBuffer agg)
                throws HiveException
        {
            final HyperLogLogBuffer state = ((HyperLogLogBuffer) agg);
            HyperLogLog hll = HllUtil.getNewHyperLogLog(maxStandardError);
            state.setHyperLogLog(hll);
            state.addMemoryUsage(hll.estimatedInMemorySize());
        }

        private boolean warned = false;

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters)
                throws HiveException
        {
            assert (parameters.length == 1);
            try {
                if (parameters[0] != null) {
                    final String iValue = PrimitiveObjectInspectorUtils.getString(parameters[0], inputOI);

                    HyperLogLogBuffer state = ((HyperLogLogBuffer) agg);
                    HyperLogLog hll = HllUtil.getOrCreateHyperLogLog(state, maxStandardError);
                    state.addMemoryUsage(-hll.estimatedInMemorySize());
                    hll.add(Slices.utf8Slice(iValue));
                    state.addMemoryUsage(hll.estimatedInMemorySize());
                }
            }
            catch (NumberFormatException e) {
                if (!warned) {
                    warned = true;
                    LOG.warn(getClass().getSimpleName() + " "
                            + StringUtils.stringifyException(e));
                    LOG.warn(getClass().getSimpleName()
                                    + " ignoring similar exceptions.");
                }
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg)
                throws HiveException
        {
            return terminate(agg);
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial)
                throws HiveException
        {
            if (partial != null) {
                BytesWritable bytesWritabl = PrimitiveObjectInspectorUtils.getBinary(partial, inputOI);
                byte[] p = Arrays.copyOfRange(bytesWritabl.getBytes(), 0, bytesWritabl.getLength());
                final Slice ortherHll = Slices.wrappedBuffer(p);
                try {
                    HyperLogLog input = HyperLogLog.newInstance(ortherHll);
                    HyperLogLogBuffer state = ((HyperLogLogBuffer) agg);
                    HyperLogLog previous = state.getHyperLogLog();
                    if (previous == null) {
                        state.setHyperLogLog(input);
                        state.addMemoryUsage(input.estimatedInMemorySize());
                    }
                    else {
                        state.addMemoryUsage(-previous.estimatedInMemorySize());
                        previous.mergeWith(input);
                        state.addMemoryUsage(previous.estimatedInMemorySize());
                    }
                }catch (Exception e){
                    LOG.error("merge 出错",e);
                    throw e;
                }
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg)
                throws HiveException
        {
            final HyperLogLog hll = ((SingleHyperLogLogBuffer) agg).getHyperLogLog();
            if (hll == null) {
                return null;
            }
            result.set(new BytesWritable(hll.serialize().getBytes()));
            return result;
        }
    }
}
