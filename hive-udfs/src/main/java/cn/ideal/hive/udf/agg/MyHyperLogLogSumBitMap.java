package cn.ideal.hive.udf.agg;

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
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 此函数用来做首次 预计算  bitmap位图(byte[])  union agg
 * 主要是预先按相应维度 做一次聚合 聚合出位图 HyperLogLog算法的位图
 * 输入bitmap 聚会成uv
 **/
@Description(name = "sum_bitmap", value = "_FUNC_(x) - Returns the long"
        , extended = "自定义近似去重函数bitmap sum->long")
public final class MyHyperLogLogSumBitMap
        extends AbstractGenericUDAFResolver
{
    static final Logger LOG = LoggerFactory.getLogger(MyHyperLogLogSumBitMap.class.getName());

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
            case BINARY:
                return new GenericUDAFSumBitmap();
            default:
                throw new UDFArgumentTypeException(0,
                        "Only string type arguments are accepted but "
                                + parameters[0].getTypeName() + " is passed.");
        }
    }

    public static class GenericUDAFSumBitmap
            extends MyHyperLogLogAggBitMap.GenericUDAFAggBitmap
    {
        private LongWritable result;
        private PrimitiveObjectInspector inputOI;
        protected ObjectInspector outputOI;

        @Override
        public ObjectInspector init(Mode mode, ObjectInspector[] parameters)
                throws HiveException
        {
            assert (parameters.length == 1);
            super.init(mode, parameters);
            result = new LongWritable(0);
            inputOI = (PrimitiveObjectInspector) parameters[0];
            outputOI = ObjectInspectorUtils.getStandardObjectInspector(inputOI,
                    ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA);

            if(mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2){
                return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
            }else {
                return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
            }
        }

        private boolean warned = false;

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters)
                throws HiveException
        {
            assert (parameters.length == 1);
            try {
                merge(agg, parameters[0]);
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
            return super.terminate(agg);
        }

        @Override
        public Object terminate(AggregationBuffer agg)
                throws HiveException
        {
            final HyperLogLog hll = ((SingleHyperLogLogBuffer) agg).getHyperLogLog();
            if (hll == null) {
                return null;
            }
            result.set(hll.cardinality());
            return result;
        }
    }
}
