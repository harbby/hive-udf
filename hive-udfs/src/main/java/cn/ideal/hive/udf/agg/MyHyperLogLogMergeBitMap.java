package cn.ideal.hive.udf.agg;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * 近似去重函数  HyperLogLog算法去重
 * 此例子 因为preto已经内置了  仅用做测试
 * */
@Description(name = "merge_bitmap", value = "_FUNC_(x) - Returns the bitmap"
        , extended = "自定义近似去重函数bitmap merge")
public final class MyHyperLogLogMergeBitMap extends AbstractGenericUDAFResolver
{
    static final Logger LOG = LoggerFactory.getLogger(MyHyperLogLogMergeBitMap.class.getName());

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
            extends MyHyperLogLogAggBitMap.GenericUDAFAggBitmap{
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

    }

}
