package cn.ideal.hive.udf.agg;

import cn.ideal.bi.hll.HyperLogLogState;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;

public abstract class HyperLogLogBuffer extends GenericUDAFEvaluator.AbstractAggregationBuffer implements HyperLogLogState
{
}
