package cn.ideal.hive.udf.scalar;

import io.airlift.slice.Slices;
import io.airlift.stats.cardinality.HyperLogLog;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

@Description(name = "value_bitmap",
        value = "_FUNC_(bin) - Convert the argument from binary to a long")
public class MyBitMapGetValue extends UDF
{
    public long evaluate(BytesWritable b){
        if (b == null) {
            return 0L;
        }
        byte[] bytes = new byte[b.getLength()];
        System.arraycopy(b.getBytes(), 0, bytes, 0, b.getLength());
        return HyperLogLog.newInstance(Slices.wrappedBuffer(bytes)).cardinality();
    }
}
