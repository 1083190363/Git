package value.reduce;


import common.KpiType;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.WritableUtils;
import value.StatsOutputValue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 用于reduce阶段输出的value的类型
 */
public class OutputValue extends StatsOutputValue {
    private KpiType kpi;
    private MapWritable value = new MapWritable();

    @Override
    public KpiType getKpi() {
        return kpi;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeEnum(dataOutput,kpi);
        value.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        kpi = WritableUtils.readEnum(dataInput,KpiType.class);
        value.readFields(dataInput);
    }
}
