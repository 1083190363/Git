package value.map;

import common.KpiType;
import org.apache.hadoop.hbase.mapreduce.HashTable;
import value.StatsOutputValue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ProjectName: git
 * @Package: value.map
 * @ClassName: TimeOutputValue
 * @Description: java类作用描述
 * @Author: CaoXueCheng
 * @CreateDate: 2018/11/3 20:59
 * @UpdateUser: CaoXueCheng
 * @UpdateDate: 2018/11/3 20:59
 * @Version: 1.0
 */
public class TimeOutputValue extends StatsOutputValue {
    private String id; //对id的泛指，可以是uuid，可以是umid，还可以是sessionId
    private long time; //时间戳

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(id);
        dataOutput.writeLong(time);
    }
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        id = dataInput.readUTF();
        time = dataInput.readLong();
    }

    @Override
    public KpiType getKpi() {
        return null;
    }
}
