package value.map;



import common.KpiType;
import value.StatsOutputValue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
/**
* @todo  用户模块和浏览器模块map阶段的value输出类型
* @author CaoXueCheng
* @date 2018/11/7 15:16
* @method
* @param
* @return
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

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }
}
