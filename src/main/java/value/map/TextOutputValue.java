package value.map;



import common.KpiType;
import value.StatsOutputValue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
* @todo  地域维度map阶段的value输出类型
* @author CaoXueCheng
* @date 2018/11/7 15:16
* @method
* @param
* @return
*/

public class TextOutputValue extends StatsOutputValue {
    private String uuid; //对id的泛指，可以是uuid，可以是umid，还可以是sessionId
    private String sessionid; //sessionID

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(uuid);
        dataOutput.writeUTF(sessionid);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        uuid = dataInput.readUTF();
        sessionid = dataInput.readUTF();
    }

    @Override
    public KpiType getKpi() {
        return null;
    }

    public String getId() {
        return uuid;
    }

    public void setId(String id) {
        this.uuid = id;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getSessionid() {
        return sessionid;
    }

    public void setSessionid(String sessionid) {
        this.sessionid = sessionid;
    }
}
