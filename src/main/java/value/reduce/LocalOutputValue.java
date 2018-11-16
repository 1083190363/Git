package value.reduce;


import common.KpiType;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.WritableUtils;
import value.StatsOutputValue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * 地域维度用于reduce阶段输出的value的类型
 */
public class LocalOutputValue extends StatsOutputValue {
    private int aus;//活跃用户数
    private int sessions;
    private int boundsessions;//跳出会话个数
    private MapWritable value = new MapWritable();
    private KpiType kpi;

    public void setKpi(KpiType kpi) {
        this.kpi = kpi;
    }
    @Override
    public KpiType getKpi() {
        return kpi;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(aus);
        out.writeInt(sessions);
        out.writeInt(boundsessions);
        WritableUtils.writeEnum(out,kpi);
        value.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.aus = in.readInt();
        this.sessions = in.readInt();
        this.boundsessions = in.readInt();
        WritableUtils.readEnum(in,KpiType.class);
        value.readFields(in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LocalOutputValue that = (LocalOutputValue) o;
        return aus == that.aus &&
                sessions == that.sessions &&
                boundsessions == that.boundsessions &&
                kpi == that.kpi;
    }

    @Override
    public int hashCode() {
        return Objects.hash(aus, sessions, boundsessions, kpi);
    }

    public int getAus() {
        return aus;
    }

    public void setAus(int aus) {
        this.aus = aus;
    }

    public int getSessions() {
        return sessions;
    }

    public void setSessions(int sessions) {
        this.sessions = sessions;
    }

    public int getBoundsessions() {
        return boundsessions;
    }

    public void setBoundsessions(int boundsessions) {
        this.boundsessions = boundsessions;
    }

    public MapWritable getValue() {
        return value;
    }

    public void setValue(MapWritable value) {
        this.value = value;
    }
}
