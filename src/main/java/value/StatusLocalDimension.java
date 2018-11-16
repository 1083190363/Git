package value;

/**
 * @ProjectName: git
 * @Package: value
 * @ClassName: StatsUserDimension
 * @Description: 封装地域维度模块的map和reduce阶段的key类型
 * @Author: CaoXueCheng
 * @CreateDate: 2018/11/3 20:58
 * @UpdateUser: CaoXueCheng
 * @UpdateDate: 2018/11/3 20:58
 * @Version: 1.0
 */

import dimension.base.BaseDimension;
import dimension.base.LocationDimension;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * 可以用于地域维度模块的map和reduce阶段的疏忽的key的类型
 */
public class StatusLocalDimension extends StatsBaseDimension{
    private StatsCommonDimension statsCommonDimension = new StatsCommonDimension();
    private LocationDimension localDimension = new LocationDimension();

    public StatusLocalDimension() {

    }
    public StatusLocalDimension(StatsCommonDimension statsCommonDimension, LocationDimension localDimension) {
        this.statsCommonDimension = statsCommonDimension;
        this.localDimension = localDimension;
    }
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.statsCommonDimension.write(dataOutput);
        this.localDimension.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.statsCommonDimension.readFields(dataInput);
        this.localDimension.readFields(dataInput);
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (this == o){
            return 0;
        }

        StatusLocalDimension other = (StatusLocalDimension) o;
        int tmp = this.statsCommonDimension.compareTo(other.statsCommonDimension);
        if (tmp != 0){
            return tmp;
        }
        return this.localDimension.compareTo(other.localDimension);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StatusLocalDimension that = (StatusLocalDimension) o;
        return Objects.equals(statsCommonDimension, that.statsCommonDimension) &&
                Objects.equals(localDimension, that.localDimension);
    }

    @Override
    public int hashCode() {
        return Objects.hash(statsCommonDimension, localDimension);
    }

    public StatsCommonDimension getStatsCommonDimension() {
        return statsCommonDimension;
    }

    public void setStatsCommonDimension(StatsCommonDimension statsCommonDimension) {
        this.statsCommonDimension = statsCommonDimension;
    }

    public LocationDimension getLocalDimension() {
        return localDimension;
    }

    public void setLocalDimension(LocationDimension localDimension) {
        this.localDimension = localDimension;
    }
}
