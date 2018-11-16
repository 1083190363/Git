package dimension.base;

import common.GlobalConstants;
import org.apache.commons.lang.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @ProjectName: git
 * @Package: dimension.base
 * @ClassName: LocationDimension
 * @Description: 构建地域为维度的集合对象
 * @Author: CaoXueCheng
 * @CreateDate: 2018/11/16 15:03
 * @UpdateUser: CaoXueCheng
 * @UpdateDate: 2018/11/16 15:03
 * @Version: 1.0
 */
public class LocationDimension extends BaseDimension{
    private int id;
    private String country;
    private String province;
    private String city;

    public LocationDimension() {
    }

    public LocationDimension(int id, String country, String province, String city) {
        this.id = id;
        this.country = country;
        this.province = province;
        this.city = city;
    }

    public LocationDimension(String country, String province, String city) {
        this.country = country;
        this.province = province;
        this.city = city;
    }
    public static List<LocationDimension> buildList(String country, String province, String city){
        if (StringUtils.isEmpty(country)){
            country = province = city = GlobalConstants.DEFAULT_VALUE;
        }
        if (StringUtils.isEmpty(province)){
            province = city = GlobalConstants.DEFAULT_VALUE;
        }
        if (StringUtils.isEmpty(city)){
            city = GlobalConstants.DEFAULT_VALUE;
        }
        List<LocationDimension> list = new ArrayList<>();
        list.add(new LocationDimension(country,province,city));
        return list;
    }

    @Override
    public void write(DataOutput out) throws IOException {
     out.writeInt(this.id);
     out.writeUTF(this.country);
     out.writeUTF(this.province);
     out.writeUTF(this.city);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      this.id = in.readInt();
      this.country = in.readUTF();
      this.province = in.readUTF();
      this.city = in.readUTF();
    }
    @Override
    public int compareTo(BaseDimension o) {
        if (this == o){
            return 0;
        }
        LocationDimension other = (LocationDimension)o;
        int tmp = this.id - other.id;
        if (tmp !=0){
            return tmp;
        }
        tmp = this.country.compareTo(other.country);
        if (tmp !=0){
            return tmp;
        }
        tmp = this.province.compareTo(other.province);
        if (tmp!=0){
            return tmp;
        }
        tmp = this.city.compareTo(other.city);
        return tmp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LocationDimension that = (LocationDimension) o;
        return id == that.id &&
                Objects.equals(country, that.country) &&
                Objects.equals(province, that.province) &&
                Objects.equals(city, that.city);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, country, province, city);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }
}
