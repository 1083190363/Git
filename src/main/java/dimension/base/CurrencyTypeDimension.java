package dimension.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * @ProjectName: git
 * @Package: dimension.base
 * @ClassName: CurrencyTypeDimension
 * @Description: java类作用描述
 * @Author: CaoXueCheng
 * @CreateDate: 2018/11/20 14:01
 * @UpdateUser: CaoXueCheng
 * @UpdateDate: 2018/11/20 14:01
 * @Version: 1.0
 */
public class CurrencyTypeDimension extends BaseDimension{
    private int id ;
    private String currency_name;

    public CurrencyTypeDimension() {
    }

    public CurrencyTypeDimension(String currency_name) {
        this.currency_name = currency_name;
    }

    @Override
    public void write(DataOutput out) throws IOException {
    out.writeInt(id);
    out.writeUTF(currency_name);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
    this.id = in.readInt();
    this.currency_name = in.readUTF();
    }
    @Override
    public int compareTo(BaseDimension o) {
        if (this == o){
            return 0;
        }
        CurrencyTypeDimension other = (CurrencyTypeDimension)o;
        int tmp = this.id - other.id;
        if (tmp !=0){
            return tmp;
        }
        return this.currency_name.compareTo(other.currency_name);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getCurrency_name() {
        return currency_name;
    }

    public void setCurrency_name(String currency_name) {
        this.currency_name = currency_name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CurrencyTypeDimension that = (CurrencyTypeDimension) o;
        return id == that.id &&
                Objects.equals(currency_name, that.currency_name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, currency_name);
    }

    @Override
    public String toString() {
        return "CurrencyTypeDimension{" +
                "id=" + id +
                ", currency_name='" + currency_name + '\'' +
                '}';
    }
}
