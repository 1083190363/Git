package dimension.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * @ProjectName: git
 * @Package: dimension.base
 * @ClassName: PaymentTypeDimensionUDF
 * @Description: java类作用描述
 * @Author: CaoXueCheng
 * @CreateDate: 2018/11/20 14:01
 * @UpdateUser: CaoXueCheng
 * @UpdateDate: 2018/11/20 14:01
 * @Version: 1.0
 */
public class PaymentTypeDimension extends BaseDimension{
    private int id ;
    private String payment_type;

    public PaymentTypeDimension() {
    }

    public PaymentTypeDimension(String payment_type) {
        this.payment_type = payment_type;
    }

    @Override
    public void write(DataOutput out) throws IOException {
     out.writeInt(id);
     out.writeUTF(payment_type);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
    this.id = in.readInt();
    this.payment_type = in.readUTF();
    }
    @Override
    public int compareTo(BaseDimension o) {
        if (this == o){
            return 0;
        }
        PaymentTypeDimension other = (PaymentTypeDimension)o;
        int tmp = this.id - other.id;
        if (tmp !=0){
            return tmp;
        }
        return this.payment_type.compareTo(other.payment_type);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getPayment_type() {
        return payment_type;
    }

    public void setPayment_type(String payment_type) {
        this.payment_type = payment_type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PaymentTypeDimension that = (PaymentTypeDimension) o;
        return id == that.id &&
                Objects.equals(payment_type, that.payment_type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, payment_type);
    }

    @Override
    public String toString() {
        return "PaymentTypeDimension{" +
                "id=" + id +
                ", payment_type='" + payment_type + '\'' +
                '}';
    }
}
