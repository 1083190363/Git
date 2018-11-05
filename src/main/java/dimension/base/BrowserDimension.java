package dimension.base;



import common.GlobalConstants;
import org.apache.commons.lang.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * @ProjectName: git
 * @Package: dimension
 * @ClassName: BrowserDimension
 * @Description: java类作用描述
 * @Author: 曹学成
 * @CreateDate: 2018/11/3 16:44
 * @UpdateUser: 曹学成
 * @UpdateDate: 2018/11/3 16:44
 * @Version: 1.0
 */
public class BrowserDimension extends BaseDimension{
    private int id;
    private String browerName;
    private String browerVersion;

    public BrowserDimension() {

    }
    public BrowserDimension(String browerName, String browerVersion) {
        this.browerName = browerName;
        this.browerVersion = browerVersion;
    }
    public BrowserDimension(int id, String browerName, String browerVersion) {
        this(browerName,browerVersion);
        this.id = id;

    }
    public static BrowserDimension getInstance(String browerName,String browerVersion){
        /**
        * @todo 构建浏览器类型的集合对象
        * @author CaoXueCheng
        * @date 2018/11/3 19:14
        * @method getInstance
        * @param []
        * @return dimension.base.BrowserDimension
        */
        if (StringUtils.isEmpty(browerName)){
            browerName = browerVersion = GlobalConstants.DEFAULT_VALUE;
        }
        if (StringUtils.isEmpty(browerVersion)){
            browerVersion = GlobalConstants.DEFAULT_VALUE;
        }
      return new BrowserDimension(browerName,browerVersion);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(id);
        dataOutput.writeUTF(browerName);
        dataOutput.writeUTF(browerVersion);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        id = dataInput.readInt();
        browerName = dataInput.readUTF();
        browerVersion = dataInput.readUTF();
    }
    @Override
    public int compareTo(BaseDimension o) {
     if (this == o){
         return  0;
     }
     BrowserDimension other = (BrowserDimension)o;
     int tmp = this.id - other.id;
     if (tmp != 0){
         return tmp;
     }
     tmp = this.browerName.compareTo(other.browerName);
        if (tmp != 0){
            return tmp;
        }
      return tmp = this.browerVersion.compareTo(other.browerVersion);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getBrowerName() {
        return browerName;
    }

    public void setBrowerName(String browerName) {
        this.browerName = browerName;
    }

    public String getBrowerVersion() {
        return browerVersion;
    }

    public void setBrowerVersion(String browerVersion) {
        this.browerVersion = browerVersion;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BrowserDimension that = (BrowserDimension) o;
        return id == that.id &&
                Objects.equals(browerName, that.browerName) &&
                Objects.equals(browerVersion, that.browerVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, browerName, browerVersion);
    }

    @Override
    public String toString() {
        return "BrowserDimension{" +
                "id=" + id +
                ", browerName='" + browerName + '\'' +
                ", browerVersion='" + browerVersion + '\'' +
                '}';
    }
}
