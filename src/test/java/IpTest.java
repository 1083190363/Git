import IpAnalysis.IPSeeker;
import IpAnalysis.IpUtil;

public class IpTest {
   /**
   * @todo
   * @author 曹学成
   * @date 2018/11/2
   * @time 20:36
   * @method 
   * @param 
   */
   
    public static void main(String[] args) {
       System.out.println(IPSeeker.getInstance().getCountry("114.231.112.23"));

        System.out.println(IpUtil.getRegionInfoByIp("114.231.112.23"));


    }
}
