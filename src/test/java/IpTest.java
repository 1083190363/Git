import IpAnalysis.IPSeeker;
import IpAnalysis.IpUtil;

public class IpTest {
    public static void main(String[] args) {
       System.out.println(IPSeeker.getInstance().getCountry("114.231.112.23"));

        System.out.println(IpUtil.getRegionInfoByIp("114.231.112.23"));


    }
}
