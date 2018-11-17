package dimension.hive.en;

import IpAnalysis.TimeUtil;
import common.DateEnum;
import dimension.base.DateDimension;
import mr.service.IDimensionConverter;
import mr.service.impl.IDimensionImplConverter;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import java.io.IOException;
import java.sql.SQLException;

/**
 * @ProjectName: git
 * @Package: dimension.hive.en
 * @ClassName: DateDimensionUDF
 * @Description: 获取时间维度ID的Udf
 * @Author: CaoXueCheng
 * @CreateDate: 2018/11/16 21:57
 * @UpdateUser: CaoXueCheng
 * @UpdateDate: 2018/11/16 21:57
 * @Version: 1.0
 */
public class DateDimensionUDF extends UDF {
    private IDimensionConverter convert = new IDimensionImplConverter();
    //获取时间ID
    public int evaluate(String str){
        if (StringUtils.isEmpty(str)){
            str = TimeUtil.getYesterday();
        }
        DateDimension dd = DateDimension.buildDate(TimeUtil.parseString2Long(str), DateEnum.DAY);
       int id = -1 ;
        try {
            id = convert.getDimensionIdByObject(dd);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return id;
    }
//    public int evaluate(Text dt, int i){
//        String dd = dt.toString();
//        if (StringUtils.isEmpty(dd)){
//           dd = TimeUtil.getYesterday();
//        }
//        DateDimension  dm = DateDimension.buildDate(TimeUtil.parseString2Long(dd), DateEnum.DAY);
//        int id = -1 ;
//        try {
//            id = convert.getDimensionIdByObject(dm);
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//        return id+i;
//    }
    public static void main(String[] args) {
        System.out.println(new DateDimensionUDF().evaluate("2018-8-30"));
       // System.out.println(new DateDimensionUDF().evaluate(new Text("2018-8-30"),2));
    }
}
