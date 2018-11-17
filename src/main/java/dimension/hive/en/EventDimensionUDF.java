package dimension.hive.en;

import IpAnalysis.TimeUtil;
import common.DateEnum;
import common.GlobalConstants;
import dimension.base.DateDimension;
import dimension.base.EventDimension;
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
 * @Description: 获取事件维度ID的Udf
 * @Author: CaoXueCheng
 * @CreateDate: 2018/11/16 21:57
 * @UpdateUser: CaoXueCheng
 * @UpdateDate: 2018/11/16 21:57
 * @Version: 1.0
 */
public class EventDimensionUDF extends UDF {
    private IDimensionConverter convert = new IDimensionImplConverter();
    //获取时间ID
    public int evaluate(String category,String action){
        if (StringUtils.isEmpty(category)){
            category = action = GlobalConstants.DEFAULT_VALUE;
        }
        EventDimension dd = new EventDimension(category,action);
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
    public static void main(String[] args) {
        //System.out.println(new EventDimensionUDF().evaluate()"));
    }
}
