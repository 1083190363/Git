package mr.nu;

/**
 * @ProjectName: git
 * @Package: mr.nu
 * @ClassName: NewUserOutputWritter
 * @Description: java类作用描述
 * @Author: CaoXueCheng
 * @CreateDate: 2018/11/5 14:46
 * @UpdateUser: CaoXueCheng
 * @UpdateDate: 2018/11/5 14:46
 * @Version: 1.0
 */

import common.GlobalConstants;
import mr.IOutputWritter;
import mr.OutputWritable;
import mr.service.IDimension;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.Logger;
import value.StatsBaseDimension;
import value.StatsOutputValue;
import value.StatsUserDimension;

import java.sql.PreparedStatement;

/**
 * 〈一句话功能简述〉<br>
 * 〈对于不同的指标，这列赋值都是不一样的〉
 *
 * @author 14751
 * @create 2018/9/21
 * @since 1.0.0
 */
public class NewUserOutputWritter implements IOutputWritter {
    private static final Logger logger = Logger.getLogger(NewUserOutputWritter.class);
    @Override
    //这里通过key和value给ps语句赋值
    public void output(Configuration conf, StatsBaseDimension key, StatsOutputValue value, PreparedStatement ps, IDimension iDimension) {

        try {
            StatsUserDimension k = (StatsUserDimension) key;
            OutputWritable v = (OutputWritable) value;

            //获取新增用户的值
            int newUser = ((IntWritable)(v.getValue().get(new IntWritable(-1)))).get();

            int i = 0;
            ps.setInt(++i,iDimension.getDimensionIdByObject(k.getStatsCommonDimension().getDateDimension()));
            ps.setInt(++i,iDimension.getDimensionIdByObject(k.getStatsCommonDimension().getPlatformDimension()));
//            //修改1
//            if(v.getKpi().equals(KpiType.BROWSER_NEW_USER)){
//                ps.setInt(++i,iDimension.getDimensionIdByObject(k.getBrowserDimension()));
//            }
            ps.setInt(++i,newUser);
            ps.setString(++i,conf.get(GlobalConstants.RUNNING_DATE));//注意这里需要在runner类里面进行赋值
            ps.setInt(++i,newUser);

            ps.addBatch();//添加到批处理中，批量执行SQL语句
        } catch (Exception e) {
            logger.warn("给ps赋值失败！！！");
        }
    }
}
