package mr.hourly;

import common.GlobalConstants;
import common.KpiType;
import mr.IOutputWritter;
import mr.service.IDimension;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.log4j.Logger;
import value.StatsBaseDimension;
import value.StatsOutputValue;
import value.StatsUserDimension;
import value.reduce.OutputValue;
import java.sql.PreparedStatement;

/**
 * @ProjectName: git
 * @Package: mr.activeuser
 * @ClassName: HourlyActiveUserOutWritter
 * @Description: 按照小时统计会话时长
 * @Author: CaoXueCheng
 * @CreateDate: 2018/11/6 20:39
 * @UpdateUser: CaoXueCheng
 * @UpdateDate: 2018/11/6 20:39
 * @Version: 1.0
 */
public class HourlyActiveUserOutWritter implements IOutputWritter {
    private static final Logger logger = Logger.getLogger(HourlyActiveUserOutWritter.class);
    @Override
    public void output(Configuration conf, StatsBaseDimension key, StatsOutputValue value, PreparedStatement ps, IDimension iDimension) {
        try {
            StatsUserDimension k = (StatsUserDimension) key;
            OutputValue v = (OutputValue) value;
             int i=0;
             switch (v.getKpi()) {
                 case ACTIVE_USER:
                 case BROWSER_ACTIVE_USER:
                     int activeUser = ((IntWritable) (v.getValue().get(new IntWritable(-1)))).get();
                     ps.setInt(++i, iDimension.getDimensionIdByObject(k.getStatsCommonDimension().getDateDimension()));
                     ps.setInt(++i, iDimension.getDimensionIdByObject(k.getStatsCommonDimension().getPlatformDimension()));
                     if (v.getKpi().equals(KpiType.BROWSER_ACTIVE_USER)){
                         ps.setInt(++i, iDimension.getDimensionIdByObject(k.getBrowserDimension()));
                     }
                     ps.setInt(++i, activeUser);
                     ps.setString(++i, conf.get(GlobalConstants.RUNNING_DATE));//注意这里需要在runner类里面进行赋值
                     ps.setInt(++i, activeUser);
                     //添加到批处理中，批量执行SQL语句
                     break;
                 //获取活跃用户的值
                 case HOURLY_ACTIVE_USER:
                 ps.setInt(++i, iDimension.getDimensionIdByObject(k.getStatsCommonDimension().getDateDimension()));
                 ps.setInt(++i, iDimension.getDimensionIdByObject(k.getStatsCommonDimension().getPlatformDimension()));
                 ps.setInt(++i, iDimension.getDimensionIdByObject(k.getStatsCommonDimension().getKpiDimension()));
                 for (int j=0; j < 24; j++) {
                     ps.setInt(++i, ((IntWritable) ((MapWritable) v.getValue()).get(new IntWritable(j))).get());
                 }
                 ps.setString(++i, conf.get(GlobalConstants.RUNNING_DATE));
                 for (int j=0; j < 24; j++) {
                     ps.setInt(++i, ((IntWritable) ((MapWritable) v.getValue()).get(new IntWritable(j))).get());
                     }
                 break;
             }
            ps.addBatch();//批处理执行sql语句
        } catch (Exception e) {
            logger.warn("给ps赋值失败！！！");
        }
    }
}
