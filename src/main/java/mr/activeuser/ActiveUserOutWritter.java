package mr.activeuser;

import common.GlobalConstants;
import common.KpiType;
import mr.IOutputWritter;
import mr.service.IDimension;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.Logger;
import value.StatsBaseDimension;
import value.StatsOutputValue;
import value.StatsUserDimension;
import value.reduce.OutputValue;

import java.sql.PreparedStatement;

/**
 * @ProjectName: git
 * @Package: mr.activeuser
 * @ClassName: ActiveMemberOutWritter
 * @Description: 活跃用户的输出类
 * @Author: CaoXueCheng
 * @CreateDate: 2018/11/6 20:39
 * @UpdateUser: CaoXueCheng
 * @UpdateDate: 2018/11/6 20:39
 * @Version: 1.0
 */
public class ActiveUserOutWritter implements IOutputWritter {
    private static final Logger logger = Logger.getLogger(ActiveUserOutWritter.class);
    @Override
    public void output(Configuration conf, StatsBaseDimension key, StatsOutputValue value, PreparedStatement ps, IDimension iDimension) {
        try {
            StatsUserDimension k = (StatsUserDimension) key;
            OutputValue v = (OutputValue) value;

            //获取活跃用户的值
            int activeUser = ((IntWritable)(v.getValue().get(new IntWritable(-1)))).get();
            int i = 0;
            ps.setInt(++i,iDimension.getDimensionIdByObject(k.getStatsCommonDimension().getDateDimension()));
            ps.setInt(++i,iDimension.getDimensionIdByObject(k.getStatsCommonDimension().getPlatformDimension()));
            //修改1

            ps.setInt(++i,activeUser);
            ps.setString(++i,conf.get(GlobalConstants.RUNNING_DATE));//注意这里需要在runner类里面进行赋值
            ps.setInt(++i,activeUser);
            ps.addBatch();
            //添加到批处理中，批量执行SQL语句
        } catch (Exception e) {
            logger.warn("给ps赋值失败！！！");
        }
    }
}
