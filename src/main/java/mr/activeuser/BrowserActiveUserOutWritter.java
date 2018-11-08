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
 * @ClassName: BrowserActiveUserOutWritter
 * @Description: 浏览器的活跃用户输出类
 * @Author: CaoXueCheng
 * @CreateDate: 2018/11/8 11:21
 * @UpdateUser: CaoXueCheng
 * @UpdateDate: 2018/11/8 11:21
 * @Version: 1.0
 */
public class BrowserActiveUserOutWritter implements IOutputWritter {
    private static final Logger logger = Logger.getLogger(BrowserActiveUserOutWritter.class);
    @Override
    public void output(Configuration conf, StatsBaseDimension key, StatsOutputValue value, PreparedStatement ps, IDimension iDimension) {
        try {
            StatsUserDimension k = (StatsUserDimension) key;
            OutputValue v = (OutputValue) value;

            //获取活跃用户的值
            int activeBrowserUser = ((IntWritable)(v.getValue().get(new IntWritable(-1)))).get();

            int i = 0;
            ps.setInt(++i,iDimension.getDimensionIdByObject(k.getStatsCommonDimension().getDateDimension()));
            ps.setInt(++i,iDimension.getDimensionIdByObject(k.getStatsCommonDimension().getPlatformDimension()));
            //修改1
            if(v.getKpi().equals(KpiType.BROWSER_ACTIVE_USER)){
                ps.setInt(++i,iDimension.getDimensionIdByObject(k.getBrowserDimension()));
            }
            ps.setInt(++i,activeBrowserUser);
            ps.setString(++i,conf.get(GlobalConstants.RUNNING_DATE));//注意这里需要在runner类里面进行赋值
            ps.setInt(++i,activeBrowserUser);

            ps.addBatch();
            //添加到批处理中，批量执行SQL语句
        } catch (Exception e) {
            logger.warn("给ps赋值失败！！！");
        }
    }
}
