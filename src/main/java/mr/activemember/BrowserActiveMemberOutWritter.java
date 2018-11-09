package mr.activemember;

import common.GlobalConstants;
import common.KpiType;
import dimension.base.BaseDimension;
import mr.IOutputWritter;
import mr.service.IDimension;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.Logger;
import value.StatsBaseDimension;
import value.StatsOutputValue;
import value.StatsUserDimension;
import value.reduce.OutputValue;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @ProjectName: git
 * @Package: mr.activemember
 * @ClassName: BrowserActiveMemberOutWritter
 * @Description: 浏览器活跃会员
 * @Author: CaoXueCheng
 * @CreateDate: 2018/11/8 15:30
 * @UpdateUser: CaoXueCheng
 * @UpdateDate: 2018/11/8 15:30
 * @Version: 1.0
 */
public class BrowserActiveMemberOutWritter implements IOutputWritter {
    private static final Logger logger = Logger.getLogger(BrowserActiveMemberOutWritter.class);
    @Override
    public void output(Configuration conf, StatsBaseDimension key, StatsOutputValue value, PreparedStatement ps, IDimension iDimension) {
        try {
            StatsUserDimension k = (StatsUserDimension) key;
            OutputValue v = (OutputValue)value;
            //计算活跃会员的值
            int activeBrowserMember = ((IntWritable) (v.getValue().get(new IntWritable(-1)))).get();
            int i = 0;
            ps.setInt(++i,iDimension.getDimensionIdByObject(k.getStatsCommonDimension().getDateDimension()));
            ps.setInt(++i,iDimension.getDimensionIdByObject(k.getStatsCommonDimension().getPlatformDimension()));
            //判断是否为会员
            if(v.getKpi().equals(KpiType.BROWSER_ACTIVE_MEMBER)){
                ps.setInt(++i,iDimension.getDimensionIdByObject(k.getBrowserDimension()));
            }
            ps.setInt(++i,activeBrowserMember);
            //设置时间
            ps.setString(++i,conf.get(GlobalConstants.RUNNING_DATE));
            ps.setInt(++i,activeBrowserMember);
            ps.addBatch();
        } catch (Exception e) {
            logger.warn("ps赋值异常");
        }
    }
}
