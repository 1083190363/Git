package mr.local;

import common.GlobalConstants;
import mr.IOutputWritter;
import mr.service.IDimension;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.Logger;
import value.StatsBaseDimension;
import value.StatsOutputValue;
import value.StatsUserDimension;
import value.StatusLocalDimension;
import value.map.TextOutputValue;
import value.reduce.LocalOutputValue;
import value.reduce.OutputValue;

import java.sql.PreparedStatement;

/**
 * @ProjectName: git
 * @Package: mr.activemember
 * @ClassName: LocalOutWritter
 * @Description: 地域维度的输出类
 * @Author: CaoXueCheng
 * @CreateDate: 2018/11/8 15:29
 * @UpdateUser: CaoXueCheng
 * @UpdateDate: 2018/11/8 15:29
 * @Version: 1.0
 */
public class LocalOutWritter implements IOutputWritter {
    private static final Logger logger = Logger.getLogger(LocalOutWritter.class);
    @Override
    public void output(Configuration conf, StatsBaseDimension key, StatsOutputValue value, PreparedStatement ps, IDimension iDimension) {
        try {
            StatusLocalDimension k = (StatusLocalDimension) key;
            LocalOutputValue v = (LocalOutputValue)value;
            //计算地域维度
            int i = 0;
            ps.setInt(++i,iDimension.getDimensionIdByObject(k.getStatsCommonDimension().getDateDimension()));
            ps.setInt(++i,iDimension.getDimensionIdByObject(k.getStatsCommonDimension().getPlatformDimension()));
            ps.setInt(++i,iDimension.getDimensionIdByObject(k.getLocalDimension()));
            ps.setInt(++i,v.getAus());
            ps.setInt(++i,v.getSessions());
            ps.setInt(++i,v.getBoundsessions());
            //设置时间
            ps.setString(++i,conf.get(GlobalConstants.RUNNING_DATE));
            ps.setInt(++i,v.getAus());
            ps.setInt(++i,v.getSessions());
            ps.setInt(++i,v.getBoundsessions());
            ps.addBatch();
        } catch (Exception e) {
            logger.warn("ps赋值异常");
        }
    }
}
