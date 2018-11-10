package mr.session;

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
 *
 *session的ps
 * @author 14751
 * @create 2018/9/21
 * @since 1.0.0
 */
public class SessionsOutputWritter implements IOutputWritter {
    private static final Logger logger = Logger.getLogger(SessionsOutputWritter.class);
    @Override
    //这里通过key和value给ps语句赋值
    public void output(Configuration conf, StatsBaseDimension key, StatsOutputValue value, PreparedStatement ps, IDimension iDimension) {

        try {
            StatsUserDimension k = (StatsUserDimension) key;
            OutputValue v = (OutputValue) value;

            //获取新增用户的的值
            int sessions = ((IntWritable)(v.getValue().get(new IntWritable(-1)))).get();
            int sessionsLength=((IntWritable)(v.getValue().get(new IntWritable(-2)))).get();
            int i = 0;
            ps.setInt(++i,iDimension.getDimensionIdByObject(k.getStatsCommonDimension().getDateDimension()));
            ps.setInt(++i,iDimension.getDimensionIdByObject(k.getStatsCommonDimension().getPlatformDimension()));
            //修改1

//            if(v.getKpi().equals(KpiType.BROWSER_SESSION)){
//                ps.setInt(++i,iDimension.getDimensionIdByObject(k.getBrowserDimension()));
//            }
            ps.setInt(++i,sessions);
            ps.setInt(++i,sessionsLength);
            ps.setString(++i,conf.get(GlobalConstants.RUNNING_DATE));
            ps.setInt(++i,sessions);
            ps.setInt(++i,sessionsLength);
            ps.addBatch();//添加到批处理中，批量执行SQL语句
        } catch (Exception e) {
            logger.warn("给ps赋值异常！！！");
        }
    }
}
