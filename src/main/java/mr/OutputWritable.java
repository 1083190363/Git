package mr;

import common.KpiType;
import mr.service.IDimension;
import org.apache.hadoop.conf.Configuration;
import value.StatsBaseDimension;
import value.StatsOutputValue;

import java.sql.PreparedStatement;
import java.util.List;
import java.util.Map;

/**
 * @ProjectName: git
 * @Package: mr
 * @ClassName: OutputWritable
 * @Description: java类作用描述
 * @Author: CaoXueCheng
 * @CreateDate: 2018/11/5 14:42
 * @UpdateUser: CaoXueCheng
 * @UpdateDate: 2018/11/5 14:42
 * @Version: 1.0
 */
public interface OutputWritable {
    /**
     * 为每一个kpi的最终结果赋值的接口
     * @param conf
     * @param key
     * @param value
     * @param ps
     * @param iDimension
     */
    void output(Configuration conf,
                StatsBaseDimension key,
                StatsOutputValue value,
                PreparedStatement ps,
                IDimension iDimension);
    Map getValue();

    KpiType getKpi();
}
