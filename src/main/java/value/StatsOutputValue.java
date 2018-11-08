package value;

import common.KpiType;
import org.apache.hadoop.io.Writable;

/**
 * @ProjectName: git
 * @Package: value
 * @ClassName: StatsOutputValue
 * @Description: 封装map或者reduce阶段输出的value类型的顶级父类
 * @Author: CaoXueCheng
 * @CreateDate: 2018/11/3 21:00
 * @UpdateUser: CaoXueCheng
 * @UpdateDate: 2018/11/3 21:00
 * @Version: 1.0
 */

public abstract class StatsOutputValue implements Writable {
    //获取kpi的抽象方法
    public abstract KpiType getKpi();

}

