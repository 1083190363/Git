package mr.activeuser;

import common.Constants;
import common.DateEnum;
import common.KpiType;
import dimension.base.BrowserDimension;
import dimension.base.DateDimension;
import dimension.base.KpiDimension;
import dimension.base.PlatformDimension;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import value.StatsCommonDimension;
import value.StatsUserDimension;
import value.map.TimeOutputValue;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

/**
 * @ProjectName: git
 * @Package: mr.activeuser
 * @ClassName: ActiveUserMapper
 * @Description: 活跃用户的mapper类
 * @Author: CaoXueCheng
 * @CreateDate: 2018/11/6 20:55
 * @UpdateUser: CaoXueCheng
 * @UpdateDate: 2018/11/6 20:55
 * @Version: 1.0
 */
public class ActiveUserMapper extends Mapper<LongWritable, Text, StatsUserDimension, TimeOutputValue> {
    private static final Logger logger = Logger.getLogger(ActiveUserMapper.class);
    private StatsUserDimension k = new StatsUserDimension();
    private TimeOutputValue v = new TimeOutputValue();

    private KpiDimension activeUserKpi = new KpiDimension(KpiType.ACTIVE_USER.kpiName);
    private KpiDimension activeBrowserUserKpi = new KpiDimension(KpiType.BROWSER_ACTIVE_USER.kpiName);
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        //HashSet<String> uuidSet = new HashSet<>();
        if(StringUtils.isEmpty(line)){
            return ;
        }
        //拆分
          String[] fields = line.split("\001");
        //en是事件名称
          String en = fields[2];
       // if(StringUtils.isNotEmpty(en) && en.equals(Constants.EventEnum.LANUCH.alias)) {
            //获取想要的字段
            String serverTime = fields[1];
            String uuid = fields[3];
            String platform = fields[13];
            String browserName = fields[24];
            String browserVersion = fields[25];

            if(StringUtils.isEmpty(serverTime) || uuid.equals("null")){
                logger.info("serverTime & uuid is null serverTime:"+serverTime+".uuid"+uuid);
                return;
            }

            //构造输出的key
            long stime = Long.valueOf(serverTime);
           PlatformDimension platformDimension = PlatformDimension.getInstance(platform);
           DateDimension dateDimension = DateDimension.buildDate(stime, DateEnum.DAY);
            StatsCommonDimension statsCommonDimension = this.k.getStatsCommonDimension();
            //为StatsCommonDimension设值
            statsCommonDimension.setDateDimension(dateDimension);
            statsCommonDimension.setPlatformDimension(platformDimension);
            //stats_user表 活跃用户
            BrowserDimension defaultBrowserDimension = new BrowserDimension("","");
            statsCommonDimension.setKpiDimension(activeUserKpi);
//            System.out.println(statsCommonDimension);
//            System.out.println("hahahahah");
            this.k.setBrowserDimension(defaultBrowserDimension);
            this.k.setStatsCommonDimension(statsCommonDimension);
            this.v.setId(uuid);
             context.write(this.k,this.v);//输出

            //stats_device_browser表  活跃浏览器用户
            statsCommonDimension.setKpiDimension(activeBrowserUserKpi);
            BrowserDimension browserDimension = new BrowserDimension(browserName,browserVersion);
            this.k.setBrowserDimension(browserDimension);
            this.k.setStatsCommonDimension(statsCommonDimension);
            this.v.setId(uuid);
            context.write(this.k,this.v);//输出
       // }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
