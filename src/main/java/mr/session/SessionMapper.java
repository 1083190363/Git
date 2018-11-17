package mr.session;

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

/**
 * @ProjectName: git
 * @Package: mr.session
 * @ClassName: SessionMapper
 * @Description: 会话的Mapper类
 * @Author: CaoXueCheng
 * @CreateDate: 2018/11/6 20:55
 * @UpdateUser: CaoXueCheng
 * @UpdateDate: 2018/11/6 20:55
 * @Version: 1.0
 */
public class SessionMapper extends Mapper<LongWritable, Text, StatsUserDimension, TimeOutputValue> {
    private static final Logger logger = Logger.getLogger(SessionMapper.class);
    private StatsUserDimension k = new StatsUserDimension();
    private TimeOutputValue v = new TimeOutputValue();

    private KpiDimension sessionKpi = new KpiDimension(KpiType.SESSION.kpiName);
    private KpiDimension browserSessionKpi = new KpiDimension(KpiType.BROWSER_SESSION.kpiName);
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
            String sessionId = fields[5];
            String platform = fields[13];
            String browserName = fields[24];
            String browserVersion = fields[25];

            if(StringUtils.isEmpty(serverTime) || sessionId.equals("null")){
                logger.info("serverTime & uuid is null serverTime:"+serverTime+".sessionId"+sessionId);
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
            statsCommonDimension.setKpiDimension(sessionKpi);
            this.k.setBrowserDimension(defaultBrowserDimension);
            this.k.setStatsCommonDimension(statsCommonDimension);
            this.v.setId(sessionId);
            //一定要设置 根据这个计算时间长度
            this.v.setTime(stime);
            context.write(this.k,this.v);//输出

            //stats_device_browser表  活跃浏览器用户
            statsCommonDimension.setKpiDimension(browserSessionKpi);
            BrowserDimension browserDimension = new BrowserDimension(browserName,browserVersion);
            this.k.setBrowserDimension(browserDimension);
            this.k.setStatsCommonDimension(statsCommonDimension);
            this.v.setId(sessionId);
            //一定要设置 根据这个时间计算时间长度
            this.v.setTime(stime);
            context.write(this.k,this.v);//输出
       // }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
