package mr.activemember;

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

/**
 * @ProjectName: git
 * @Package: mr.activemember
 * @ClassName: ActiveMemberMapper
 * @Description: 活跃会员的Mapper类
 * @Author: CaoXueCheng
 * @CreateDate: 2018/11/8 15:36
 * @UpdateUser: CaoXueCheng
 * @UpdateDate: 2018/11/8 15:36
 * @Version: 1.0
 */
public class ActiveMemberMapper extends Mapper<LongWritable, Text, StatsUserDimension, TimeOutputValue> {
    private static final Logger logger = Logger.getLogger(ActiveMemberMapper.class);
    private StatsUserDimension k = new StatsUserDimension();
    private TimeOutputValue v = new TimeOutputValue();

    private KpiDimension activeMemberKpi = new KpiDimension(KpiType.ACTIVE_MEMBER.kpiName);
    private KpiDimension activeBrowserMemberKpi = new KpiDimension(KpiType.BROWSER_ACTIVE_MEMBER.kpiName);
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if(StringUtils.isEmpty(line)){
            return ;
        }
        //拆分
        String[] fields = line.split("\001");
        //en是事件名称
        String en = fields[2];
         if(StringUtils.isNotEmpty(en) && en.equals(Constants.EventEnum.PAGEVIEW.alias)) {
        //获取想要的字段
        String serverTime = fields[1];
        String mid = fields[4];
        String platform = fields[13];
        String browserName = fields[24];
        String browserVersion = fields[25];

             if(StringUtils.isEmpty(serverTime) || mid.equals("null")){
            logger.info("serverTime & mid is null serverTime:"+serverTime+".mid"+mid);
            return;
        }
            // System.out.println(mid+"---------------------");
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
        statsCommonDimension.setKpiDimension(activeMemberKpi);
//            System.out.println(statsCommonDimension);
//            System.out.println("hahahahah");
        this.k.setBrowserDimension(defaultBrowserDimension);
        this.k.setStatsCommonDimension(statsCommonDimension);
        this.v.setId(mid);
        context.write(this.k,this.v);//输出

        //stats_device_browser表  活跃浏览器用户
        statsCommonDimension.setKpiDimension(activeBrowserMemberKpi);
        BrowserDimension browserDimension = new BrowserDimension(browserName,browserVersion);
        this.k.setBrowserDimension(browserDimension);
        this.k.setStatsCommonDimension(statsCommonDimension);
        this.v.setId(mid);
        context.write(this.k,this.v);//输出
         }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
