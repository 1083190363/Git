package mr.event;

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
 * @author 14751
 * @create 2018/9/19
 * @since 1.0.0
 * 事件的Mapper类
 */
public class EventMapper extends Mapper<LongWritable,Text,StatsUserDimension,TimeOutputValue> {
    private static final Logger logger = Logger.getLogger(EventMapper.class);
    private StatsUserDimension k = new StatsUserDimension();
    private TimeOutputValue v = new TimeOutputValue();
    private KpiDimension pageViewKpi = new KpiDimension(KpiType.PAGEVIEW.kpiName);

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
        //if(StringUtils.isNotEmpty(en) && en.equals(Constants.EventEnum.LANUCH.alias)){
                //获取想要的字段
                String serverTime = fields[1];
            // System.out.println(serverTime);
                String platform = fields[13];
                String url = fields[10];
                String browserName = fields[24];
                String browserVersion = fields[25];
            if(StringUtils.isEmpty(serverTime) || url.equals("null")){
                logger.info("servertimeis null url:"+serverTime+".url"+url);
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

            statsCommonDimension.setKpiDimension(pageViewKpi);
            BrowserDimension browserDimension = new BrowserDimension(browserName,browserVersion);
            this.k.setBrowserDimension(browserDimension);
            this.k.setStatsCommonDimension(statsCommonDimension);
            this.v.setId(url);
            context.write(this.k,this.v);//输出
        }
   // }
}
