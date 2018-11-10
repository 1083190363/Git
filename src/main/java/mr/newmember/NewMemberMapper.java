package mr.newmember;

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
 * @Package: mr.newmemeber
 * @ClassName: NewMemberMapper
 * @Description: java类作用描述
 * @Author: CaoXueCheng
 * @CreateDate: 2018/11/9 19:18
 * @UpdateUser: CaoXueCheng
 * @UpdateDate: 2018/11/9 19:18
 * @Version: 1.0
 */
public class NewMemberMapper extends Mapper<LongWritable,Text,StatsUserDimension,TimeOutputValue>{
    private static final Logger logger = Logger.getLogger(NewMemberMapper.class);
    private StatsUserDimension k = new StatsUserDimension();
    private TimeOutputValue v = new TimeOutputValue();

    private KpiDimension newMember = new KpiDimension(KpiType.NEW_MEMBER.kpiName);
    private KpiDimension newBrowserMemberKpi = new KpiDimension(KpiType.BROWSER_NEW_USER.kpiName);

    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if(StringUtils.isEmpty(line)){
            return ;
        }

        //拆分
        String[] fields = line.split("\001");
        //en是事件名称
        String en = fields[2];
      //  if(StringUtils.isNotEmpty(en) && en.equals(Constants.EventEnum.PAGEVIEW.alias)){
            //获取想要的字段
            String serverTime = fields[1];
            // System.out.println(serverTime);
            String platform = fields[13];
            String u_mid = fields[4];
            String browserName = fields[24];
            String browserVersion = fields[25];

            if(StringUtils.isEmpty(serverTime) || u_mid.equals("null")){
                logger.info("serverTime & uuid is null serverTime:"+serverTime+".umid"+u_mid);
                return;
            }

            //构造输出的key
            long stime = Long.valueOf(serverTime);
            System.out.println(stime);
            PlatformDimension platformDimension = PlatformDimension.getInstance(platform);
            DateDimension dateDimension = DateDimension.buildDate(stime, DateEnum.DAY);
            StatsCommonDimension statsCommonDimension = this.k.getStatsCommonDimension();
            //为StatsCommonDimension设值
            statsCommonDimension.setDateDimension(dateDimension);
            statsCommonDimension.setPlatformDimension(platformDimension);
            System.out.println(statsCommonDimension);
            //用户模块新增用户
            //设置默认的浏览器对象(因为新增用户指标并不需要浏览器维度，所以赋值为空)
            BrowserDimension defaultBrowserDimension = new BrowserDimension("","");
            statsCommonDimension.setKpiDimension(newMember);
            this.k.setBrowserDimension(defaultBrowserDimension);
            this.k.setStatsCommonDimension(statsCommonDimension);
            this.v.setId(u_mid);
            context.write(this.k,this.v);//输出

            //浏览器模块新增用户
            statsCommonDimension.setKpiDimension(newBrowserMemberKpi);
            BrowserDimension browserDimension = new BrowserDimension(browserName,browserVersion);
            // System.out.println(browserName);
            this.k.setBrowserDimension(browserDimension);
            this.k.setStatsCommonDimension(statsCommonDimension);
            context.write(this.k,this.v);//输出
       // }
    }
}
