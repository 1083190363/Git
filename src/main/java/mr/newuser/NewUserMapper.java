package mr.newuser;

import common.Constants;
import common.DateEnum;
import common.KpiType;
import dimension.base.BrowserDimension;
import dimension.base.DateDimension;
import dimension.base.KpiDimension;
import dimension.base.PlatformDimension;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import value.StatsCommonDimension;
import value.StatsUserDimension;
import value.map.TimeOutputValue;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * todo 新增用户的Mapper类
 * @author
 * @create 2018/9/19
 * @since 1.0.0
 * 用户模块下的新增用户
 *
 * 注意点：每次测试前都要清空数据库中的数据
 * 新建查询---执行所有的SQL语句
 * 如下：
truncate dimension_browser;
truncate dimension_currency_type;
truncate dimension_date;
truncate dimension_event;
truncate dimension_inbound;
truncate dimension_kpi;
truncate dimension_location;
truncate dimension_os;
truncate dimension_payment_type;
truncate dimension_platform;
truncate event_info;
truncate order_info;
truncate stats_device_browser;
truncate stats_device_location;
truncate stats_event;
truncate stats_hourly;
truncate stats_inbound;
truncate stats_order;
truncate stats_user;
truncate stats_view_depth;
 */
public class NewUserMapper extends Mapper<LongWritable,Text,StatsUserDimension,TimeOutputValue> {
    private static final Logger logger = Logger.getLogger(NewUserMapper.class);
    private StatsUserDimension k = new StatsUserDimension();
    private TimeOutputValue v = new TimeOutputValue();
    //指定kpitype为new_user
    private KpiDimension newUserKpi = new KpiDimension(KpiType.NEW_USER.kpiName);
    //指定浏览器新增用户的类型
    private KpiDimension newBrowserUserKpi = new KpiDimension(KpiType.BROWSER_NEW_USER.kpiName);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if(StringUtils.isEmpty(line)){
            return ;
        }
        //拆分
        String[] fields = line.split("\001");
        //获取事件类型
        String en = fields[2];
        if(StringUtils.isNotEmpty(en) && en.equals(Constants.EventEnum.LANUCH.alias)){
                //获取想要的字段
                String serverTime = fields[1];
            // System.out.println(serverTime);
                String platform = fields[13];
                String uuid = fields[3];
                String browserName = fields[24];
                String browserVersion = fields[25];

                //判断时间戳和uuid是否为空值,将空值过滤掉
            if(StringUtils.isEmpty(serverTime) || uuid.equals("null")){
                logger.info("serverTime & uuid is null serverTime:"+serverTime+".uuid"+uuid);
                return;
            }

            //构造输出的key
            //获取时间戳
            long stime = Long.valueOf(serverTime);
            //将平台信息封装到凭条维度类中
            PlatformDimension platformDimension = PlatformDimension.getInstance(platform);
            //将获取的时间封装到时间维度类中
            DateDimension dateDimension = DateDimension.buildDate(stime, DateEnum.DAY);
            //将上面两个有值的公共纬度类封装到公共维度类StatsCommonDimension中
            StatsCommonDimension statsCommonDimension = this.k.getStatsCommonDimension();
            //为StatsCommonDimension设值
            statsCommonDimension.setDateDimension(dateDimension);
            statsCommonDimension.setPlatformDimension(platformDimension);
            //用户模块新增用户
            //设置默认的浏览器对象(因为新增用户指标并不需要浏览器维度，所以赋值为空)
            BrowserDimension defaultBrowserDimension = new BrowserDimension("","");
            //设置kpi类型
            statsCommonDimension.setKpiDimension(newUserKpi);
            this.k.setBrowserDimension(defaultBrowserDimension);
            this.k.setStatsCommonDimension(statsCommonDimension);
            this.v.setId(uuid);
            //写出
            context.write(this.k,this.v);

            //浏览器模块新增用户
            statsCommonDimension.setKpiDimension(newBrowserUserKpi);
            BrowserDimension browserDimension = new BrowserDimension(browserName,browserVersion);
           // System.out.println(browserName);
            this.k.setBrowserDimension(browserDimension);
            this.k.setStatsCommonDimension(statsCommonDimension);
            context.write(this.k,this.v);//输出
        }
    }
}
