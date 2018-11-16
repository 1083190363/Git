package mr.local;

import common.Constants;
import common.DateEnum;
import common.KpiType;
import dimension.base.DateDimension;
import dimension.base.KpiDimension;
import dimension.base.LocationDimension;
import dimension.base.PlatformDimension;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import value.StatsCommonDimension;
import value.StatusLocalDimension;
import value.map.TextOutputValue;

import java.io.IOException;

/**
 * @ProjectName: git
 * @Package: mr.activemember
 * @ClassName: ActiveMemberMapper
 * @Description: 地域维度用户的Mappper类
 * @Author: CaoXueCheng
 * @CreateDate: 2018/11/8 15:36
 * @UpdateUser: CaoXueCheng
 * @UpdateDate: 2018/11/8 15:36
 * @Version: 1.0
 */
public class LocalMapper extends Mapper<LongWritable, Text, StatusLocalDimension, TextOutputValue> {
    private static final Logger logger = Logger.getLogger(LocalMapper.class);
    private StatusLocalDimension k = new StatusLocalDimension();
    private TextOutputValue v = new TextOutputValue();

    private KpiDimension localKpi = new KpiDimension(KpiType.LOCAL.kpiName);
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
        String uuid = fields[3];
        String platform = fields[13];
        String sessionId = fields[5];
        String country = fields[28];
        String province = fields[29];
        String city = fields[30];
        if(StringUtils.isEmpty(uuid) || sessionId.equals("null")){
            logger.info("serverId& mid is null serverTime:"+serverTime+".sessionId"+sessionId);
            return;
        }
        //构造输出的key
        long stime = Long.valueOf(serverTime);
        LocationDimension locationDimension = new LocationDimension(country,province,city);
        PlatformDimension platformDimension = PlatformDimension.getInstance(platform);
        DateDimension dateDimension = DateDimension.buildDate(stime, DateEnum.DAY);
        StatsCommonDimension statsCommonDimension = this.k.getStatsCommonDimension();
        //为StatsCommonDimension设值

        statsCommonDimension.setDateDimension(dateDimension);
        statsCommonDimension.setPlatformDimension(platformDimension);
        statsCommonDimension.setKpiDimension(localKpi);
        this.k.setStatsCommonDimension(statsCommonDimension);
        this.k.setLocalDimension(locationDimension);
        this.v.setId(uuid);
        this.v.setSessionid(sessionId);
        context.write(this.k,this.v);//输出

         }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
