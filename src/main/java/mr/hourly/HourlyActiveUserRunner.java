package mr.hourly;

import IpAnalysis.TimeUtil;
import common.GlobalConstants;
import mr.OutputToMySqlFormat;
import mr.newuser.NewUserRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import value.StatsUserDimension;
import value.map.TimeOutputValue;
import value.reduce.OutputValue;

import java.io.IOException;

/**
 * @ProjectName: git
 * @Package: mr.activeuser
 * @ClassName: ActiveUserRunner
 * @Description: 活跃用户的reducer类
 * @Author: CaoXueCheng
 * @CreateDate: 2018/11/6 22:30
 * @UpdateUser: CaoXueCheng
 * @UpdateDate: 2018/11/6 22:30
 * @Version: 1.0
 */
public class HourlyActiveUserRunner implements Tool {

    private static final Logger logger = Logger.getLogger(NewUserRunner.class);
    private Configuration conf = new Configuration();
    //主函数---入口
    public static void main(String[] args){
        try {
                ToolRunner.run(new Configuration(),new HourlyActiveUserRunner(),args);
        } catch (Exception e) {
            logger.warn("Hourly_Active_USER TO MYSQL is failed !!!",e);
        }
    }

    @Override
    public void setConf(Configuration configuration) {
        conf.addResource("output_mapping.xml");
        conf.addResource("output_writter.xml");
        conf.addResource("other_mapping.xml");
//        conf.addResource("total_mapping.xml");//修改1
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();

        //为结果表中的created赋值，设置到conf中,需要我们传递参数---一定要在job获取前设置参数
        this.setArgs(args,conf);
//        String date = TimeUtil.parseLongToString(GlobalConstants.DEFAULT_FORMAT);//这么做不符合实际生产，因为数据不可能是当天立刻产生的
//        conf.set(GlobalConstants.RUNNING_DATE,date);

        Job job = Job.getInstance(conf,"Hourly_Active_USER TO MYSQL");

        job.setJarByClass(HourlyActiveUserRunner.class);

        //设置map相关参数
        job.setMapperClass(HourlyActiveUserMapper.class);
        job.setMapOutputKeyClass(StatsUserDimension.class);
        job.setMapOutputValueClass(TimeOutputValue.class);

        //设置reduce相关参数
        //设置reduce端的输出格式类
        job.setReducerClass(HourlyActiveUserReducer.class);
        job.setOutputKeyClass(StatsUserDimension.class);
        job.setOutputValueClass(OutputValue.class);
        job.setOutputFormatClass(OutputToMySqlFormat.class);

        //设置reduce task的数量
        job.setNumReduceTasks(1);

        //设置输入参数
        this.handleInputOutput(job);
         return job.waitForCompletion(true)? 0:1;
    }

    /**
     * 参数处理,将接收到的日期存储在conf中，以供后续使用
     * @param args  如果没有传递日期，则默认使用昨天的日期
     * @param conf
     */
    private void setArgs(String[] args, Configuration conf) {
        String date = null;
        for (int i = 0;i < args.length;i++){
            if(args[i].equals("-d")){
                if(i+1 < args.length){
                    date = args[i+1];
                    break;
                }
            }
        }
        //代码到这儿，date还是null，默认用昨天的时间
        if(date == null){
            date = TimeUtil.getYesterday();
        }
        //然后将date设置到时间conf中
        conf.set(GlobalConstants.RUNNING_DATE,date);
    }

    /**
     * 设置输入输出,_SUCCESS文件里面是空的，所以可以直接读取清洗后的数据存储目录
     * @param job
     */
    private void handleInputOutput(Job job) {
        String[] fields = job.getConfiguration().get(GlobalConstants.RUNNING_DATE).split("-");
        String month = fields[1];
        String day = fields[2];

        try {
            FileSystem fs = FileSystem.get(job.getConfiguration());
            Path inpath = new Path("/ods/" + 11 + "/" + "06");
            if(fs.exists(inpath)){
                FileInputFormat.addInputPath(job,inpath);
            }else{
                throw new RuntimeException("输入路径不存在inpath" + inpath.toString());
            }
        } catch (IOException e) {
            logger.warn("设置输入输出路径异常！！！",e);
        }
    }
}
