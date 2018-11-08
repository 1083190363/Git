package mr.activeuser;

import IpAnalysis.JdbcUtil;
import IpAnalysis.TimeUtil;
import common.DateEnum;
import common.GlobalConstants;
import dimension.base.DateDimension;
import mr.OutputToMySqlFormat;
import mr.newuser.NewUserRunner;
import mr.service.IDimension;
import mr.service.impl.IDimensionImpl;
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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

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
public class ActiveUserRunner implements Tool {

    private static final Logger logger = Logger.getLogger(NewUserRunner.class);
    private Configuration conf = new Configuration();

    //主函数---入口
    public static void main(String[] args){
        try {
                ToolRunner.run(new Configuration(),new ActiveUserRunner(),args);
        } catch (Exception e) {
            logger.warn("Active_USER TO MYSQL is failed !!!",e);
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

        Job job = Job.getInstance(conf,"Active_USER TO MYSQL");

        job.setJarByClass(ActiveUserRunner.class);

        //设置map相关参数
        job.setMapperClass(ActiveUserMapper.class);
        job.setMapOutputKeyClass(StatsUserDimension.class);
        job.setMapOutputValueClass(TimeOutputValue.class);

        //设置reduce相关参数
        //设置reduce端的输出格式类
        job.setReducerClass(ActiveUserReducer.class);
        job.setOutputKeyClass(StatsUserDimension.class);
        job.setOutputValueClass(OutputValue.class);

        job.setOutputFormatClass(OutputToMySqlFormat.class);

        //设置reduce task的数量
        job.setNumReduceTasks(1);

        //设置输入参数
        this.handleInputOutput(job);
        this.computeNewTotalUser(job);
        // return job.waitForCompletion(true)? 0:1;
        if(job.waitForCompletion(true)){
            this.computeNewTotalUser(job);//修改1
            return 0;
        }else{
            return 1;
        }
    }

    //修改1
    /**
     * 计算新增的总用户（重点）
     *
     * 1、获取运行当天的日期，然后再获取到运行当天前一天的日期，然后获取对应时间维度Id
     * 2、当对应时间维度Id都大于0，则正常计算：查询前一天的新增总用户，获取当天的新增用户
     * @param job
     */
    private void computeNewTotalUser(Job job) {
        /**
         *  1、根据运行当天获取日期
         *  2、获取日期和前一天的对应的时间维度
         *  3、根据时间维度获取对应的时间维度ID
         *  4、根据前天的时间维度Id获取前天的新增总用户，根据当天的时间维度Id获取当天的新增用户
         *  5、更新当天的新增总用户
         *  6、同一维度前一天？？
         */


        String date = job.getConfiguration().get(GlobalConstants.RUNNING_DATE);
        long nowday = TimeUtil.parseString2Long(date);
        long yesterday = nowday - GlobalConstants.DAY_OF_MILLSECOND;

        //获取时间维度
        DateDimension nowDateDiemnsion = DateDimension.buildDate(nowday, DateEnum.DAY);
        DateDimension yesterdayDateDiemnsion = DateDimension.buildDate(yesterday,DateEnum.DAY);

        IDimension iDimension = new IDimensionImpl();
        //获取时间维度Id
        int nowDateDimensionId = -1;
        int yesterdayDateDimensionId = -1;
        //System.out.println(nowDateDiemnsion+"111111111111");

        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            nowDateDimensionId = iDimension.getDimensionIdByObject(nowDateDiemnsion);
            yesterdayDateDimensionId = iDimension.getDimensionIdByObject(yesterdayDateDiemnsion);

            conn = JdbcUtil.getConn();
            Map<String,Integer> map = new HashMap<String,Integer>();
            //开始判断维度Id是否正确
            //System.out.println("哈哈哈");
            if(nowDateDimensionId > 0){
                ps = conn.prepareStatement(conf.get("other_new_total_browser_user_now_sql"));
                ps.setInt(1,nowDateDimensionId);
                rs = ps.executeQuery();
                while (rs.next()) {
                    int platformId = rs.getInt("platform_dimension_id");
                    int browserId = rs.getInt("browser_dimension_id");
                    int newUsers = rs.getInt("new_install_users");
                    map.put(platformId+"_"+browserId,newUsers);
                }
            }



            //更新
            if(map.size() > 0){
                System.out.println(map.size()+"大小");
                for (Map.Entry<String,Integer> en:map.entrySet()){
                    ps = conn.prepareStatement(conf.get("other_new_total_browser_user_update_sql"));

                    //赋值
                    String[] fields = en.getKey().split("_");
                    ps.setInt(1,nowDateDimensionId);
                    ps.setInt(2,Integer.parseInt(fields[0]));
                    ps.setInt(3,Integer.parseInt(fields[1]));
                    ps.setInt(4,en.getValue());
                    ps.setString(5,conf.get(GlobalConstants.RUNNING_DATE));
                    ps.setInt(6,en.getValue());
                    //执行更新
                    ps.execute();
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            JdbcUtil.close(conn,ps,rs);
        }

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
            Path inpath = new Path("/ods/" + month + "/" + day);
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
