package mr;


import IpAnalysis.LogUtil;
import common.Constants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;


public class Etl2HdfsMapper extends Mapper<LongWritable, Text,LogWritable, NullWritable> {
    private static Logger logger = Logger.getLogger(Etl2HdfsMapper.class);
    private static LogWritable k = new LogWritable();
    private static int inputRecords,filterRecords,outputRecords;
    @Override
  /**
  * @todo 对数据进行解析,为以后MR存储到数据库做准备
  * @author CaoXueCheng
  * @date 2018/11/2 20:58
  * @method map
  * @param [key, value, context]
  * @return void
  */
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        inputRecords++;
        if (StringUtils.isEmpty(line)){
            filterRecords++;
            return;
        }

        //调用LogUtil中的parserLog方法，返回map，然后循环map将数据分别输出
        Map<String, String> map = LogUtil.parserLog(line);

        //可以将数据根据事件分别输出
        String eventName = map.get(Constants.LOG_EVENT_NAME);
        //获取到事件对应的枚举值
        Constants.EventEnum event = Constants.EventEnum.valueOfAlias(eventName);
        switch (event){
            case LANUCH:
            case PAGEVIEW:
            case EVENT:
            case CHARGESUCCESS:
            case CHARGEREQUEST:
            case CHARGEREFUND:
                handleLog(map,context);  //处理输出
                break;
            default:
                break;
        }
    }

   

    
    private void handleLog(Map<String, String> map, Context context) {


        try {
            for (Map.Entry<String,String> en: map.entrySet() ) {
                //this.k.setB_iev(en.getValue());
                switch (en.getKey()){
                    case "ver": this.k.setVer(en.getValue()); break;
                    case "s_time": this.k.setS_time(en.getValue()); break;
                    case "en": this.k.setEn(en.getValue()); break;
                    case "u_ud": this.k.setU_ud(en.getValue()); break;
                    case "u_mid": this.k.setU_mid(en.getValue()); break;
                    case "u_sd": this.k.setU_sd(en.getValue()); break;
                    case "c_time": this.k.setC_time(en.getValue()); break;
                    case "l": this.k.setL(en.getValue()); break;
                    case "b_iev": this.k.setB_iev(en.getValue()); break;
                    case "b_rst": this.k.setB_rst(en.getValue()); break;
                    case "p_url": this.k.setP_url(en.getValue()); break;
                    case "p_ref": this.k.setP_ref(en.getValue()); break;
                    case "tt": this.k.setTt(en.getValue()); break;
                    case "pl": this.k.setPl(en.getValue()); break;
                    case "ip": this.k.setIp(en.getValue()); break;
                    case "oid": this.k.setOid(en.getValue()); break;
                    case "on": this.k.setOn(en.getValue()); break;
                    case "cua": this.k.setCua(en.getValue()); break;
                    case "cut": this.k.setCut(en.getValue()); break;
                    case "pt": this.k.setPt(en.getValue()); break;
                    case "ca": this.k.setCa(en.getValue()); break;
                    case "ac": this.k.setAc(en.getValue()); break;
                    case "kv_": this.k.setKv_(en.getValue()); break;
                    case "du": this.k.setDu(en.getValue()); break;
                    case "browserName": this.k.setBrowserName(en.getValue()); break;
                    case "browserVersion": this.k.setBrowserVersion(en.getValue()); break;
                    case "osName": this.k.setOsName(en.getValue()); break;
                    case "osVersion": this.k.setOsVersion(en.getValue()); break;
                    case "country": this.k.setCountry(en.getValue()); break;
                    case "province": this.k.setProvince(en.getValue()); break;
                    case "city": this.k.setCity(en.getValue()); break;
                    default:break;
                }
            }
            outputRecords++;
            context.write(k,NullWritable.get());
        } catch (Exception e) {
            logger.error("etl最终输出错误",e);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        logger.info("inputRecords:" + inputRecords +
                "   filterRecords:" + filterRecords +
                "   outputRecords:" + outputRecords);
    }
    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://hadoop01:9000");
        //conf.set("mapreduce.framework.name","local");
        // 获取命令行的参数
//        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
//        if (otherArgs.length != 3) {
//            System.err.println("参数传递错误");
//            System.exit(1);
//        }
        //给路径赋值
//        INPUT_PATH1 = otherArgs[0];
//        INPUT_PATH2 = otherArgs[1];
//        OUT_PATH = otherArgs[2];
//        创建文件系统,如果文件存在就删除
//         创建文件系统
        //FileSystem fileSystem = FileSystem.get(new URI(args[2]), conf);

        // 添加到内存中的文件(随便添加多少个文件)
        //DistributedCache.addCacheFile(new Path(args[0]).toUri(), conf);
        //创建任务
        Job job = Job.getInstance(conf,"qlis");
        // 设置输入目录和设置输入数据格式化的类
        FileInputFormat.setInputPaths(job, "/logs/11/06");
        // job.setInputFormatClass(TextInputFormat.class);
        //打成jar包运行
        job.setJarByClass(Etl2HdfsMapper.class);
        //指定运行的map类
        job.setMapperClass(Etl2HdfsMapper.class);
        job.setMapOutputKeyClass(LogWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        //设置reduce的相关参数(reduce个数)
        job.setNumReduceTasks(0);
        //结果的输出路径
        FileOutputFormat.setOutputPath(job, new Path("/ods1"));
        // 提交作业 退出
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
