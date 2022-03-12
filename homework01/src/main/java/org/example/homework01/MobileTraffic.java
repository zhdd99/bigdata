package org.example.homework01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

public class MobileTraffic {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("mobiletraffic <inDir> <outDir>");
            ToolRunner.printGenericCommandUsage(System.out);
            System.exit(2);
        }
        Configuration configuration = new Configuration();
        //1、获取job信息
        Job job = Job.getInstance(configuration);
        //2、获取jar的存储路径
        job.setJarByClass(MobileTraffic.class);
        //3、关联map和reduce的class类
        job.setMapperClass(TrafficMapper.class);
        job.setReducerClass(TrafficReducer.class);
        job.setNumReduceTasks(1);
        //4、设置map阶段输出的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TrafficBean.class);
        //5、设置最后输出数据的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TrafficBean.class);
        //6、设置输入数据的路径和输出数据的路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //7、提交
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

}
