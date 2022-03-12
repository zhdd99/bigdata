package org.example.homework01;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author zhangdongdong <zhangdongdong@kuaishou.com>
 * Created on 2022-03-12
 */
public class TrafficMapper extends Mapper<LongWritable, Text, Text, TrafficBean> {
    TrafficBean bean = new TrafficBean();
    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        //1 获取一行数据
        String line = value.toString();
        //2 截取字段
        String[] fields = line.split("\t");
        //3 封装bean对象以及获取电话号
        String phoneNum = fields[1];
        long upFlow = Long.parseLong(fields[fields.length - 3]);
        long downFlow = Long.parseLong(fields[fields.length - 2]);
        bean.set(upFlow, downFlow);
        k.set(phoneNum);
        //4 写出去
        context.write(k, bean);
    }
}
