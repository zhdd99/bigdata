package org.example.homework01;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author zhangdongdong <zhangdongdong@kuaishou.com>
 * Created on 2022-03-12
 */
public class TrafficReducer extends Reducer<Text, TrafficBean, Text, TrafficBean> {
    @Override
    protected void reduce(Text key, Iterable<TrafficBean> values, Context context)
            throws IOException, InterruptedException {
        // 计算总的流量
        long sum_upFlow = 0;
        long sum_downFlow = 0;
        for (TrafficBean bean : values) {
            sum_upFlow += bean.getUpFlow();
            sum_downFlow += bean.getDownFlow();
        }
        //输出
        context.write(key, new TrafficBean(sum_upFlow, sum_downFlow));
    }
}
