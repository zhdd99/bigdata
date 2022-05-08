package org.apache.spark.examples.sql;


import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.rules.Rule;

/**
 * @author zhangdongdong <zhangdongdong@kuaishou.com>
 * Created on 2022-05-08
 */
public class MyPushDown extends Rule<LogicalPlan> {

    public MyPushDown(SparkSession spark) {
    }

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        logWarning(() -> "==========================自定义优化规则生效=====================");
        return plan;
    }
}
