package org.apache.spark.examples.sql;

import org.apache.spark.sql.SparkSessionExtensions;

import scala.Function1;
import scala.runtime.BoxedUnit;

/**
 * @author zhangdongdong <zhangdongdong@kuaishou.com>
 * Created on 2022-05-08
 */
public class MyPushDownExtention implements Function1<SparkSessionExtensions, BoxedUnit> {
    @Override
    public BoxedUnit apply(SparkSessionExtensions extensions) {
        extensions.injectOptimizerRule(MyPushDown::new);
        return BoxedUnit.UNIT;
    }
}
