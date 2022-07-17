## 作业
report(transactions).executeInsert(“spend_report”);
将 transactions 表经过 report 函数处理后写入到 spend_report 表。

每分钟（或小时）计算在五分钟（或小时）内每个账号的平均交易金额（滑动窗口）？使用分钟还是小时作为单位均可。


## 一、补全代码

### 补充report
```java
public static Table report(Table transactions) {
    Table table = transactions.window(
                    Slide.over(lit(5).minute()).every(lit(1).minute()).on($("transaction_time")).as("log_ts"))
            .groupBy($("account_id"), $("log_ts"))
            .select($("account_id"), $("log_ts").start().as("log_ts"), $("amount").avg().as("amount"));

    return table;
}
```

### 修改kafka消费点位，避免被反压

```java
tEnv.executeSql("CREATE TABLE transactions (\n" +
                "    account_id  BIGINT,\n" +
                "    amount      BIGINT,\n" +
                "    transaction_time TIMESTAMP(3),\n" +
                "    WATERMARK FOR transaction_time AS transaction_time - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic'     = 'transactions',\n" +
                "    'properties.bootstrap.servers' = 'kafka:9092',\n" +
                "    'scan.startup.mode' = 'latest-offset',\n" +   // 从最新的地方开始消费
                "    'format'    = 'csv'\n" +
                ")");
```

## 二、编译打包代码

```shell
docker-compose build
```

## 三、启动环境代码：
```shell
docker-compose up -d
```

## 四、验证结果

```shell
zhdd99@zhangdongdong table-walkthrough % docker-compose exec mysql mysql -Dsql-demo -usql-demo -pdemo-sql
mysql: [Warning] Using a password on the command line interface can be insecure.
Reading table information for completion of table and column names
You can turn off this feature to get a quicker startup with -A

Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 9
Server version: 8.0.19 MySQL Community Server - GPL

Copyright (c) 2000, 2020, Oracle and/or its affiliates. All rights reserved.

Oracle is a registered trademark of Oracle Corporation and/or its
affiliates. Other names may be trademarks of their respective
owners.

Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

mysql> use sql-demo;
Database changed
mysql> select count(*) from spend_report;
+----------+
| count(*) |
+----------+
|   217575 |
+----------+
1 row in set (0.01 sec)

mysql> select count(*) from spend_report;
+----------+
| count(*) |
+----------+
|   218583 |
+----------+
1 row in set (0.01 sec)

mysql> select count(*) from spend_report;
+----------+
| count(*) |
+----------+
|   219482 |
+----------+
1 row in set (0.01 sec)

mysql> select count(*) from spend_report;
+----------+
| count(*) |
+----------+
|   220970 |
+----------+
1 row in set (0.01 sec)
```

## 五、停止 Docker 环境

```shell
docker-compose down -v
```