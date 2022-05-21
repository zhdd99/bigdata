## 作业：
## 实现 Compact table command

1、要求： 添加 compact table 命令，用于合并小文件，例如表 test1 总共有 50000 个文件，每个 1MB，通过该命令，合成为 500 个文件，每个约 100MB。

2、语法： COMPACT TABLE table_identify [partitionSpec] [INTO fileNum FILES]；

3、说明： 基本要求是完成以下功能：

+ COMPACT TABLE test1 INTO 500 FILES；
+ 如果添加 partitionSpec，则只合并指定的 partition 目录的文件；
+ 如果不加 into fileNum files，则把表中的文件合并成 128MB 大小。

4、本次作业属于 SparkSQL 的内容，请根据课程内容完成作业。

5、代码参考：
```antlrv4
SqlBase.g4

| COMPACT TABLE target=tableIdentifier partitionSpec?
        (INTO fileNum=INTEGER_VALUE FILES)?                              #compactTable
```

## 答：

### 改动一 SqlBase.g4
```antlrv4
statement 
| COMPACT TABLE target=tableIdentifier partitionSpec?
        (INTO fileNum=INTEGER_VALUE FILES)?                              #compactTable
        
ansiNonReserved
| FILES

nonReserved
| FILES

//============================
// Start of the keywords list
//============================
//--SPARK-KEYWORD-LIST-START
FILES: 'FILES';
```

### 改动二 SparkSqlParser.scala
```scala
override def visitCompactTable(ctx: CompactTableContext): LogicalPlan = withOrigin(ctx) {
  val table: TableIdentifier = visitTableIdentifier(ctx.tableIdentifier())
  val fileNum: Option[Int] = Option(ctx.INTEGER_VALUE().getText.toInt)
  CompactTableCommand(table, fileNum)
}
```

### 改动三 新增 CompactTableCommand.scala
```scala
package org.apache.spark.sql.execution.command

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.types.StringType

case class CompactTableCommand(table: TableIdentifier, fileNum: Option[Int])
  extends LeafRunnableCommand {

  override def output: Seq[Attribute] = Seq(AttributeReference("no_return", StringType, false)())

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val dataDF: DataFrame = sparkSession.table(table)
    val num: Int = fileNum match {
      case Some(i) => i
      case _ =>
        (sparkSession
          .sessionState
          .executePlan(dataDF.queryExecution.logical)
          .optimizedPlan
          .stats.sizeInBytes / (1024L * 1024L * 128L)
          ).toInt
    }
    log.warn(s"fileNum is $num")
    val tmpTableName = table.identifier + "_tmp"
    dataDF.write.mode(SaveMode.Overwrite).saveAsTable(tmpTableName)
    sparkSession.table(tmpTableName).repartition(num).write.mode(SaveMode.Overwrite)
      .saveAsTable(table.identifier)
    sparkSession.sql(s"drop table if exists $tmpTableName")
    log.warn("Compacte Table Completed.")
    Seq()
  }
}
```

## 作业调试：

### 编译spark源码
```shell
[INFO] ------------------------------------------------------------------------
[INFO] Reactor Summary for Spark Project Parent POM 3.2.0:
[INFO] 
[INFO] Spark Project Parent POM ........................... SUCCESS [  5.628 s]
[INFO] Spark Project Tags ................................. SUCCESS [ 10.908 s]
[INFO] Spark Project Sketch ............................... SUCCESS [  9.725 s]
[INFO] Spark Project Local DB ............................. SUCCESS [  6.960 s]
[INFO] Spark Project Networking ........................... SUCCESS [ 11.564 s]
[INFO] Spark Project Shuffle Streaming Service ............ SUCCESS [  8.967 s]
[INFO] Spark Project Unsafe ............................... SUCCESS [ 11.824 s]
[INFO] Spark Project Launcher ............................. SUCCESS [  5.985 s]
[INFO] Spark Project Core ................................. SUCCESS [03:52 min]
[INFO] Spark Project ML Local Library ..................... SUCCESS [ 48.402 s]
[INFO] Spark Project GraphX ............................... SUCCESS [ 53.004 s]
[INFO] Spark Project Streaming ............................ SUCCESS [01:28 min]
[INFO] Spark Project Catalyst ............................. SUCCESS [04:24 min]
[INFO] Spark Project SQL .................................. SUCCESS [05:27 min]
[INFO] Spark Project ML Library ........................... SUCCESS [02:44 min]
[INFO] Spark Project Tools ................................ SUCCESS [  8.685 s]
[INFO] Spark Project Hive ................................. SUCCESS [01:46 min]
[INFO] Spark Project REPL ................................. SUCCESS [ 24.358 s]
[INFO] Spark Project Hive Thrift Server ................... SUCCESS [ 52.667 s]
[INFO] Spark Project Assembly ............................. SUCCESS [  5.716 s]
[INFO] Kafka 0.10+ Token Provider for Streaming ........... SUCCESS [ 23.977 s]
[INFO] Spark Integration for Kafka 0.10 ................... SUCCESS [ 31.167 s]
[INFO] Kafka 0.10+ Source for Structured Streaming ........ SUCCESS [ 49.363 s]
[INFO] Spark Project Examples ............................. SUCCESS [ 54.010 s]
[INFO] Spark Integration for Kafka 0.10 Assembly .......... SUCCESS [ 11.087 s]
[INFO] Spark Avro ......................................... SUCCESS [ 42.369 s]
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  27:42 min
[INFO] Finished at: 2022-05-21T22:26:06+08:00
[INFO] ------------------------------------------------------------------------

Process finished with exit code 0
```
### 运行./bin/spark-sql
```shell
zhdd99@bogon spark % ./bin/spark-sql 
WARNING: An illegal reflective access operation has occurred
WARNING: Illegal reflective access by org.apache.spark.unsafe.Platform (file:/Users/zhdd99/KuaishouProjects/bigdata/spark/assembly/target/scala-2.12/jars/spark-unsafe_2.12-3.2.0.jar) to constructor java.nio.DirectByteBuffer(long,int)
WARNING: Please consider reporting this to the maintainers of org.apache.spark.unsafe.Platform
WARNING: Use --illegal-access=warn to enable warnings of further illegal reflective access operations
WARNING: All illegal access operations will be denied in a future release
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
22/05/21 23:03:26 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
22/05/21 23:03:29 WARN HiveConf: HiveConf of name hive.stats.jdbc.timeout does not exist
22/05/21 23:03:29 WARN HiveConf: HiveConf of name hive.stats.retries.wait does not exist
22/05/21 23:03:31 WARN ObjectStore: Version information not found in metastore. hive.metastore.schema.verification is not enabled so recording the schema version 2.3.0
22/05/21 23:03:31 WARN ObjectStore: setMetaStoreSchemaVersion called but recording version is disabled: version = 2.3.0, comment = Set by MetaStore zhdd99@192.168.1.104
22/05/21 23:03:31 WARN ObjectStore: Failed to get database default, returning NoSuchObjectException
Spark master: local[*], Application Id: local-1653145408148
spark-sql> show tables;
22/05/21 23:04:27 WARN ObjectStore: Failed to get database global_temp, returning NoSuchObjectException
Time taken: 2.023 seconds
spark-sql> show v;
Spark Version: 3.2.0, Java Version: 11.0.13, Scala Version: 2.12.15
Time taken: 0.296 seconds, Fetched 1 row(s)
```
### 创建一张测试表，插入一些数据，制造一些小文件
```shell
spark-sql> CREATE TABLE people (`id` INT, `name` STRING, `age` INT) USING parquet;
Time taken: 0.074 seconds
spark-sql> insert into people values (1, 'zhangdongdong', 32);
Time taken: 0.196 seconds
spark-sql> insert into people values (2, 'qiyu', 32);
Time taken: 0.174 seconds
spark-sql> insert into people values (3, 'qichunli', 59);
Time taken: 0.161 seconds
spark-sql> insert into people values (4, 'jihong', 55);
Time taken: 0.156 seconds
spark-sql> insert into people values (5, 'zhangjiayu', 2);
Time taken: 0.166 seconds
spark-sql> insert into people values (6, '6', 6);
Time taken: 0.153 seconds
spark-sql> insert into people values (7, '7', 7);
Time taken: 0.168 seconds
spark-sql> insert into people values (8, '8', 8);
Time taken: 0.148 seconds
spark-sql> insert into people values (9, '9', 9);
Time taken: 0.198 seconds
spark-sql> insert into people values (10, '10', 10);
Time taken: 0.16 seconds
spark-sql> select * from people;
1	zhangdongdong	32
5	zhangjiayu	2
3	qichunli	59
4	jihong	55
2	qiyu	32
10	10	10
8	8	8
9	9	9
7	7	7
6	6	6
Time taken: 0.101 seconds, Fetched 10 row(s)
```
### 看下是否有对应的小文件(10条记录，10个小文件)
```shell
zhdd99@bogon spark-warehouse % tree 
.
└── people
    ├── _SUCCESS
    ├── part-00000-124b12b8-d40d-4c4a-8309-c570990d9ac8-c000.snappy.parquet
    ├── part-00000-139b394c-db26-425d-954a-125650a7b16f-c000.snappy.parquet
    ├── part-00000-34abe1ed-5ad8-4986-8515-036dcdfe6429-c000.snappy.parquet
    ├── part-00000-3d04e99f-d41a-42d4-a6de-ab1f305f83ca-c000.snappy.parquet
    ├── part-00000-63de4f07-3a0a-4682-9de7-e5acc2d81b86-c000.snappy.parquet
    ├── part-00000-84a5f2a1-0a49-4cbb-80e4-2d29de6c2794-c000.snappy.parquet
    ├── part-00000-90213855-9c30-422a-88d9-26bc0347ed47-c000.snappy.parquet
    ├── part-00000-907709b7-b95b-4e6b-aea0-5678b21cd17f-c000.snappy.parquet
    ├── part-00000-f186b432-a0b7-4499-ba22-2f6eb373e556-c000.snappy.parquet
    └── part-00000-fb99cfed-6266-43d2-ba89-ae20b89b28e2-c000.snappy.parquet

1 directory, 11 files
```

### 运行合并文件的命令
```shell
spark-sql> COMPACT TABLE people INTO 2 FILES;
22/05/22 00:18:31 WARN CompactTableCommand: fileNum is 2
22/05/22 00:18:32 WARN CompactTableCommand: Compacte Table Completed.
Time taken: 0.715 seconds
```

### 查看最终的结果(合并成2个文件，每个文件各5条数据)

```shell
zhdd99@bogon spark-warehouse % tree
.
└── people
    ├── _SUCCESS
    ├── part-00000-6ab6449c-704e-463c-9d19-c621ac10b1e6-c000.snappy.parquet
    └── part-00001-6ab6449c-704e-463c-9d19-c621ac10b1e6-c000.snappy.parquet

1 directory, 3 files

```
