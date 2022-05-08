## 作业一：构建 SQL 满足如下要求

### 改动一 SqlBase.g4
```antlrv4
statement 
| SHOW VERSION                                                     #showVersion

ansiNonReserved
| VERSION

nonReserved
| VERSION

//============================
// Start of the keywords list
//============================
//--SPARK-KEYWORD-LIST-START
VERSION: 'VERSION' | 'V';
```

### 改动二 SparkSqlParser.scala
```scala
  override def visitShowVersion(ctx: ShowVersionContext): LogicalPlan = withOrigin(ctx) {
    ShowVersionCommand()
  }
```

### 改动三 新增 ShowVersionCommand.scala
```scala
package org.apache.spark.sql.execution.command

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.types.StringType

case class ShowVersionCommand() extends LeafRunnableCommand {

  override val output: Seq[Attribute] =
    Seq(AttributeReference("version", StringType)())

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val sparkVersion = sparkSession.version
    val javaVersion = System.getProperty("java.version")
    val scalaVersion = scala.util.Properties.releaseVersion
    val output = "Spark Version: %s, Java Version: %s, Scala Version: %s"
      .format(sparkVersion, javaVersion, scalaVersion.getOrElse(""))
    Seq(Row(output))
  }
}
```

### 运行结果
```shell
Spark master: local[*], Application Id: local-1652003568664
spark-sql> show version;
Spark Version: 3.2.0, Java Version: 1.8.0_202, Scala Version: 2.12.15
Time taken: 0.025 seconds, Fetched 1 row(s)
```

## 作业二：构建 SQL 满足如下要求

```text
通过 set spark.sql.planChangeLog.level=WARN，查看：

1、构建一条 SQL，同时 apply 下面三条优化规则：
    CombineFilters
    CollapseProject
    BooleanSimplification

2、构建一条 SQL，同时 apply 下面五条优化规则：
    ConstantFolding
    PushDownPredicates
    ReplaceDistinctWithAggregate
    ReplaceExceptWithAntiJoin
    FoldablePropagation
```
### 优化规则
|  规则   | 释义  |
|  ----  | ----  |
| CombineFilters  | Combines two adjacent Filter operators into one, merging the non-redundant conditions into one conjunctive predicate. |
| CollapseProject  | Combines two Project operators into one and perform alias substitution, merging the expressions into one single expression for the following cases. 1. When two Project operators are adjacent. 2. When two Project operators have LocalLimit/Sample/Repartition operator between them and the upper project consists of the same number of columns which is equal or aliasing. GlobalLimit(LocalLimit) pattern is also considered. |
| BooleanSimplification  | Simplifies boolean expressions: 1. Simplifies expressions whose answer can be determined without evaluating both sides. 2. Eliminates / extracts common factors. 3. Merge same expressions 4. Removes Not operator. |
| ConstantFolding  | Replaces Expressions that can be statically evaluated with equivalent Literal values. |
| PushDownPredicates  | The unified version for predicate pushdown of normal operators and joins. This rule improves performance of predicate pushdown for cascading joins such as: Filter-Join-Join-Join. Most predicates can be pushed down in a single pass. |
| ReplaceDistinctWithAggregate  | eplaces logical Distinct operator with an Aggregate operator. SELECT DISTINCT f1, f2 FROM t  ==>  SELECT f1, f2 FROM t GROUP BY f1, f2 |
| ReplaceExceptWithAntiJoin  | Replaces logical Except operator with a left-anti Join operator. SELECT a1, a2 FROM Tab1 EXCEPT SELECT b1, b2 FROM Tab2 ==>  SELECT DISTINCT a1, a2 FROM Tab1 LEFT ANTI JOIN Tab2 ON a1<=>b1 AND a2<=>b2 Note: 1. This rule is only applicable to EXCEPT DISTINCT. Do not use it for EXCEPT ALL. 2. This rule has to be done after de-duplicating the attributes; otherwise, the generated join conditions will be incorrect. |
| FoldablePropagation  | Replace attributes with aliases of the original foldable expressions if possible. Other optimizations will take advantage of the propagated foldable expressions. For example, this rule can optimize |

### 前置准备
```sql
CREATE TABLE people (`id` INT, `name` STRING, `age` INT, `phone` STRING, `adress` STRING) USING parquet;
set spark.sql.planChangeLog.level=WARN;
```

### sql 一
```sql
select p.name from (select name, address, age from people where 1=1 and age > 5) p where p.age<30
```
### 运行效果
```shell
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.PushDownPredicates ===
 Project [name#11]                                                        Project [name#11]
!+- Filter (age#12 < 30)                                                  +- Project [name#11, address#14, age#12]
!   +- Project [name#11, address#14, age#12]                                 +- Filter (((1 = 1) AND (age#12 > 5)) AND (age#12 < 30))
!      +- Filter ((1 = 1) AND (age#12 > 5))                                     +- Relation[id#10,name#11,age#12,phone#13,address#14] parquet
!         +- Relation[id#10,name#11,age#12,phone#13,address#14] parquet   
           
22/05/08 19:07:09 WARN PlanChangeLogger: 
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.ConstantFolding ===
 Project [name#11]                                                  Project [name#11]
!+- Filter (((1 = 1) AND (age#12 > 5)) AND (age#12 < 30))           +- Filter ((true AND (age#12 > 5)) AND (age#12 < 30))
    +- Relation[id#10,name#11,age#12,phone#13,address#14] parquet      +- Relation[id#10,name#11,age#12,phone#13,address#14] parquet
           
22/05/08 19:07:09 WARN PlanChangeLogger: 
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.BooleanSimplification ===
 Project [name#11]                                                  Project [name#11]
!+- Filter ((true AND (age#12 > 5)) AND (age#12 < 30))              +- Filter ((age#12 > 5) AND (age#12 < 30))
    +- Relation[id#10,name#11,age#12,phone#13,address#14] parquet      +- Relation[id#10,name#11,age#12,phone#13,address#14] parquet
```

### sql 二
```sql
(select p.address , p.age + (100 + 80) , Now() z  from (select distinct name, age , address from people) a  where p.age>10 order by z) except (select p.address, p.age + (100 + 80), Now() z  from (  select distinct name, age , address from people ) a  where p.name="saya");
```
### 运行效果
```shell
22/05/08 19:16:55 WARN PlanChangeLogger: 
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.ConstantFolding ===
 Aggregate [address#14, (age + (100 + 80))#20, 1652008615744000], [address#14, (age + (100 + 80))#20, 1652008615744000 AS z#18]                                 Aggregate [address#14, (age + (100 + 80))#20, 1652008615744000], [address#14, (age + (100 + 80))#20, 1652008615744000 AS z#18]
 +- Sort [1652008615744000 ASC NULLS FIRST], true                                                                                                               +- Sort [1652008615744000 ASC NULLS FIRST], true
!   +- Aggregate [name#11, age#12, address#14], [address#14, (age#12 + (100 + 80)) AS (age + (100 + 80))#20, 1652008615744000 AS z#18]                             +- Aggregate [name#11, age#12, address#14], [address#14, (age#12 + 180) AS (age + (100 + 80))#20, 1652008615744000 AS z#18]
       +- Project [name#11, age#12, address#14]                                                                                                                       +- Project [name#11, age#12, address#14]
!         +- Join LeftAnti, (((address#14 <=> address#26) AND ((age#12 + (100 + 80)) <=> (age + (100 + 80))#21)) AND (1652008615744000 <=> 1652008615744000))            +- Join LeftAnti, (((address#14 <=> address#26) AND ((age#12 + 180) <=> (age + (100 + 80))#21)) AND true)
             :- Project [name#11, age#12, address#14]                                                                                                                       :- Project [name#11, age#12, address#14]
             :  +- Filter (age#12 > 10)                                                                                                                                     :  +- Filter (age#12 > 10)
             :     +- Relation[id#10,name#11,age#12,phone#13,address#14] parquet                                                                                            :     +- Relation[id#10,name#11,age#12,phone#13,address#14] parquet
!            +- Aggregate [name#23, age#24, address#26], [address#26, (age#24 + (100 + 80)) AS (age + (100 + 80))#21, 1652008615744000 AS z#19]                             +- Aggregate [name#23, age#24, address#26], [address#26, (age#24 + 180) AS (age + (100 + 80))#21, 1652008615744000 AS z#19]
                +- Project [name#23, age#24, address#26]                                                                                                                       +- Project [name#23, age#24, address#26]
                   +- Filter (name#23 = saya)                                                                                                                                     +- Filter (name#23 = saya)
                      +- Relation[id#22,name#23,age#24,phone#25,address#26] parquet    
22/05/08 19:16:55 WARN PlanChangeLogger: 
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.PushDownPredicates ===
 Aggregate [address#14, (age + (100 + 80))#20, z#18], [address#14, (age + (100 + 80))#20, z#18]                                Aggregate [address#14, (age + (100 + 80))#20, z#18], [address#14, (age + (100 + 80))#20, z#18]
 +- Join LeftAnti, (((address#14 <=> address#26) AND ((age + (100 + 80))#20 <=> (age + (100 + 80))#21)) AND (z#18 <=> z#19))   +- Join LeftAnti, (((address#14 <=> address#26) AND ((age + (100 + 80))#20 <=> (age + (100 + 80))#21)) AND (z#18 <=> z#19))
    :- Sort [z#18 ASC NULLS FIRST], true                                                                                          :- Sort [z#18 ASC NULLS FIRST], true
    :  +- Project [address#14, (age#12 + (100 + 80)) AS (age + (100 + 80))#20, 1652008615744000 AS z#18]                          :  +- Project [address#14, (age#12 + (100 + 80)) AS (age + (100 + 80))#20, 1652008615744000 AS z#18]
!   :     +- Filter (age#12 > 10)                                                                                                 :     +- Aggregate [name#11, age#12, address#14], [name#11, age#12, address#14]
!   :        +- Aggregate [name#11, age#12, address#14], [name#11, age#12, address#14]                                            :        +- Project [name#11, age#12, address#14]
!   :           +- Project [name#11, age#12, address#14]                                                                          :           +- Filter (age#12 > 10)
    :              +- Relation[id#10,name#11,age#12,phone#13,address#14] parquet                                                  :              +- Relation[id#10,name#11,age#12,phone#13,address#14] parquet
    +- Project [address#26, (age#24 + (100 + 80)) AS (age + (100 + 80))#21, 1652008615744000 AS z#19]                             +- Project [address#26, (age#24 + (100 + 80)) AS (age + (100 + 80))#21, 1652008615744000 AS z#19]
!      +- Filter (name#23 = saya)                                                                                                    +- Aggregate [name#23, age#24, address#26], [name#23, age#24, address#26]
!         +- Aggregate [name#23, age#24, address#26], [name#23, age#24, address#26]                                                     +- Project [name#23, age#24, address#26]
!            +- Project [name#23, age#24, address#26]                                                                                      +- Filter (name#23 = saya)
                +- Relation[id#22,name#23,age#24,phone#25,address#26] parquet   
22/05/08 19:16:55 WARN PlanChangeLogger: 
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.ReplaceDistinctWithAggregate ===
!Distinct                                                                                                                      Aggregate [address#14, (age + (100 + 80))#20, z#18], [address#14, (age + (100 + 80))#20, z#18]
 +- Join LeftAnti, (((address#14 <=> address#26) AND ((age + (100 + 80))#20 <=> (age + (100 + 80))#21)) AND (z#18 <=> z#19))   +- Join LeftAnti, (((address#14 <=> address#26) AND ((age + (100 + 80))#20 <=> (age + (100 + 80))#21)) AND (z#18 <=> z#19))
    :- Sort [z#18 ASC NULLS FIRST], true                                                                                          :- Sort [z#18 ASC NULLS FIRST], true
    :  +- Project [address#14, (age#12 + (100 + 80)) AS (age + (100 + 80))#20, 1652008615744000 AS z#18]                          :  +- Project [address#14, (age#12 + (100 + 80)) AS (age + (100 + 80))#20, 1652008615744000 AS z#18]
    :     +- Filter (age#12 > 10)                                                                                                 :     +- Filter (age#12 > 10)
!   :        +- Distinct                                                                                                          :        +- Aggregate [name#11, age#12, address#14], [name#11, age#12, address#14]
    :           +- Project [name#11, age#12, address#14]                                                                          :           +- Project [name#11, age#12, address#14]
    :              +- Relation[id#10,name#11,age#12,phone#13,address#14] parquet                                                  :              +- Relation[id#10,name#11,age#12,phone#13,address#14] parquet
    +- Project [address#26, (age#24 + (100 + 80)) AS (age + (100 + 80))#21, 1652008615744000 AS z#19]                             +- Project [address#26, (age#24 + (100 + 80)) AS (age + (100 + 80))#21, 1652008615744000 AS z#19]
       +- Filter (name#23 = saya)                                                                                                    +- Filter (name#23 = saya)
!         +- Distinct                                                                                                                   +- Aggregate [name#23, age#24, address#26], [name#23, age#24, address#26]
             +- Project [name#23, age#24, address#26]                                                                                      +- Project [name#23, age#24, address#26]
                +- Relation[id#22,name#23,age#24,phone#25,address#26] parquet   
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.ReplaceExceptWithAntiJoin ===
!Except false                                                                                           Distinct
!:- Sort [z#18 ASC NULLS FIRST], true                                                                   +- Join LeftAnti, (((address#14 <=> address#26) AND ((age + (100 + 80))#20 <=> (age + (100 + 80))#21)) AND (z#18 <=> z#19))
!:  +- Project [address#14, (age#12 + (100 + 80)) AS (age + (100 + 80))#20, 1652008615744000 AS z#18]      :- Sort [z#18 ASC NULLS FIRST], true
!:     +- Filter (age#12 > 10)                                                                             :  +- Project [address#14, (age#12 + (100 + 80)) AS (age + (100 + 80))#20, 1652008615744000 AS z#18]
!:        +- Distinct                                                                                      :     +- Filter (age#12 > 10)
!:           +- Project [name#11, age#12, address#14]                                                      :        +- Distinct
!:              +- Relation[id#10,name#11,age#12,phone#13,address#14] parquet                              :           +- Project [name#11, age#12, address#14]
!+- Project [address#26, (age#24 + (100 + 80)) AS (age + (100 + 80))#21, 1652008615744000 AS z#19]         :              +- Relation[id#10,name#11,age#12,phone#13,address#14] parquet
!   +- Filter (name#23 = saya)                                                                             +- Project [address#26, (age#24 + (100 + 80)) AS (age + (100 + 80))#21, 1652008615744000 AS z#19]
!      +- Distinct                                                                                            +- Filter (name#23 = saya)
!         +- Project [name#23, age#24, address#26]                                                               +- Distinct
!            +- Relation[id#22,name#23,age#24,phone#25,address#26] parquet                                          +- Project [name#23, age#24, address#26]
!                                                                                                                      +- Relation[id#22,name#23,age#24,phone#25,address#26] parquet
22/05/08 19:16:55 WARN PlanChangeLogger: 
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.FoldablePropagation ===
!Aggregate [address#14, (age + (100 + 80))#20, z#18], [address#14, (age + (100 + 80))#20, z#18]                                                     Aggregate [address#14, (age + (100 + 80))#20, 1652008615744000], [address#14, (age + (100 + 80))#20, 1652008615744000 AS z#18]
!+- Sort [z#18 ASC NULLS FIRST], true                                                                                                               +- Sort [1652008615744000 ASC NULLS FIRST], true
    +- Aggregate [name#11, age#12, address#14], [address#14, (age#12 + (100 + 80)) AS (age + (100 + 80))#20, 1652008615744000 AS z#18]                 +- Aggregate [name#11, age#12, address#14], [address#14, (age#12 + (100 + 80)) AS (age + (100 + 80))#20, 1652008615744000 AS z#18]
       +- Project [name#11, age#12, address#14]                                                                                                           +- Project [name#11, age#12, address#14]
!         +- Join LeftAnti, (((address#14 <=> address#26) AND ((age#12 + (100 + 80)) <=> (age + (100 + 80))#21)) AND (1652008615744000 <=> z#19))            +- Join LeftAnti, (((address#14 <=> address#26) AND ((age#12 + (100 + 80)) <=> (age + (100 + 80))#21)) AND (1652008615744000 <=> 1652008615744000))
             :- Project [name#11, age#12, address#14]                                                                                                           :- Project [name#11, age#12, address#14]
             :  +- Filter (age#12 > 10)                                                                                                                         :  +- Filter (age#12 > 10)
             :     +- Relation[id#10,name#11,age#12,phone#13,address#14] parquet                                                                                :     +- Relation[id#10,name#11,age#12,phone#13,address#14] parquet
             +- Aggregate [name#23, age#24, address#26], [address#26, (age#24 + (100 + 80)) AS (age + (100 + 80))#21, 1652008615744000 AS z#19]                 +- Aggregate [name#23, age#24, address#26], [address#26, (age#24 + (100 + 80)) AS (age + (100 + 80))#21, 1652008615744000 AS z#19]
                +- Project [name#23, age#24, address#26]                                                                                                           +- Project [name#23, age#24, address#26]
                   +- Filter (name#23 = saya)                                                                                                                         +- Filter (name#23 = saya)
                      +- Relation[id#22,name#23,age#24,phone#25,address#26] parquet   
```

## 作业三
### 启动命令
```shell
bin/spark-sql --jars /Users/zhdd99/KuaishouProjects/bigdata/spark/examples/target/original-spark-examples_2.12-3.2.0.jar --conf spark.sql.extensions=org.apache.spark.examples.sql.MyPushDownExtention
```

### 运行效果
```shell
22/05/08 20:48:50 WARN MyPushDown: ==========================自定义优化规则生效=====================
22/05/08 20:48:50 WARN MyPushDown: ==========================自定义优化规则生效=====================
22/05/08 20:48:50 WARN PlanChangeLogger: 
=== Result of Batch Operator Optimization before Inferring Filters ===
!Project [id#10, name#11, age#12, phone#13, address#14]             Filter (age#12 > 30)
!+- Filter (age#12 > 30)                                            +- Relation[id#10,name#11,age#12,phone#13,address#14] parquet
!   +- Relation[id#10,name#11,age#12,phone#13,address#14] parquet   
          
22/05/08 20:48:51 WARN PlanChangeLogger: 
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.InferFiltersFromConstraints ===
!Filter (age#12 > 30)                                            Filter (isnotnull(age#12) AND (age#12 > 30))
 +- Relation[id#10,name#11,age#12,phone#13,address#14] parquet   +- Relation[id#10,name#11,age#12,phone#13,address#14] parquet
           
22/05/08 20:48:51 WARN PlanChangeLogger: 
=== Result of Batch Infer Filters ===
!Filter (age#12 > 30)                                            Filter (isnotnull(age#12) AND (age#12 > 30))
 +- Relation[id#10,name#11,age#12,phone#13,address#14] parquet   +- Relation[id#10,name#11,age#12,phone#13,address#14] parquet
          
22/05/08 20:48:51 WARN MyPushDown: ==========================自定义优化规则生效=====================
22/05/08 20:48:51 WARN PlanChangeLogger: Batch Operator Optimization after Inferring Filters has no effect.
```


