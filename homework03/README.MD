## 创建表

### 用户
```sql
CREATE EXTERNAL TABLE `t_user_zdd`(
  `userid` int, 
  `sex` string, 
  `age` int, 
  `occupation` string, 
  `zipcode` string)
ROW FORMAT SERDE  'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' 
WITH SERDEPROPERTIES ('field.delim'='::') 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/data/hive/users';
```

### 电影
```sql
CREATE EXTERNAL TABLE `t_movie_zdd`(
  `movieid` int, 
  `moviename` string, 
  `movietype` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' 
WITH SERDEPROPERTIES ('field.delim'='::') 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/data/hive/movies';
```

### 评分
```sql
CREATE EXTERNAL TABLE `t_rating_zdd`(
  `userid` int, 
  `movieid` int, 
  `rate` int, 
  `times` bigint)
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' 
WITH SERDEPROPERTIES ('field.delim'='::') 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/data/hive/ratings';
```

### 运行结果
```shell
hive> CREATE EXTERNAL TABLE `t_user_zdd`(
    >   `userid` int, 
    >   `sex` string, 
    >   `age` int, 
    >   `occupation` string, 
    >   `zipcode` string)
    > COMMENT '观众表'
    > ROW FORMAT SERDE  'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' 
    > WITH SERDEPROPERTIES ('field.delim'='::') 
    > STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
    > OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    > LOCATION '/data/hive/users'
    > ;
OK
Time taken: 0.6 seconds
hive> CREATE EXTERNAL TABLE `t_movie_zdd`(
    >   `movieid` int, 
    >   `moviename` string, 
    >   `movietype` string)
    > ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' 
    > WITH SERDEPROPERTIES ('field.delim'='::') 
    > STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
    > OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    > LOCATION '/data/hive/movies'
    > ;
OK
Time taken: 0.161 seconds
hive> CREATE EXTERNAL TABLE `t_rating_zdd`(
    >   `userid` int, 
    >   `movieid` int, 
    >   `rate` int, 
    >   `times` bigint)
    > ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' 
    > WITH SERDEPROPERTIES ('field.delim'='::') 
    > STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
    > OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    > LOCATION '/data/hive/ratings';
OK
Time taken: 0.157 seconds
```

## 题目一
### sql
```sql
select u.age, avg(r.rate) as avgrate
from t_user_zdd u
join t_rating_zdd r
on u.userid = r.userid
where r.movieid = 2116
group by u.age;
```

### 运行结果
```shell
hive> select u.age, avg(r.rate) as avgrate
    > from t_user_zdd u
    > join t_rating_zdd r
    > on u.userid = r.userid
    > where r.movieid = 2116
    > group by u.age;
Query ID = student5_20220403225911_c4376f45-f64f-444e-b979-a5c365626d3a
Total jobs = 1
Launching Job 1 out of 1
Status: Running (Executing on YARN cluster with App id application_1645699879292_2014)

----------------------------------------------------------------------------------------------
        VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED  
----------------------------------------------------------------------------------------------
Map 1 .......... container     SUCCEEDED      1          1        0        0       0       0  
Map 2 .......... container     SUCCEEDED      1          1        0        0       0       0  
Reducer 3 ...... container     SUCCEEDED      2          2        0        0       0       0  
----------------------------------------------------------------------------------------------
VERTICES: 03/03  [==========================>>] 100%  ELAPSED TIME: 8.03 s     
----------------------------------------------------------------------------------------------
OK
1	3.2941176470588234
18	3.3580246913580245
25	3.436548223350254
35	3.2278481012658227
45	2.8275862068965516
50	3.32
56	3.5
```


## 题目二
### sql
```sql
select 'M' as sex, m.moviename as name, avg(r.rate) as avgrate, count(*) as total
from t_user_zdd u
join t_rating_zdd r
on u.userid = r.userid
join t_movie_zdd m
on r.movieid = m.movieid
where u.sex = 'M'
group by m.moviename
having total > 50
order by avgrate desc
limit 10;
```

### 运行结果
```shell
hive> select 'M' as sex, m.moviename as name, avg(r.rate) as avgrate, count(*) as total
    > from t_user_zdd u
    > join t_rating_zdd r
    > on u.userid = r.userid
    > join t_movie_zdd m
    > on r.movieid = m.movieid
    > where u.sex = 'M'
    > group by m.moviename
    > having total > 50
    > order by avgrate desc
    > limit 10;
Query ID = student5_20220403230835_58e669c5-8592-41da-ac53-bc8f07f66cf9
Total jobs = 1
Launching Job 1 out of 1
Status: Running (Executing on YARN cluster with App id application_1645699879292_2021)

----------------------------------------------------------------------------------------------
        VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED  
----------------------------------------------------------------------------------------------
Map 1 .......... container     SUCCEEDED      1          1        0        0       0       0  
Map 5 .......... container     SUCCEEDED      1          1        0        0       0       0  
Map 2 .......... container     SUCCEEDED      1          1        0        0       0       0  
Reducer 3 ...... container     SUCCEEDED      4          4        0        0       0       0  
Reducer 4 ...... container     SUCCEEDED      1          1        0        0       0       0  
----------------------------------------------------------------------------------------------
VERTICES: 05/05  [==========================>>] 100%  ELAPSED TIME: 12.10 s    
----------------------------------------------------------------------------------------------
OK
M	Sanjuro (1962)	4.639344262295082	61
M	Godfather, The (1972)	4.583333333333333	1740
M	Seven Samurai (The Magnificent Seven) (Shichinin no samurai) (1954)	4.576628352490421	522
M	Shawshank Redemption, The (1994)	4.560625	1600
M	Raiders of the Lost Ark (1981)	4.520597322348094	1942
M	Usual Suspects, The (1995)	4.518248175182482	1370
M	Star Wars: Episode IV - A New Hope (1977)	4.495307167235495	2344
M	Schindler's List (1993)	4.49141503848431	1689
M	Paths of Glory (1957)	4.485148514851486	202
M	Wrong Trousers, The (1993)	4.478260869565218	644
Time taken: 14.094 seconds, Fetched: 10 row(s)
```


## 题目三
### sql
```sql
select  m.moviename, avg(r.rate) as avgrate
from t_movie_zdd m
join t_rating_zdd r
on m.movieid = r.movieid
join
    (
        select r.movieid
        from t_rating_zdd r
        join 
            (
                select u.userid, count(*) as num
                from t_user_zdd u
                join t_rating_zdd r
                on u.userid = r.userid
                where u.sex = 'F'
                group by u.userid
                order by num desc
                limit 1
            ) u
        on r.userid = u.userid
        order by r.rate desc
        limit 10
    ) mid
on m.movieid = mid.movieid
group by m.moviename;
```

### 运行结果
```shell
hive>select  m.moviename, avg(r.rate) as avgrate
    > from t_movie_zdd m
    > join t_rating_zdd r
    > on m.movieid = r.movieid
    > join
    >     (
    >         select r.movieid
    >         from t_rating_zdd r
    >         join 
    >             (
    >                 select u.userid, count(*) as num
    >                 from t_user_zdd u
    >                 join t_rating_zdd r
    >                 on u.userid = r.userid
    >                 where u.sex = 'F'
    >                 group by u.userid
    >                 order by num desc
    >                 limit 1
    >             ) u
    >         on r.userid = u.userid
    >         order by r.rate desc
    >         limit 10
    >     ) mid
    > on m.movieid = mid.movieid
    > group by m.moviename;
No Stats for default@t_movie_zdd, Columns: movieid, moviename
No Stats for default@t_rating_zdd, Columns: rate, movieid
No Stats for default@t_rating_zdd, Columns: userid
No Stats for default@t_user_zdd, Columns: sex, userid
Query ID = student5_20220403232423_8974296a-d8e9-4b45-84dc-c9295678445e
Total jobs = 1
Launching Job 1 out of 1
Status: Running (Executing on YARN cluster with App id application_1645699879292_2028)

----------------------------------------------------------------------------------------------
        VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED  
----------------------------------------------------------------------------------------------
Map 3 .......... container     SUCCEEDED      1          1        0        0       0       0  
Map 9 .......... container     SUCCEEDED      1          1        0        0       0       0  
Map 6 .......... container     SUCCEEDED      1          1        0        0       0       0  
Reducer 7 ...... container     SUCCEEDED      4          4        0        0       0       0  
Reducer 8 ...... container     SUCCEEDED      1          1        0        0       0       0  
Map 4 .......... container     SUCCEEDED      1          1        0        0       0       0  
Reducer 5 ...... container     SUCCEEDED      1          1        0        0       0       0  
Map 1 .......... container     SUCCEEDED      1          1        0        0       0       0  
Reducer 2 ...... container     SUCCEEDED      6          6        0        0       0       0  
----------------------------------------------------------------------------------------------
VERTICES: 09/09  [==========================>>] 100%  ELAPSED TIME: 16.39 s    
----------------------------------------------------------------------------------------------
OK
Big Lebowski, The (1998)	3.7383773928896993
Rear Window (1954)	4.476190476190476
Star Wars: Episode IV - A New Hope (1977)	4.453694416583082
Sound of Music, The (1965)	3.931972789115646
Waiting for Guffman (1996)	4.147186147186147
Badlands (1973)	4.078838174273859
House of Yes, The (1997)	3.4742268041237114
Fast, Cheap & Out of Control (1997)	3.8518518518518516
Roger & Me (1989)	4.0739348370927315
City of Lost Children, The (1995)	4.062034739454094
Time taken: 18.344 seconds, Fetched: 10 row(s)
```

