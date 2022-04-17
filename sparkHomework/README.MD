#作业一
## 运行命令
```shell
./spark-submit --class org.apache.spark.examples.JavaInvertedIndex --master local /Users/zhdd99/KuaishouProjects/bigdata/spark/examples/target/original-spark-examples_2.12-3.4.0-SNAPSHOT.jar
```
## 运行日志
```shell
22/04/17 19:35:36 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
22/04/17 19:35:36 INFO SparkContext: Running Spark version 3.1.2
22/04/17 19:35:36 INFO ResourceUtils: ==============================================================
22/04/17 19:35:36 INFO ResourceUtils: No custom resources configured for spark.driver.
22/04/17 19:35:36 INFO ResourceUtils: ==============================================================
22/04/17 19:35:36 INFO SparkContext: Submitted application: JavaInvertedIndex
22/04/17 19:35:36 INFO ResourceProfile: Default ResourceProfile created, executor resources: Map(cores -> name: cores, amount: 1, script: , vendor: , memory -> name: memory, amount: 1024, script: , vendor: , offHeap -> name: offHeap, amount: 0, script: , vendor: ), task resources: Map(cpus -> name: cpus, amount: 1.0)
22/04/17 19:35:36 INFO ResourceProfile: Limiting resource is cpu
22/04/17 19:35:36 INFO ResourceProfileManager: Added ResourceProfile id: 0
22/04/17 19:35:36 INFO SecurityManager: Changing view acls to: zhdd99
22/04/17 19:35:36 INFO SecurityManager: Changing modify acls to: zhdd99
22/04/17 19:35:36 INFO SecurityManager: Changing view acls groups to: 
22/04/17 19:35:36 INFO SecurityManager: Changing modify acls groups to: 
22/04/17 19:35:36 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(zhdd99); groups with view permissions: Set(); users  with modify permissions: Set(zhdd99); groups with modify permissions: Set()
22/04/17 19:35:36 INFO Utils: Successfully started service 'sparkDriver' on port 53936.
22/04/17 19:35:36 INFO SparkEnv: Registering MapOutputTracker
22/04/17 19:35:36 INFO SparkEnv: Registering BlockManagerMaster
22/04/17 19:35:36 INFO BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
22/04/17 19:35:36 INFO BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
22/04/17 19:35:36 INFO SparkEnv: Registering BlockManagerMasterHeartbeat
22/04/17 19:35:36 INFO DiskBlockManager: Created local directory at /private/var/folders/d4/04dy_6592tb838b15yvtnv4w0000gn/T/blockmgr-c540507b-4d9d-43d4-929e-6fb7a20f0410
22/04/17 19:35:36 INFO MemoryStore: MemoryStore started with capacity 366.3 MiB
22/04/17 19:35:36 INFO SparkEnv: Registering OutputCommitCoordinator
22/04/17 19:35:37 WARN Utils: Service 'SparkUI' could not bind on port 4040. Attempting port 4041.
22/04/17 19:35:37 INFO Utils: Successfully started service 'SparkUI' on port 4041.
22/04/17 19:35:37 INFO SparkUI: Bound SparkUI to 0.0.0.0, and started at http://192.168.1.103:4041
22/04/17 19:35:37 INFO SparkContext: Added JAR file:/Users/zhdd99/KuaishouProjects/bigdata/spark/examples/target/original-spark-examples_2.12-3.4.0-SNAPSHOT.jar at spark://192.168.1.103:53936/jars/original-spark-examples_2.12-3.4.0-SNAPSHOT.jar with timestamp 1650195336462
22/04/17 19:35:37 INFO Executor: Starting executor ID driver on host 192.168.1.103
22/04/17 19:35:37 INFO Executor: Fetching spark://192.168.1.103:53936/jars/original-spark-examples_2.12-3.4.0-SNAPSHOT.jar with timestamp 1650195336462
22/04/17 19:35:37 INFO TransportClientFactory: Successfully created connection to /192.168.1.103:53936 after 28 ms (0 ms spent in bootstraps)
22/04/17 19:35:37 INFO Utils: Fetching spark://192.168.1.103:53936/jars/original-spark-examples_2.12-3.4.0-SNAPSHOT.jar to /private/var/folders/d4/04dy_6592tb838b15yvtnv4w0000gn/T/spark-ccb6e37b-8238-4cc1-b036-87be3aef9164/userFiles-053f2f6c-0f0a-415d-bebd-db0029c66d5c/fetchFileTemp5342107757043794653.tmp
22/04/17 19:35:37 INFO Executor: Adding file:/private/var/folders/d4/04dy_6592tb838b15yvtnv4w0000gn/T/spark-ccb6e37b-8238-4cc1-b036-87be3aef9164/userFiles-053f2f6c-0f0a-415d-bebd-db0029c66d5c/original-spark-examples_2.12-3.4.0-SNAPSHOT.jar to class loader
22/04/17 19:35:37 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 53938.
22/04/17 19:35:37 INFO NettyBlockTransferService: Server created on 192.168.1.103:53938
22/04/17 19:35:37 INFO BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
22/04/17 19:35:37 INFO BlockManagerMaster: Registering BlockManager BlockManagerId(driver, 192.168.1.103, 53938, None)
22/04/17 19:35:37 INFO BlockManagerMasterEndpoint: Registering block manager 192.168.1.103:53938 with 366.3 MiB RAM, BlockManagerId(driver, 192.168.1.103, 53938, None)
22/04/17 19:35:37 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, 192.168.1.103, 53938, None)
22/04/17 19:35:37 INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, 192.168.1.103, 53938, None)
22/04/17 19:35:38 INFO MemoryStore: Block broadcast_0 stored as values in memory (estimated size 305.1 KiB, free 366.0 MiB)
22/04/17 19:35:38 INFO MemoryStore: Block broadcast_0_piece0 stored as bytes in memory (estimated size 27.0 KiB, free 366.0 MiB)
22/04/17 19:35:38 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on 192.168.1.103:53938 (size: 27.0 KiB, free: 366.3 MiB)
22/04/17 19:35:38 INFO SparkContext: Created broadcast 0 from wholeTextFiles at JavaInvertedIndex.java:47
22/04/17 19:35:38 INFO FileInputFormat: Total input files to process : 3
22/04/17 19:35:38 INFO FileInputFormat: Total input files to process : 3
22/04/17 19:35:38 INFO deprecation: mapred.output.dir is deprecated. Instead, use mapreduce.output.fileoutputformat.outputdir
22/04/17 19:35:38 INFO HadoopMapRedCommitProtocol: Using output committer class org.apache.hadoop.mapred.FileOutputCommitter
22/04/17 19:35:38 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
22/04/17 19:35:38 INFO FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
22/04/17 19:35:38 INFO SparkContext: Starting job: runJob at SparkHadoopWriter.scala:83
22/04/17 19:35:38 INFO DAGScheduler: Registering RDD 3 (mapToPair at JavaInvertedIndex.java:61) as input to shuffle 1
22/04/17 19:35:38 INFO DAGScheduler: Registering RDD 5 (mapToPair at JavaInvertedIndex.java:72) as input to shuffle 0
22/04/17 19:35:38 INFO DAGScheduler: Got job 0 (runJob at SparkHadoopWriter.scala:83) with 1 output partitions
22/04/17 19:35:38 INFO DAGScheduler: Final stage: ResultStage 2 (runJob at SparkHadoopWriter.scala:83)
22/04/17 19:35:38 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 1)
22/04/17 19:35:38 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 1)
22/04/17 19:35:38 INFO DAGScheduler: Submitting ShuffleMapStage 0 (MapPartitionsRDD[3] at mapToPair at JavaInvertedIndex.java:61), which has no missing parents
22/04/17 19:35:38 INFO MemoryStore: Block broadcast_1 stored as values in memory (estimated size 7.1 KiB, free 366.0 MiB)
22/04/17 19:35:38 INFO MemoryStore: Block broadcast_1_piece0 stored as bytes in memory (estimated size 3.8 KiB, free 366.0 MiB)
22/04/17 19:35:38 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on 192.168.1.103:53938 (size: 3.8 KiB, free: 366.3 MiB)
22/04/17 19:35:38 INFO SparkContext: Created broadcast 1 from broadcast at DAGScheduler.scala:1388
22/04/17 19:35:38 INFO DAGScheduler: Submitting 1 missing tasks from ShuffleMapStage 0 (MapPartitionsRDD[3] at mapToPair at JavaInvertedIndex.java:61) (first 15 tasks are for partitions Vector(0))
22/04/17 19:35:38 INFO TaskSchedulerImpl: Adding task set 0.0 with 1 tasks resource profile 0
22/04/17 19:35:38 INFO TaskSetManager: Starting task 0.0 in stage 0.0 (TID 0) (192.168.1.103, executor driver, partition 0, PROCESS_LOCAL, 4772 bytes) taskResourceAssignments Map()
22/04/17 19:35:38 INFO Executor: Running task 0.0 in stage 0.0 (TID 0)
22/04/17 19:35:39 INFO WholeTextFileRDD: Input split: Paths:/Users/zhdd99/KuaishouProjects/bigdata/spark/examples/input1/0:0+17,/Users/zhdd99/KuaishouProjects/bigdata/spark/examples/input1/1:0+11,/Users/zhdd99/KuaishouProjects/bigdata/spark/examples/input1/2:0+15
22/04/17 19:35:39 INFO Executor: Finished task 0.0 in stage 0.0 (TID 0). 1334 bytes result sent to driver
22/04/17 19:35:39 INFO TaskSetManager: Finished task 0.0 in stage 0.0 (TID 0) in 470 ms on 192.168.1.103 (executor driver) (1/1)
22/04/17 19:35:39 INFO TaskSchedulerImpl: Removed TaskSet 0.0, whose tasks have all completed, from pool 
22/04/17 19:35:39 INFO DAGScheduler: ShuffleMapStage 0 (mapToPair at JavaInvertedIndex.java:61) finished in 0.595 s
22/04/17 19:35:39 INFO DAGScheduler: looking for newly runnable stages
22/04/17 19:35:39 INFO DAGScheduler: running: Set()
22/04/17 19:35:39 INFO DAGScheduler: waiting: Set(ShuffleMapStage 1, ResultStage 2)
22/04/17 19:35:39 INFO DAGScheduler: failed: Set()
22/04/17 19:35:39 INFO DAGScheduler: Submitting ShuffleMapStage 1 (MapPartitionsRDD[5] at mapToPair at JavaInvertedIndex.java:72), which has no missing parents
22/04/17 19:35:39 INFO MemoryStore: Block broadcast_2 stored as values in memory (estimated size 5.8 KiB, free 366.0 MiB)
22/04/17 19:35:39 INFO MemoryStore: Block broadcast_2_piece0 stored as bytes in memory (estimated size 3.2 KiB, free 366.0 MiB)
22/04/17 19:35:39 INFO BlockManagerInfo: Added broadcast_2_piece0 in memory on 192.168.1.103:53938 (size: 3.2 KiB, free: 366.3 MiB)
22/04/17 19:35:39 INFO SparkContext: Created broadcast 2 from broadcast at DAGScheduler.scala:1388
22/04/17 19:35:39 INFO DAGScheduler: Submitting 1 missing tasks from ShuffleMapStage 1 (MapPartitionsRDD[5] at mapToPair at JavaInvertedIndex.java:72) (first 15 tasks are for partitions Vector(0))
22/04/17 19:35:39 INFO TaskSchedulerImpl: Adding task set 1.0 with 1 tasks resource profile 0
22/04/17 19:35:39 INFO TaskSetManager: Starting task 0.0 in stage 1.0 (TID 1) (192.168.1.103, executor driver, partition 0, NODE_LOCAL, 4260 bytes) taskResourceAssignments Map()
22/04/17 19:35:39 INFO Executor: Running task 0.0 in stage 1.0 (TID 1)
22/04/17 19:35:39 INFO ShuffleBlockFetcherIterator: Getting 1 (304.0 B) non-empty blocks including 1 (304.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) remote blocks
22/04/17 19:35:39 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 7 ms
22/04/17 19:35:39 INFO Executor: Finished task 0.0 in stage 1.0 (TID 1). 1463 bytes result sent to driver
22/04/17 19:35:39 INFO TaskSetManager: Finished task 0.0 in stage 1.0 (TID 1) in 60 ms on 192.168.1.103 (executor driver) (1/1)
22/04/17 19:35:39 INFO TaskSchedulerImpl: Removed TaskSet 1.0, whose tasks have all completed, from pool 
22/04/17 19:35:39 INFO DAGScheduler: ShuffleMapStage 1 (mapToPair at JavaInvertedIndex.java:72) finished in 0.071 s
22/04/17 19:35:39 INFO DAGScheduler: looking for newly runnable stages
22/04/17 19:35:39 INFO DAGScheduler: running: Set()
22/04/17 19:35:39 INFO DAGScheduler: waiting: Set(ResultStage 2)
22/04/17 19:35:39 INFO DAGScheduler: failed: Set()
22/04/17 19:35:39 INFO DAGScheduler: Submitting ResultStage 2 (MapPartitionsRDD[7] at saveAsTextFile at JavaInvertedIndex.java:96), which has no missing parents
22/04/17 19:35:39 INFO MemoryStore: Block broadcast_3 stored as values in memory (estimated size 85.8 KiB, free 365.9 MiB)
22/04/17 19:35:39 INFO MemoryStore: Block broadcast_3_piece0 stored as bytes in memory (estimated size 30.9 KiB, free 365.8 MiB)
22/04/17 19:35:39 INFO BlockManagerInfo: Added broadcast_3_piece0 in memory on 192.168.1.103:53938 (size: 30.9 KiB, free: 366.2 MiB)
22/04/17 19:35:39 INFO SparkContext: Created broadcast 3 from broadcast at DAGScheduler.scala:1388
22/04/17 19:35:39 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 2 (MapPartitionsRDD[7] at saveAsTextFile at JavaInvertedIndex.java:96) (first 15 tasks are for partitions Vector(0))
22/04/17 19:35:39 INFO TaskSchedulerImpl: Adding task set 2.0 with 1 tasks resource profile 0
22/04/17 19:35:39 INFO TaskSetManager: Starting task 0.0 in stage 2.0 (TID 2) (192.168.1.103, executor driver, partition 0, NODE_LOCAL, 4271 bytes) taskResourceAssignments Map()
22/04/17 19:35:39 INFO Executor: Running task 0.0 in stage 2.0 (TID 2)
22/04/17 19:35:39 INFO ShuffleBlockFetcherIterator: Getting 1 (445.0 B) non-empty blocks including 1 (445.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) remote blocks
22/04/17 19:35:39 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
22/04/17 19:35:39 INFO HadoopMapRedCommitProtocol: Using output committer class org.apache.hadoop.mapred.FileOutputCommitter
22/04/17 19:35:39 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
22/04/17 19:35:39 INFO FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
22/04/17 19:35:39 INFO FileOutputCommitter: Saved output of task 'attempt_20220417193538140107032698296518_0007_m_000000_0' to file:/Users/zhdd99/KuaishouProjects/bigdata/spark/examples/output1/_temporary/0/task_20220417193538140107032698296518_0007_m_000000
22/04/17 19:35:39 INFO SparkHadoopMapRedUtil: attempt_20220417193538140107032698296518_0007_m_000000_0: Committed
22/04/17 19:35:39 INFO Executor: Finished task 0.0 in stage 2.0 (TID 2). 1631 bytes result sent to driver
22/04/17 19:35:39 INFO TaskSetManager: Finished task 0.0 in stage 2.0 (TID 2) in 87 ms on 192.168.1.103 (executor driver) (1/1)
22/04/17 19:35:39 INFO TaskSchedulerImpl: Removed TaskSet 2.0, whose tasks have all completed, from pool 
22/04/17 19:35:39 INFO DAGScheduler: ResultStage 2 (runJob at SparkHadoopWriter.scala:83) finished in 0.113 s
22/04/17 19:35:39 INFO DAGScheduler: Job 0 is finished. Cancelling potential speculative or zombie tasks for this job
22/04/17 19:35:39 INFO TaskSchedulerImpl: Killing all running tasks in stage 2: Stage finished
22/04/17 19:35:39 INFO DAGScheduler: Job 0 finished: runJob at SparkHadoopWriter.scala:83, took 0.857191 s
22/04/17 19:35:39 INFO SparkHadoopWriter: Job job_20220417193538140107032698296518_0007 committed.
22/04/17 19:35:39 INFO SparkUI: Stopped Spark web UI at http://192.168.1.103:4041
22/04/17 19:35:39 INFO MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
22/04/17 19:35:39 INFO MemoryStore: MemoryStore cleared
22/04/17 19:35:39 INFO BlockManager: BlockManager stopped
22/04/17 19:35:39 INFO BlockManagerMaster: BlockManagerMaster stopped
22/04/17 19:35:39 INFO OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!
22/04/17 19:35:39 INFO SparkContext: Successfully stopped SparkContext
22/04/17 19:35:39 INFO ShutdownHookManager: Shutdown hook called
22/04/17 19:35:39 INFO ShutdownHookManager: Deleting directory /private/var/folders/d4/04dy_6592tb838b15yvtnv4w0000gn/T/spark-ecd58332-3165-4511-8034-8e965d585279
22/04/17 19:35:39 INFO ShutdownHookManager: Deleting directory /private/var/folders/d4/04dy_6592tb838b15yvtnv4w0000gn/T/spark-ccb6e37b-8238-4cc1-b036-87be3aef9164
```
## 执行结果：
```shell
(banana,[(2,1)])
(it,[(1,1), (0,2), (2,1)])
(is,[(2,1), (1,1), (0,2)])
(a,[(2,1)])
(what,[(0,1), (1,1)])
```

# 作业二

## 运行命令（明天上班，没时间了，参数直接写死）
```shell
./spark-submit --class org.apache.spark.examples.JavaSparkDistCp --master local /Users/zhdd99/KuaishouProjects/bigdata/spark/examples/target/original-spark-examples_2.12-3.4.0-SNAPSHOT.jar
```
## 运行日志
```shell
22/04/17 20:54:12 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
22/04/17 20:54:12 INFO SparkContext: Running Spark version 3.1.2
22/04/17 20:54:12 INFO ResourceUtils: ==============================================================
22/04/17 20:54:12 INFO ResourceUtils: No custom resources configured for spark.driver.
22/04/17 20:54:12 INFO ResourceUtils: ==============================================================
22/04/17 20:54:12 INFO SparkContext: Submitted application: JavaSparkDistCp
22/04/17 20:54:13 INFO ResourceProfile: Default ResourceProfile created, executor resources: Map(cores -> name: cores, amount: 1, script: , vendor: , memory -> name: memory, amount: 1024, script: , vendor: , offHeap -> name: offHeap, amount: 0, script: , vendor: ), task resources: Map(cpus -> name: cpus, amount: 1.0)
22/04/17 20:54:13 INFO ResourceProfile: Limiting resource is cpu
22/04/17 20:54:13 INFO ResourceProfileManager: Added ResourceProfile id: 0
22/04/17 20:54:13 INFO SecurityManager: Changing view acls to: zhdd99
22/04/17 20:54:13 INFO SecurityManager: Changing modify acls to: zhdd99
22/04/17 20:54:13 INFO SecurityManager: Changing view acls groups to: 
22/04/17 20:54:13 INFO SecurityManager: Changing modify acls groups to: 
22/04/17 20:54:13 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(zhdd99); groups with view permissions: Set(); users  with modify permissions: Set(zhdd99); groups with modify permissions: Set()
22/04/17 20:54:13 INFO Utils: Successfully started service 'sparkDriver' on port 55850.
22/04/17 20:54:13 INFO SparkEnv: Registering MapOutputTracker
22/04/17 20:54:13 INFO SparkEnv: Registering BlockManagerMaster
22/04/17 20:54:13 INFO BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
22/04/17 20:54:13 INFO BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
22/04/17 20:54:13 INFO SparkEnv: Registering BlockManagerMasterHeartbeat
22/04/17 20:54:13 INFO DiskBlockManager: Created local directory at /private/var/folders/d4/04dy_6592tb838b15yvtnv4w0000gn/T/blockmgr-3dcec244-7fd2-438a-9dda-a7a8ac9b3d00
22/04/17 20:54:13 INFO MemoryStore: MemoryStore started with capacity 366.3 MiB
22/04/17 20:54:13 INFO SparkEnv: Registering OutputCommitCoordinator
22/04/17 20:54:13 WARN Utils: Service 'SparkUI' could not bind on port 4040. Attempting port 4041.
22/04/17 20:54:13 INFO Utils: Successfully started service 'SparkUI' on port 4041.
22/04/17 20:54:13 INFO SparkUI: Bound SparkUI to 0.0.0.0, and started at http://192.168.1.103:4041
22/04/17 20:54:13 INFO SparkContext: Added JAR file:/Users/zhdd99/KuaishouProjects/bigdata/spark/examples/target/original-spark-examples_2.12-3.4.0-SNAPSHOT.jar at spark://192.168.1.103:55850/jars/original-spark-examples_2.12-3.4.0-SNAPSHOT.jar with timestamp 1650200052929
22/04/17 20:54:13 INFO Executor: Starting executor ID driver on host 192.168.1.103
22/04/17 20:54:13 INFO Executor: Fetching spark://192.168.1.103:55850/jars/original-spark-examples_2.12-3.4.0-SNAPSHOT.jar with timestamp 1650200052929
22/04/17 20:54:13 INFO TransportClientFactory: Successfully created connection to /192.168.1.103:55850 after 27 ms (0 ms spent in bootstraps)
22/04/17 20:54:13 INFO Utils: Fetching spark://192.168.1.103:55850/jars/original-spark-examples_2.12-3.4.0-SNAPSHOT.jar to /private/var/folders/d4/04dy_6592tb838b15yvtnv4w0000gn/T/spark-f1efcbdb-9510-4097-91ec-4ac116733dec/userFiles-ed7dca20-0d80-41a6-b335-6ce5cc57e812/fetchFileTemp5806526945181529684.tmp
22/04/17 20:54:13 INFO Executor: Adding file:/private/var/folders/d4/04dy_6592tb838b15yvtnv4w0000gn/T/spark-f1efcbdb-9510-4097-91ec-4ac116733dec/userFiles-ed7dca20-0d80-41a6-b335-6ce5cc57e812/original-spark-examples_2.12-3.4.0-SNAPSHOT.jar to class loader
22/04/17 20:54:13 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 55852.
22/04/17 20:54:13 INFO NettyBlockTransferService: Server created on 192.168.1.103:55852
22/04/17 20:54:13 INFO BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
22/04/17 20:54:13 INFO BlockManagerMaster: Registering BlockManager BlockManagerId(driver, 192.168.1.103, 55852, None)
22/04/17 20:54:13 INFO BlockManagerMasterEndpoint: Registering block manager 192.168.1.103:55852 with 366.3 MiB RAM, BlockManagerId(driver, 192.168.1.103, 55852, None)
22/04/17 20:54:13 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, 192.168.1.103, 55852, None)
22/04/17 20:54:13 INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, 192.168.1.103, 55852, None)
22/04/17 20:54:14 INFO SparkContext: Starting job: collect at JavaSparkDistCp.java:81
22/04/17 20:54:14 INFO DAGScheduler: Got job 0 (collect at JavaSparkDistCp.java:81) with 10 output partitions
22/04/17 20:54:14 INFO DAGScheduler: Final stage: ResultStage 0 (collect at JavaSparkDistCp.java:81)
22/04/17 20:54:14 INFO DAGScheduler: Parents of final stage: List()
22/04/17 20:54:14 INFO DAGScheduler: Missing parents: List()
22/04/17 20:54:14 INFO DAGScheduler: Submitting ResultStage 0 (MapPartitionsRDD[1] at mapPartitions at JavaSparkDistCp.java:67), which has no missing parents
22/04/17 20:54:14 INFO MemoryStore: Block broadcast_0 stored as values in memory (estimated size 3.4 KiB, free 366.3 MiB)
22/04/17 20:54:14 INFO MemoryStore: Block broadcast_0_piece0 stored as bytes in memory (estimated size 1915.0 B, free 366.3 MiB)
22/04/17 20:54:15 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on 192.168.1.103:55852 (size: 1915.0 B, free: 366.3 MiB)
22/04/17 20:54:15 INFO SparkContext: Created broadcast 0 from broadcast at DAGScheduler.scala:1388
22/04/17 20:54:15 INFO DAGScheduler: Submitting 10 missing tasks from ResultStage 0 (MapPartitionsRDD[1] at mapPartitions at JavaSparkDistCp.java:67) (first 15 tasks are for partitions Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
22/04/17 20:54:15 INFO TaskSchedulerImpl: Adding task set 0.0 with 10 tasks resource profile 0
22/04/17 20:54:15 INFO TaskSetManager: Starting task 0.0 in stage 0.0 (TID 0) (192.168.1.103, executor driver, partition 0, PROCESS_LOCAL, 7102 bytes) taskResourceAssignments Map()
22/04/17 20:54:15 INFO Executor: Running task 0.0 in stage 0.0 (TID 0)
22/04/17 20:54:15 INFO Executor: Finished task 0.0 in stage 0.0 (TID 0). 880 bytes result sent to driver
22/04/17 20:54:15 INFO TaskSetManager: Starting task 1.0 in stage 0.0 (TID 1) (192.168.1.103, executor driver, partition 1, PROCESS_LOCAL, 7220 bytes) taskResourceAssignments Map()
22/04/17 20:54:15 INFO Executor: Running task 1.0 in stage 0.0 (TID 1)
22/04/17 20:54:15 INFO TaskSetManager: Finished task 0.0 in stage 0.0 (TID 0) in 489 ms on 192.168.1.103 (executor driver) (1/10)
22/04/17 20:54:15 INFO Executor: Finished task 1.0 in stage 0.0 (TID 1). 837 bytes result sent to driver
22/04/17 20:54:15 INFO TaskSetManager: Starting task 2.0 in stage 0.0 (TID 2) (192.168.1.103, executor driver, partition 2, PROCESS_LOCAL, 7339 bytes) taskResourceAssignments Map()
22/04/17 20:54:15 INFO TaskSetManager: Finished task 1.0 in stage 0.0 (TID 1) in 223 ms on 192.168.1.103 (executor driver) (2/10)
22/04/17 20:54:15 INFO Executor: Running task 2.0 in stage 0.0 (TID 2)
22/04/17 20:54:15 INFO Executor: Finished task 2.0 in stage 0.0 (TID 2). 837 bytes result sent to driver
22/04/17 20:54:15 INFO TaskSetManager: Starting task 3.0 in stage 0.0 (TID 3) (192.168.1.103, executor driver, partition 3, PROCESS_LOCAL, 7181 bytes) taskResourceAssignments Map()
22/04/17 20:54:15 INFO TaskSetManager: Finished task 2.0 in stage 0.0 (TID 2) in 227 ms on 192.168.1.103 (executor driver) (3/10)
22/04/17 20:54:15 INFO Executor: Running task 3.0 in stage 0.0 (TID 3)
22/04/17 20:54:16 INFO Executor: Finished task 3.0 in stage 0.0 (TID 3). 837 bytes result sent to driver
22/04/17 20:54:16 INFO TaskSetManager: Starting task 4.0 in stage 0.0 (TID 4) (192.168.1.103, executor driver, partition 4, PROCESS_LOCAL, 7103 bytes) taskResourceAssignments Map()
22/04/17 20:54:16 INFO Executor: Running task 4.0 in stage 0.0 (TID 4)
22/04/17 20:54:16 INFO TaskSetManager: Finished task 3.0 in stage 0.0 (TID 3) in 216 ms on 192.168.1.103 (executor driver) (4/10)
22/04/17 20:54:16 INFO Executor: Finished task 4.0 in stage 0.0 (TID 4). 837 bytes result sent to driver
22/04/17 20:54:16 INFO TaskSetManager: Starting task 5.0 in stage 0.0 (TID 5) (192.168.1.103, executor driver, partition 5, PROCESS_LOCAL, 7129 bytes) taskResourceAssignments Map()
22/04/17 20:54:16 INFO TaskSetManager: Finished task 4.0 in stage 0.0 (TID 4) in 237 ms on 192.168.1.103 (executor driver) (5/10)
22/04/17 20:54:16 INFO Executor: Running task 5.0 in stage 0.0 (TID 5)
22/04/17 20:54:16 INFO Executor: Finished task 5.0 in stage 0.0 (TID 5). 837 bytes result sent to driver
22/04/17 20:54:16 INFO TaskSetManager: Starting task 6.0 in stage 0.0 (TID 6) (192.168.1.103, executor driver, partition 6, PROCESS_LOCAL, 6949 bytes) taskResourceAssignments Map()
22/04/17 20:54:16 INFO TaskSetManager: Finished task 5.0 in stage 0.0 (TID 5) in 242 ms on 192.168.1.103 (executor driver) (6/10)
22/04/17 20:54:16 INFO Executor: Running task 6.0 in stage 0.0 (TID 6)
22/04/17 20:54:16 INFO Executor: Finished task 6.0 in stage 0.0 (TID 6). 837 bytes result sent to driver
22/04/17 20:54:16 INFO TaskSetManager: Starting task 7.0 in stage 0.0 (TID 7) (192.168.1.103, executor driver, partition 7, PROCESS_LOCAL, 7118 bytes) taskResourceAssignments Map()
22/04/17 20:54:16 INFO TaskSetManager: Finished task 6.0 in stage 0.0 (TID 6) in 232 ms on 192.168.1.103 (executor driver) (7/10)
22/04/17 20:54:16 INFO Executor: Running task 7.0 in stage 0.0 (TID 7)
22/04/17 20:54:17 INFO Executor: Finished task 7.0 in stage 0.0 (TID 7). 880 bytes result sent to driver
22/04/17 20:54:17 INFO TaskSetManager: Starting task 8.0 in stage 0.0 (TID 8) (192.168.1.103, executor driver, partition 8, PROCESS_LOCAL, 7201 bytes) taskResourceAssignments Map()
22/04/17 20:54:17 INFO Executor: Running task 8.0 in stage 0.0 (TID 8)
22/04/17 20:54:17 INFO TaskSetManager: Finished task 7.0 in stage 0.0 (TID 7) in 239 ms on 192.168.1.103 (executor driver) (8/10)
22/04/17 20:54:17 INFO Executor: Finished task 8.0 in stage 0.0 (TID 8). 837 bytes result sent to driver
22/04/17 20:54:17 INFO TaskSetManager: Starting task 9.0 in stage 0.0 (TID 9) (192.168.1.103, executor driver, partition 9, PROCESS_LOCAL, 7546 bytes) taskResourceAssignments Map()
22/04/17 20:54:17 INFO TaskSetManager: Finished task 8.0 in stage 0.0 (TID 8) in 220 ms on 192.168.1.103 (executor driver) (9/10)
22/04/17 20:54:17 INFO Executor: Running task 9.0 in stage 0.0 (TID 9)
22/04/17 20:54:17 INFO Executor: Finished task 9.0 in stage 0.0 (TID 9). 837 bytes result sent to driver
22/04/17 20:54:17 INFO TaskSetManager: Finished task 9.0 in stage 0.0 (TID 9) in 228 ms on 192.168.1.103 (executor driver) (10/10)
22/04/17 20:54:17 INFO TaskSchedulerImpl: Removed TaskSet 0.0, whose tasks have all completed, from pool 
22/04/17 20:54:17 INFO DAGScheduler: ResultStage 0 (collect at JavaSparkDistCp.java:81) finished in 2.894 s
22/04/17 20:54:17 INFO DAGScheduler: Job 0 is finished. Cancelling potential speculative or zombie tasks for this job
22/04/17 20:54:17 INFO TaskSchedulerImpl: Killing all running tasks in stage 0: Stage finished
22/04/17 20:54:17 INFO DAGScheduler: Job 0 finished: collect at JavaSparkDistCp.java:81, took 2.932140 s
22/04/17 20:54:17 INFO SparkUI: Stopped Spark web UI at http://192.168.1.103:4041
22/04/17 20:54:17 INFO MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
22/04/17 20:54:17 INFO MemoryStore: MemoryStore cleared
22/04/17 20:54:17 INFO BlockManager: BlockManager stopped
22/04/17 20:54:17 INFO BlockManagerMaster: BlockManagerMaster stopped
22/04/17 20:54:17 INFO OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!
22/04/17 20:54:17 INFO SparkContext: Successfully stopped SparkContext
22/04/17 20:54:17 INFO ShutdownHookManager: Shutdown hook called
22/04/17 20:54:17 INFO ShutdownHookManager: Deleting directory /private/var/folders/d4/04dy_6592tb838b15yvtnv4w0000gn/T/spark-f1efcbdb-9510-4097-91ec-4ac116733dec
22/04/17 20:54:17 INFO ShutdownHookManager: Deleting directory /private/var/folders/d4/04dy_6592tb838b15yvtnv4w0000gn/T/spark-acd20ff9-4541-4f32-8c6d-d076218909b6
```
## 执行结果：
```shell
zhdd99@bogon homework2 % tree
.
├── source
│   └── java
│       └── org
│           └── apache
│               └── spark
│                   └── examples
│                       ├── JavaHdfsLR.java
│                       ├── JavaInvertedIndex.java
│                       ├── JavaLogQuery.java
│                       ├── JavaPageRank.java
│                       ├── JavaSparkDistCp.java
│                       ├── JavaSparkPi.java
│                       ├── JavaStatusTrackerDemo.java
│                       ├── JavaTC.java
│                       ├── JavaWordCount.java
│                       ├── mllib
│                       │   ├── JavaALS.java
│                       │   ├── JavaAssociationRulesExample.java
│                       │   ├── JavaBinaryClassificationMetricsExample.java
│                       │   ├── JavaBisectingKMeansExample.java
│                       │   ├── JavaChiSqSelectorExample.java
│                       │   ├── JavaCorrelationsExample.java
│                       │   ├── JavaDecisionTreeClassificationExample.java
│                       │   ├── JavaDecisionTreeRegressionExample.java
│                       │   ├── JavaElementwiseProductExample.java
│                       │   ├── JavaGaussianMixtureExample.java
│                       │   ├── JavaGradientBoostingClassificationExample.java
│                       │   ├── JavaGradientBoostingRegressionExample.java
│                       │   ├── JavaHypothesisTestingExample.java
│                       │   ├── JavaHypothesisTestingKolmogorovSmirnovTestExample.java
│                       │   ├── JavaIsotonicRegressionExample.java
│                       │   ├── JavaKMeansExample.java
│                       │   ├── JavaKernelDensityEstimationExample.java
│                       │   ├── JavaLBFGSExample.java
│                       │   ├── JavaLatentDirichletAllocationExample.java
│                       │   ├── JavaLogisticRegressionWithLBFGSExample.java
│                       │   ├── JavaMultiLabelClassificationMetricsExample.java
│                       │   ├── JavaMulticlassClassificationMetricsExample.java
│                       │   ├── JavaNaiveBayesExample.java
│                       │   ├── JavaPCAExample.java
│                       │   ├── JavaPowerIterationClusteringExample.java
│                       │   ├── JavaPrefixSpanExample.java
│                       │   ├── JavaRandomForestClassificationExample.java
│                       │   ├── JavaRandomForestRegressionExample.java
│                       │   ├── JavaRankingMetricsExample.java
│                       │   ├── JavaRecommendationExample.java
│                       │   ├── JavaSVDExample.java
│                       │   ├── JavaSVMWithSGDExample.java
│                       │   ├── JavaSimpleFPGrowth.java
│                       │   ├── JavaStratifiedSamplingExample.java
│                       │   ├── JavaStreamingTestExample.java
│                       │   └── JavaSummaryStatisticsExample.java
│                       ├── sql
│                       │   ├── JavaSQLDataSourceExample.java
│                       │   ├── JavaSparkSQLExample.java
│                       │   ├── JavaUserDefinedScalar.java
│                       │   ├── JavaUserDefinedTypedAggregation.java
│                       │   ├── JavaUserDefinedUntypedAggregation.java
│                       │   ├── hive
│                       │   │   └── JavaSparkHiveExample.java
│                       │   └── streaming
│                       │       ├── JavaStructuredComplexSessionization.java
│                       │       ├── JavaStructuredKafkaWordCount.java
│                       │       ├── JavaStructuredKerberizedKafkaWordCount.java
│                       │       ├── JavaStructuredNetworkWordCount.java
│                       │       ├── JavaStructuredNetworkWordCountWindowed.java
│                       │       └── JavaStructuredSessionization.java
│                       └── streaming
│                           ├── JavaCustomReceiver.java
│                           ├── JavaDirectKafkaWordCount.java
│                           ├── JavaDirectKerberizedKafkaWordCount.java
│                           ├── JavaNetworkWordCount.java
│                           ├── JavaQueueStream.java
│                           ├── JavaRecord.java
│                           ├── JavaRecoverableNetworkWordCount.java
│                           ├── JavaSqlNetworkWordCount.java
│                           └── JavaStatefulNetworkWordCount.java
└── target
    └── java
        └── org
            └── apache
                └── spark
                    └── examples
                        ├── JavaHdfsLR.java
                        ├── JavaInvertedIndex.java
                        ├── JavaLogQuery.java
                        ├── JavaPageRank.java
                        ├── JavaSparkDistCp.java
                        ├── JavaSparkPi.java
                        ├── JavaStatusTrackerDemo.java
                        ├── JavaTC.java
                        ├── JavaWordCount.java
                        ├── mllib
                        │   ├── JavaALS.java
                        │   ├── JavaAssociationRulesExample.java
                        │   ├── JavaBinaryClassificationMetricsExample.java
                        │   ├── JavaBisectingKMeansExample.java
                        │   ├── JavaChiSqSelectorExample.java
                        │   ├── JavaCorrelationsExample.java
                        │   ├── JavaDecisionTreeClassificationExample.java
                        │   ├── JavaDecisionTreeRegressionExample.java
                        │   ├── JavaElementwiseProductExample.java
                        │   ├── JavaGaussianMixtureExample.java
                        │   ├── JavaGradientBoostingClassificationExample.java
                        │   ├── JavaGradientBoostingRegressionExample.java
                        │   ├── JavaHypothesisTestingExample.java
                        │   ├── JavaHypothesisTestingKolmogorovSmirnovTestExample.java
                        │   ├── JavaIsotonicRegressionExample.java
                        │   ├── JavaKMeansExample.java
                        │   ├── JavaKernelDensityEstimationExample.java
                        │   ├── JavaLBFGSExample.java
                        │   ├── JavaLatentDirichletAllocationExample.java
                        │   ├── JavaLogisticRegressionWithLBFGSExample.java
                        │   ├── JavaMultiLabelClassificationMetricsExample.java
                        │   ├── JavaMulticlassClassificationMetricsExample.java
                        │   ├── JavaNaiveBayesExample.java
                        │   ├── JavaPCAExample.java
                        │   ├── JavaPowerIterationClusteringExample.java
                        │   ├── JavaPrefixSpanExample.java
                        │   ├── JavaRandomForestClassificationExample.java
                        │   ├── JavaRandomForestRegressionExample.java
                        │   ├── JavaRankingMetricsExample.java
                        │   ├── JavaRecommendationExample.java
                        │   ├── JavaSVDExample.java
                        │   ├── JavaSVMWithSGDExample.java
                        │   ├── JavaSimpleFPGrowth.java
                        │   ├── JavaStratifiedSamplingExample.java
                        │   ├── JavaStreamingTestExample.java
                        │   └── JavaSummaryStatisticsExample.java
                        ├── sql
                        │   ├── JavaSQLDataSourceExample.java
                        │   ├── JavaSparkSQLExample.java
                        │   ├── JavaUserDefinedScalar.java
                        │   ├── JavaUserDefinedTypedAggregation.java
                        │   ├── JavaUserDefinedUntypedAggregation.java
                        │   ├── hive
                        │   │   └── JavaSparkHiveExample.java
                        │   └── streaming
                        │       ├── JavaStructuredComplexSessionization.java
                        │       ├── JavaStructuredKafkaWordCount.java
                        │       ├── JavaStructuredKerberizedKafkaWordCount.java
                        │       ├── JavaStructuredNetworkWordCount.java
                        │       ├── JavaStructuredNetworkWordCountWindowed.java
                        │       └── JavaStructuredSessionization.java
                        └── streaming
                            ├── JavaCustomReceiver.java
                            ├── JavaDirectKafkaWordCount.java
                            ├── JavaDirectKerberizedKafkaWordCount.java
                            ├── JavaNetworkWordCount.java
                            ├── JavaQueueStream.java
                            ├── JavaRecord.java
                            ├── JavaRecoverableNetworkWordCount.java
                            ├── JavaSqlNetworkWordCount.java
                            └── JavaStatefulNetworkWordCount.java

22 directories, 132 files
```