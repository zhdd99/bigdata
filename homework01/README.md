## 运行结果：
```shell
[student5@emr-header-1 zhangdongdong]$ hadoop jar homework01-1.0-SNAPSHOT.jar mobiletraffic input zhangdongdongresult
22/03/12 22:38:59 INFO client.RMProxy: Connecting to ResourceManager at emr-header-1.cluster-285604/192.168.0.197:8032
22/03/12 22:38:59 INFO client.AHSProxy: Connecting to Application History server at emr-header-1.cluster-285604/192.168.0.197:10200
22/03/12 22:38:59 WARN mapreduce.JobResourceUploader: Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
22/03/12 22:38:59 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /tmp/hadoop-yarn/staging/student5/.staging/job_1645699879292_0315
22/03/12 22:38:59 INFO sasl.SaslDataTransferClient: SASL encryption trust check: localHostTrusted = false, remoteHostTrusted = false
22/03/12 22:38:59 INFO input.FileInputFormat: Total input files to process : 1
22/03/12 22:38:59 INFO lzo.GPLNativeCodeLoader: Loaded native gpl library from the embedded binaries
22/03/12 22:38:59 INFO lzo.LzoCodec: Successfully loaded & initialized native-lzo library [hadoop-lzo rev 97184efe294f64a51a4c5c172cbc22146103da53]
22/03/12 22:38:59 INFO sasl.SaslDataTransferClient: SASL encryption trust check: localHostTrusted = false, remoteHostTrusted = false
22/03/12 22:38:59 INFO sasl.SaslDataTransferClient: SASL encryption trust check: localHostTrusted = false, remoteHostTrusted = false
22/03/12 22:38:59 INFO mapreduce.JobSubmitter: number of splits:1
22/03/12 22:38:59 INFO sasl.SaslDataTransferClient: SASL encryption trust check: localHostTrusted = false, remoteHostTrusted = false
22/03/12 22:38:59 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1645699879292_0315
22/03/12 22:38:59 INFO mapreduce.JobSubmitter: Executing with tokens: []
22/03/12 22:39:00 INFO conf.Configuration: resource-types.xml not found
22/03/12 22:39:00 INFO resource.ResourceUtils: Unable to find 'resource-types.xml'.
22/03/12 22:39:00 INFO impl.YarnClientImpl: Submitted application application_1645699879292_0315
22/03/12 22:39:00 INFO mapreduce.Job: The url to track the job: http://emr-header-1.cluster-285604:20888/proxy/application_1645699879292_0315/
22/03/12 22:39:00 INFO mapreduce.Job: Running job: job_1645699879292_0315
22/03/12 22:39:06 INFO mapreduce.Job: Job job_1645699879292_0315 running in uber mode : false
22/03/12 22:39:06 INFO mapreduce.Job:  map 0% reduce 0%
22/03/12 22:39:11 INFO mapreduce.Job:  map 100% reduce 0%
22/03/12 22:39:16 INFO mapreduce.Job:  map 100% reduce 100%
22/03/12 22:39:16 INFO mapreduce.Job: Job job_1645699879292_0315 completed successfully
22/03/12 22:39:16 INFO mapreduce.Job: Counters: 54
	File System Counters
		FILE: Number of bytes read=377
		FILE: Number of bytes written=487269
		FILE: Number of read operations=0
		FILE: Number of large read operations=0
		FILE: Number of write operations=0
		HDFS: Number of bytes read=2377
		HDFS: Number of bytes written=551
		HDFS: Number of read operations=8
		HDFS: Number of large read operations=0
		HDFS: Number of write operations=2
		HDFS: Number of bytes read erasure-coded=0
	Job Counters 
		Launched map tasks=1
		Launched reduce tasks=1
		Data-local map tasks=1
		Total time spent by all maps in occupied slots (ms)=113164
		Total time spent by all reduces in occupied slots (ms)=184725
		Total time spent by all map tasks (ms)=2978
		Total time spent by all reduce tasks (ms)=2463
		Total vcore-milliseconds taken by all map tasks=2978
		Total vcore-milliseconds taken by all reduce tasks=2463
		Total megabyte-milliseconds taken by all map tasks=3621248
		Total megabyte-milliseconds taken by all reduce tasks=5911200
	Map-Reduce Framework
		Map input records=22
		Map output records=22
		Map output bytes=789
		Map output materialized bytes=369
		Input split bytes=148
		Combine input records=0
		Combine output records=0
		Reduce input groups=21
		Reduce shuffle bytes=369
		Reduce input records=22
		Reduce output records=21
		Spilled Records=44
		Shuffled Maps =1
		Failed Shuffles=0
		Merged Map outputs=1
		GC time elapsed (ms)=113
		CPU time spent (ms)=2240
		Physical memory (bytes) snapshot=729997312
		Virtual memory (bytes) snapshot=6961856512
		Total committed heap usage (bytes)=817889280
		Peak Map Physical memory (bytes)=474120192
		Peak Map Virtual memory (bytes)=2954149888
		Peak Reduce Physical memory (bytes)=255877120
		Peak Reduce Virtual memory (bytes)=4007706624
	Shuffle Errors
		BAD_ID=0
		CONNECTION=0
		IO_ERROR=0
		WRONG_LENGTH=0
		WRONG_MAP=0
		WRONG_REDUCE=0
	File Input Format Counters 
		Bytes Read=2229
	File Output Format Counters 
		Bytes Written=551
```

## 计算结果
```shell
[student5@emr-header-1 zhangdongdong]$ hadoop fs -cat zhangdongdongresult/part-r-00000
22/03/12 22:40:05 INFO sasl.SaslDataTransferClient: SASL encryption trust check: localHostTrusted = false, remoteHostTrusted = false
13480253104	180	180	360
13502468823	7335	110349	117684
13560436666	1116	954	2070
13560439658	2034	5892	7926
13602846565	1938	2910	4848
13660577991	6960	690	7650
13719199419	240	0	240
13726230503	2481	24681	27162
13726238888	2481	24681	27162
13760778710	120	120	240
13826544101	264	0	264
13922314466	3008	3720	6728
13925057413	11058	48243	59301
13926251106	240	0	240
13926435656	132	1512	1644
15013685858	3659	3538	7197
15920133257	3156	2936	6092
15989002119	1938	180	2118
18211575961	1527	2106	3633
18320173382	9531	2412	11943
84138413	4116	1432	5548
```