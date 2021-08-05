Hive hm



# Question1

![image-20210805103300669](C:\Document\Hadoop Spark\geek\homework Aug1\hive_question_1.jpg)

```
INFO  : Compiling command(queryId=hive_20210805103111_778d7d32-ab0b-4b98-a9ae-45daa7e479ce): SELECT u.age, avg(r.rate)
FROM hive_sql_test1.t_rating AS r inner join t_user U on r.userid=u.userid
where r.movieid=2116
group by u.age
INFO  : Semantic Analysis Completed
INFO  : Returning Hive schema: Schema(fieldSchemas:[FieldSchema(name:u.age, type:int, comment:null), FieldSchema(name:_c1, type:double, comment:null)], properties:null)
INFO  : Completed compiling command(queryId=hive_20210805103111_778d7d32-ab0b-4b98-a9ae-45daa7e479ce); Time taken: 0.097 seconds
INFO  : Executing command(queryId=hive_20210805103111_778d7d32-ab0b-4b98-a9ae-45daa7e479ce): SELECT u.age, avg(r.rate)
FROM hive_sql_test1.t_rating AS r inner join t_user U on r.userid=u.userid
where r.movieid=2116
group by u.age
WARN  : 
INFO  : Query ID = hive_20210805103111_778d7d32-ab0b-4b98-a9ae-45daa7e479ce
INFO  : Total jobs = 2
INFO  : Launching Job 1 out of 2
INFO  : Starting task [Stage-1:MAPRED] in serial mode
INFO  : Number of reduce tasks not specified. Estimated from input data size: 1
INFO  : In order to change the average load for a reducer (in bytes):
INFO  :   set hive.exec.reducers.bytes.per.reducer=<number>
INFO  : In order to limit the maximum number of reducers:
INFO  :   set hive.exec.reducers.max=<number>
INFO  : In order to set a constant number of reducers:
INFO  :   set mapreduce.job.reduces=<number>
INFO  : number of splits:2
INFO  : Submitting tokens for job: job_1626505659980_2825
INFO  : Executing with tokens: []
INFO  : The url to track the job: http://jikehadoop03:8088/proxy/application_1626505659980_2825/
INFO  : Starting Job = job_1626505659980_2825, Tracking URL = http://jikehadoop03:8088/proxy/application_1626505659980_2825/
INFO  : Kill Command = /opt/cloudera/parcels/CDH-6.3.2-1.cdh6.3.2.p0.1605554/lib/hadoop/bin/hadoop job  -kill job_1626505659980_2825
INFO  : Hadoop job information for Stage-1: number of mappers: 2; number of reducers: 1
INFO  : 2021-08-05 10:31:22,950 Stage-1 map = 0%,  reduce = 0%
INFO  : 2021-08-05 10:31:31,368 Stage-1 map = 50%,  reduce = 0%, Cumulative CPU 4.58 sec
INFO  : 2021-08-05 10:31:36,578 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 14.54 sec
INFO  : 2021-08-05 10:31:43,788 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 17.3 sec
INFO  : MapReduce Total cumulative CPU time: 17 seconds 300 msec
INFO  : Ended Job = job_1626505659980_2825
INFO  : Launching Job 2 out of 2
INFO  : Starting task [Stage-2:MAPRED] in serial mode
INFO  : Number of reduce tasks not specified. Estimated from input data size: 1
INFO  : In order to change the average load for a reducer (in bytes):
INFO  :   set hive.exec.reducers.bytes.per.reducer=<number>
INFO  : In order to limit the maximum number of reducers:
INFO  :   set hive.exec.reducers.max=<number>
INFO  : In order to set a constant number of reducers:
INFO  :   set mapreduce.job.reduces=<number>
INFO  : number of splits:1
INFO  : Submitting tokens for job: job_1626505659980_2826
INFO  : Executing with tokens: []
INFO  : The url to track the job: http://jikehadoop03:8088/proxy/application_1626505659980_2826/
INFO  : Starting Job = job_1626505659980_2826, Tracking URL = http://jikehadoop03:8088/proxy/application_1626505659980_2826/
INFO  : Kill Command = /opt/cloudera/parcels/CDH-6.3.2-1.cdh6.3.2.p0.1605554/lib/hadoop/bin/hadoop job  -kill job_1626505659980_2826
INFO  : Hadoop job information for Stage-2: number of mappers: 1; number of reducers: 1
INFO  : 2021-08-05 10:32:08,044 Stage-2 map = 0%,  reduce = 0%
INFO  : 2021-08-05 10:32:16,429 Stage-2 map = 100%,  reduce = 0%, Cumulative CPU 2.47 sec
INFO  : 2021-08-05 10:32:24,139 Stage-2 map = 100%,  reduce = 100%, Cumulative CPU 5.44 sec
INFO  : MapReduce Total cumulative CPU time: 5 seconds 440 msec
INFO  : Ended Job = job_1626505659980_2826
INFO  : MapReduce Jobs Launched: 
INFO  : Stage-Stage-1: Map: 2  Reduce: 1   Cumulative CPU: 17.3 sec   HDFS Read: 24745837 HDFS Write: 321 HDFS EC Read: 0 SUCCESS
INFO  : Stage-Stage-2: Map: 1  Reduce: 1   Cumulative CPU: 5.44 sec   HDFS Read: 6863 HDFS Write: 294 HDFS EC Read: 0 SUCCESS
INFO  : Total MapReduce CPU Time Spent: 22 seconds 740 msec
INFO  : Completed executing command(queryId=hive_20210805103111_778d7d32-ab0b-4b98-a9ae-45daa7e479ce); Time taken: 74.996 seconds
INFO  : OK
```

## Question2



![image-20210805102453011](C:\Document\Hadoop Spark\geek\homework Aug1\hive_question_2.jpg)



```
with  rate as (
select u.sex ,r.movieid, count(1) as total_rate_times, avg(r.rate)as avg_rate  
from hive_sql_test1.t_rating r 
inner join t_user u on u.userid =r.userid 
where u.sex='M' 
group by r.movieid,u.sex having count(1)>50 
)
select r.sex, m.moviename, r.avg_rate,r.total_rate_times from rate r inner join t_movie m on m.movieid=r.movieid
order by avg_rate desc
limit 10

```

logs

```
INFO  : Compiling command(queryId=hive_20210805100828_1f3088cd-c3d3-4a70-97b9-74e6d4fa69e0): --  inner join t_movie m on r.movieid =m.movieid
with  rate as (
select u.sex ,r.movieid, count(1) as total_rate_times, avg(r.rate)as avg_rate  
from hive_sql_test1.t_rating r 
inner join t_user u on u.userid =r.userid 
where u.sex='M' 
group by r.movieid,u.sex having count(1)>50 
)
select r.sex, m.moviename, r.avg_rate,r.total_rate_times from rate r inner join t_movie m on m.movieid=r.movieid
order by avg_rate desc
limit 10
INFO  : Semantic Analysis Completed
INFO  : Returning Hive schema: Schema(fieldSchemas:[FieldSchema(name:r.sex, type:string, comment:null), FieldSchema(name:m.moviename, type:string, comment:null), FieldSchema(name:r.avg_rate, type:double, comment:null), FieldSchema(name:r.total_rate_times, type:bigint, comment:null)], properties:null)
INFO  : Completed compiling command(queryId=hive_20210805100828_1f3088cd-c3d3-4a70-97b9-74e6d4fa69e0); Time taken: 0.106 seconds
INFO  : Executing command(queryId=hive_20210805100828_1f3088cd-c3d3-4a70-97b9-74e6d4fa69e0): --  inner join t_movie m on r.movieid =m.movieid
with  rate as (
select u.sex ,r.movieid, count(1) as total_rate_times, avg(r.rate)as avg_rate  
from hive_sql_test1.t_rating r 
inner join t_user u on u.userid =r.userid 
where u.sex='M' 
group by r.movieid,u.sex having count(1)>50 
)
select r.sex, m.moviename, r.avg_rate,r.total_rate_times from rate r inner join t_movie m on m.movieid=r.movieid
order by avg_rate desc
limit 10
WARN  : 
INFO  : Query ID = hive_20210805100828_1f3088cd-c3d3-4a70-97b9-74e6d4fa69e0
INFO  : Total jobs = 4
INFO  : Launching Job 1 out of 4
INFO  : Starting task [Stage-1:MAPRED] in serial mode
INFO  : Number of reduce tasks not specified. Estimated from input data size: 1
INFO  : In order to change the average load for a reducer (in bytes):
INFO  :   set hive.exec.reducers.bytes.per.reducer=<number>
INFO  : In order to limit the maximum number of reducers:
INFO  :   set hive.exec.reducers.max=<number>
INFO  : In order to set a constant number of reducers:
INFO  :   set mapreduce.job.reduces=<number>
INFO  : number of splits:2
INFO  : Submitting tokens for job: job_1626505659980_2808
INFO  : Executing with tokens: []
INFO  : The url to track the job: http://jikehadoop03:8088/proxy/application_1626505659980_2808/
INFO  : Starting Job = job_1626505659980_2808, Tracking URL = http://jikehadoop03:8088/proxy/application_1626505659980_2808/
INFO  : Kill Command = /opt/cloudera/parcels/CDH-6.3.2-1.cdh6.3.2.p0.1605554/lib/hadoop/bin/hadoop job  -kill job_1626505659980_2808
INFO  : Hadoop job information for Stage-1: number of mappers: 2; number of reducers: 1
INFO  : 2021-08-05 10:12:31,443 Stage-1 map = 0%,  reduce = 0%
INFO  : 2021-08-05 10:12:50,456 Stage-1 map = 50%,  reduce = 0%, Cumulative CPU 4.79 sec
INFO  : 2021-08-05 10:12:53,571 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 17.58 sec
INFO  : 2021-08-05 10:13:03,215 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 24.89 sec
INFO  : MapReduce Total cumulative CPU time: 24 seconds 890 msec
INFO  : Ended Job = job_1626505659980_2808
INFO  : Launching Job 2 out of 4
INFO  : Starting task [Stage-2:MAPRED] in serial mode
INFO  : Number of reduce tasks not specified. Estimated from input data size: 1
INFO  : In order to change the average load for a reducer (in bytes):
INFO  :   set hive.exec.reducers.bytes.per.reducer=<number>
INFO  : In order to limit the maximum number of reducers:
INFO  :   set hive.exec.reducers.max=<number>
INFO  : In order to set a constant number of reducers:
INFO  :   set mapreduce.job.reduces=<number>
INFO  : number of splits:1
INFO  : Submitting tokens for job: job_1626505659980_2809
INFO  : Executing with tokens: []
INFO  : The url to track the job: http://jikehadoop03:8088/proxy/application_1626505659980_2809/
INFO  : Starting Job = job_1626505659980_2809, Tracking URL = http://jikehadoop03:8088/proxy/application_1626505659980_2809/
INFO  : Kill Command = /opt/cloudera/parcels/CDH-6.3.2-1.cdh6.3.2.p0.1605554/lib/hadoop/bin/hadoop job  -kill job_1626505659980_2809
INFO  : Hadoop job information for Stage-2: number of mappers: 1; number of reducers: 1
INFO  : 2021-08-05 10:13:16,901 Stage-2 map = 0%,  reduce = 0%
INFO  : 2021-08-05 10:13:25,140 Stage-2 map = 100%,  reduce = 0%, Cumulative CPU 2.6 sec
INFO  : 2021-08-05 10:13:33,360 Stage-2 map = 100%,  reduce = 100%, Cumulative CPU 7.14 sec
INFO  : MapReduce Total cumulative CPU time: 7 seconds 140 msec
INFO  : Ended Job = job_1626505659980_2809
INFO  : Launching Job 3 out of 4
INFO  : Starting task [Stage-3:MAPRED] in serial mode
INFO  : Number of reduce tasks not specified. Estimated from input data size: 1
INFO  : In order to change the average load for a reducer (in bytes):
INFO  :   set hive.exec.reducers.bytes.per.reducer=<number>
INFO  : In order to limit the maximum number of reducers:
INFO  :   set hive.exec.reducers.max=<number>
INFO  : In order to set a constant number of reducers:
INFO  :   set mapreduce.job.reduces=<number>
INFO  : number of splits:2
INFO  : Submitting tokens for job: job_1626505659980_2810
INFO  : Executing with tokens: []
INFO  : The url to track the job: http://jikehadoop03:8088/proxy/application_1626505659980_2810/
INFO  : Starting Job = job_1626505659980_2810, Tracking URL = http://jikehadoop03:8088/proxy/application_1626505659980_2810/
INFO  : Kill Command = /opt/cloudera/parcels/CDH-6.3.2-1.cdh6.3.2.p0.1605554/lib/hadoop/bin/hadoop job  -kill job_1626505659980_2810
INFO  : Hadoop job information for Stage-3: number of mappers: 2; number of reducers: 1
INFO  : 2021-08-05 10:13:44,876 Stage-3 map = 0%,  reduce = 0%
INFO  : 2021-08-05 10:13:53,158 Stage-3 map = 50%,  reduce = 0%, Cumulative CPU 4.4 sec
INFO  : 2021-08-05 10:13:59,390 Stage-3 map = 100%,  reduce = 0%, Cumulative CPU 8.89 sec
INFO  : 2021-08-05 10:14:06,615 Stage-3 map = 100%,  reduce = 100%, Cumulative CPU 11.84 sec
INFO  : MapReduce Total cumulative CPU time: 11 seconds 840 msec
INFO  : Ended Job = job_1626505659980_2810
INFO  : Launching Job 4 out of 4
INFO  : Starting task [Stage-4:MAPRED] in serial mode
INFO  : Number of reduce tasks determined at compile time: 1
INFO  : In order to change the average load for a reducer (in bytes):
INFO  :   set hive.exec.reducers.bytes.per.reducer=<number>
INFO  : In order to limit the maximum number of reducers:
INFO  :   set hive.exec.reducers.max=<number>
INFO  : In order to set a constant number of reducers:
INFO  :   set mapreduce.job.reduces=<number>
INFO  : number of splits:1
INFO  : Submitting tokens for job: job_1626505659980_2811
INFO  : Executing with tokens: []
INFO  : The url to track the job: http://jikehadoop03:8088/proxy/application_1626505659980_2811/
INFO  : Starting Job = job_1626505659980_2811, Tracking URL = http://jikehadoop03:8088/proxy/application_1626505659980_2811/
INFO  : Kill Command = /opt/cloudera/parcels/CDH-6.3.2-1.cdh6.3.2.p0.1605554/lib/hadoop/bin/hadoop job  -kill job_1626505659980_2811
INFO  : Hadoop job information for Stage-4: number of mappers: 1; number of reducers: 1
INFO  : 2021-08-05 10:14:20,001 Stage-4 map = 0%,  reduce = 0%
INFO  : 2021-08-05 10:14:28,250 Stage-4 map = 100%,  reduce = 0%, Cumulative CPU 2.2 sec
```



## Question3



```
with most_rate_times_userX as (select u.userid ,  count(1) cnt from t_user u inner join  t_rating  r  on u.userid =r.userid and u.sex='F'
group by  u.userid order by cnt desc 
limit 1
)
,
rates_top10_by_a_userX  as (
select r.movieid,avg(r.rate) as avg_rate  from t_rating  r
inner join most_rate_times_userX rt on r.userid=rt.userid
 group by r.movieid
 order by avg_rate desc 
 limit 10 
 
 )
 
 , avg_rates as (
 select  r.movieid, avg(r.rate) avg_rate from t_rating r  inner join rates_top10_by_a_userX x  on r.movieid =x.movieid
 
 group by r.movieid
 )
 
 select m.moviename ,r.avg_rate  from avg_rates r inner join  
 t_movie m on r.movieid= m.movieid
 
 
```





