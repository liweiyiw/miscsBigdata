# sparkRDD exerciser 
本人win10， 读取目录或者 saveAsTextFile 都会报  exception org.apache.hadoop.io.nativeio.NativeIO$Windows.access0, 尝试之后没有解决这个问题，所以代码是读一个文件，然后println



代码路径

https://github.com/liweiyiw/miscsBigdata/tree/main/hm/sparkRDD/src/main/scala/inversedIndex



运行结果

```
21/08/27 16:46:11 INFO DAGScheduler: Job 0 finished: collect at InversedIndex.scala:31, took 1.300311 s
21/08/27 16:46:11 INFO SparkUI: Stopped Spark web UI at http://DESKTOP-JJOU3L3.cn.ibm.com:4040
Spark:{(text1.txt,162)}
to:{(text1.txt,150)}
the:{(text1.txt,138)}
of:{(text1.txt,102)}
on:{(text1.txt,96)}
and:{(text1.txt,78)}
can:{(text1.txt,72)}
in:{(text1.txt,60)}
or:{(text1.txt,54)}
as:{(text1.txt,54)}
run:{(text1.txt,48)}
you:{(text1.txt,42)}
data:{(text1.txt,42)}
I:{(text1.txt,42)}
a:{(text1.txt,42)}
is:{(text1.txt,36)}
Hadoop:{(text1.txt,36)}
are:{(text1.txt,36)}
use:{(text1.txt,30)}
....
```





