package inversedIndex

import org.apache.spark.sql.SparkSession

case object InversedIndex {
  def main(args: Array[String]) {

    /*
     本人win10， 读取目录或者 saveAsTextFile 都会报  exception org.apache.hadoop.io.nativeio.NativeIO$Windows.access0,
     尝试之后没有解决这个问题，所以练习是读一个文件，然后println
     */

    //      val docDir = "C:\\data\\"
    val docDir = "C:\\data\\text1.txt"

    val spark = SparkSession.builder.appName("InversedIndex")
      .master("local")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    val files = spark.sparkContext.wholeTextFiles(docDir) //wholeTextFiles exists in sparkContext
    val wordDocPair=files
      .flatMap {
        line =>
          val docName = line._1.substring(line._1.lastIndexOf("/") + 1)
          line._2.split("[\\n\\r\\s?]+").map(word => (word, docName)) // in scala, the last statement is the return
      }.map(wordDoc => ((wordDoc._1, wordDoc._2), 1))

      .reduceByKey(_ + _)
      .map(wd=> wd._1._1.toString()+":" + "{"+  (wd._1._2,wd._2) +   "}" ) //

    wordDocPair.collect().foreach(println)

    spark.stop()
  }
}
