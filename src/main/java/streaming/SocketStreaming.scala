package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by PC on 2016/11/28.
  */
object SocketStreaming {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("networkcount")
    val socketStream = new StreamingContext(conf, Seconds(1))
//     val line: Any = socketStream.socketTextStream(args(0),args(1).toInt,StorageLevel.MEMRY_AND_DISK_SER)L
    val line = socketStream.socketTextStream("localhost", 8888)
    val word: DStream[String] = line.flatMap(row => {
      println("The current row value is "+row)
      row.split(" ")
    })
    val pairs: DStream[(String, Int)] = word.map(row=>{(row,1)})
    val wordCount: DStream[(String, Int)] = pairs.reduceByKey((x:Int, y:Int) => {
      x + y
    })
    wordCount.print()
    socketStream.start()
    socketStream.awaitTermination()
  }
}
