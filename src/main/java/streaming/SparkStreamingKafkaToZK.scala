package streaming

import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, TaskContext}
import org.slf4j.LoggerFactory


/***
  * 这个类只是在任务执行完毕后 把kafka 读到的offset 更新到zookeeper 中  并不能在下次执行从保存到zk中获取到每个分区的数据进行
  */
object SparkStreamingKafkaToZK {

  private val zkClient = new ZkClient("10.10.11.11:2181,10.10.11.12:2181,10.10.11.13:2181/kafka",5000,5000,ZKStringSerializer)
  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]) {
    //    if (args.length < 2) {
    //      println("Usage:WebPagePopularityValueCalculator zkserver1:2181,zkserver2:2181,zkserver3:2181 consumeMsgDataTimeInterval (secs) ")
    //      System.exit(1)
    //    }

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    // Kafka configurations
    val topics = Set("SparkTest")
    val kafkaParams: Map[String, String] = getKafkaParam()
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("KafkaStreamingTest")
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "1000") // 控制上行 下行数据 rate
    sparkConf.set("spark.local.dir", "D:/SparkTmp")

    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    //430


    /*val Array(zkQuorum, group, topics, processingInterval) = args
    val topicMap = topics.split(",").map((_, processingInterval.toInt)).toMap
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("KafkaStreamingTest")
    sparkConf.set("spark.local.dir", "D:/SparkTmp")
    sparkConf.set("spark.streaming.receiver.writeAheadLog.enable","false")

    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")
    val kafkaStream = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)*/

    val msgDataRDD: DStream[String] = kafkaStream.map(_._2) // _._1  值为null

    //for debug use only
        msgDataRDD.print()
    // e.g page37|5|1.5119122|-1
     val popularityData = msgDataRDD.map { msgLine => {
     val dataArr: Array[String] = msgLine.split("\\|")
     val pageID = dataArr(0)
     //calculate the popularity value
     val popValue: Double = dataArr(1).toFloat * 0.8 + dataArr(2).toFloat * 0.8 + dataArr(3).toFloat * 1
     (pageID, popValue)
   }
   }
   //    popularityData.print()
   //sum the previous popularity value and current value
   val updatePopularityValue = (iterator: Iterator[(String, Seq[Double], Option[Double])]) => {
     def extract: ((String, Seq[Double], Option[Double])) => Iterable[(String, Double)] = {
       t => {
         //          println("key is" + t._1 + "updateState " + t._2 + "========" + t._3.getOrElse(0))
         val newValue: Double = t._2.sum
         val stateValue: Double = t._3.getOrElse(0)
         Some(newValue + stateValue)
       }.map(sumedValue => (t._1, sumedValue))
     }
     iterator.flatMap(extract)
   }
val initialRDD = ssc.sparkContext.parallelize(List(("page1", 0.00)))
   val stateDstream = popularityData.updateStateByKey[Double](updatePopularityValue,
     new HashPartitioner(ssc.sparkContext.defaultParallelism), true, initialRDD)
    //set the checkpoint interval to avoid too frequently data checkpoint which may
    //may significantly reduce operation throughput
//    stateDstream.checkpoint(Duration(8 * 2 * 1000));
//    stateDstream.print()



    kafkaStream.foreachRDD { rdd =>
      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition { iter =>
        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        val topicDir = new ZKGroupTopicDirs(kafkaParams.get("group.id").get, o.topic)
        val zkPath = s"${topicDir.consumerOffsetDir}/${o.partition}"
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset} ${zkPath}")
        ZkUtils.updatePersistentPath(zkClient,zkPath, o.untilOffset.toString)
        logger.info(s"Offset update: set offset of ${o.topic}/${o.partition} as ${o.untilOffset.toString}")
      }
    }
    //after calculation, we need to sort the result and only show the top 10 hot pages
    /* stateDstream.foreachRDD { rdd => {
       val sortedData = rdd.map { case (k, v) => (v, k) }.sortByKey(false)
       val topKData = sortedData.take(10).map { case (v, k) => (k, v) }
       topKData.foreach(x => {
         println(x)
       })
     }
     }*/

    sys.ShutdownHookThread {
      ssc.stop(true, true)
      println("Application stopped")
    }
    ssc.start()
    ssc.awaitTermination()
  }

  def getKafkaParam(): Map[String, String] = {
    //    val brokers = "10.10.11.14:9092,10.10.11.15:9092,10.10.11.16:9092"
    //    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "serializer.class" -> "kafka.serializer.StringEncoder")
    val param = collection.mutable.Map[String, String]()
    param += ("bootstrap.servers" -> "10.10.11.14:9092,10.10.11.15:9092,10.10.11.16:9092")
    param += ("serializer.class" -> "kafka.serializer.StringEncoder")
    param += ("group.id" -> "test-spark")
    param += ("auto.offset.reset" -> "smallest")
    //    param += ("auto.offset.reset" -> "largest")
    param += ("enable.auto.commit" -> "false")
    //    param +("enable.auto.commit"->) // 是否提
    // 交到zk
    param.toMap
  }


}