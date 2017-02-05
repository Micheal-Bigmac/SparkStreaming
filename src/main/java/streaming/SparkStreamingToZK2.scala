package streaming

import java.util

import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo, PartitionOffsetsResponse}
import kafka.common.{BrokerNotAvailableException, TopicAndPartition}
import kafka.consumer.SimpleConsumer
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{Json, ZKGroupTopicDirs, ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.zookeeper.data.Stat
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
/**
  * 这个类是 在每次任务成功后把数据提交到ZK 中 同时在下次重新启动时也重新读取ZK 中获取每个分区数据 继续消费  缺点： 不能对每个分区动态切换
  */
object SparkStreamingToZK2 {
  val logger = LoggerFactory.getLogger(this.getClass)
  val chroot = "/kafka"
  val zkClient = new ZkClient("10.10.11.11:2181,10.10.11.12:2181,10.10.11.13:2181/kafka", 5000, 5000, ZKStringSerializer)

  def main(args: Array[String]) {
    val groupId = "test-Spark-version1"

    val topics = List("SparkTest")
    val topicPartition = ZkUtils.getPartitionsForTopics(zkClient, topics)
//    var fromOffsets: collection.mutable.Map[TopicAndPartition, Long] = collection.mutable.Map()

    val fromOffsets=new util.HashMap[TopicAndPartition,Long]()
    topicPartition.foreach(row => {
      val topic = row._1
      val partitions = row._2
      val topicDir = new ZKGroupTopicDirs(groupId, topic)
      partitions.foreach(partition => {
        val zkPath = s"${chroot}${topicDir.consumerOffsetDir}/${partition}"
        ZkUtils.makeSurePersistentPathExists(zkClient, zkPath)
        val untilOffset: String = zkClient.readData[String](zkPath)

        val tp = TopicAndPartition(topic, partition)
        val offset = {
          if (untilOffset == null || untilOffset.trim == "") {
            getMaxOffset1(tp)
          } else {
            untilOffset.toLong
          }
        }
        fromOffsets.put(tp,offset)
        logger.info(s" offset init : set offset of $topic/$partition as $offset")
      })
    })
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    // Kafka configurations
    val kafkaParams: Map[String, String] = getKafkaParam()
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("KafkaStreamingTest")
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "1000") // 控制上行 下行数据 rate
    sparkConf.set("spark.local.dir", "D:/SparkTmp")

    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")
    val messageHandler: (MessageAndMetadata[String, String]) => (String, String) = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets.toMap, messageHandler)
    //    KafkaUtils.createDirectStream()

    //    val kafkaData: DStream[String] = kafkaStream.map(_._2)
    //    kafkaData.print() //debug


    kafkaStream.foreachRDD { rdd =>
      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition { iter =>
        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
        //        kafkaStream.asInstanceOf[CanCommitOffsets]
        val topicDir = new ZKGroupTopicDirs(kafkaParams.get("group.id").get, o.topic)
        val zkPath = s"${topicDir.consumerOffsetDir}/${o.partition}"
        ZkUtils.updatePersistentPath(zkClient, zkPath, o.untilOffset.toString)
        logger.info(s"Offset update: set offset of ${o.topic}/${o.partition} as ${o.untilOffset.toString}")
      }

    }
    sys.ShutdownHookThread {
      ssc.stop(true, true)
      println("Application stopped")
    }
    ssc.start()
    ssc.awaitTermination()
  }

  def getMaxOffset1(tp:TopicAndPartition):Long={
    val requestMap = collection.immutable.Map(tp -> PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1))
    val request: OffsetRequest = OffsetRequest(requestMap)
    ZkUtils.getLeaderForPartition(zkClient, tp.topic, tp.partition) match {
      case Some(brokerId) => {
        val Data=  zkClient.readData[String](ZkUtils.BrokerIdsPath + "/" + brokerId)
        Json.parseFull(Data) match {
          case Some(m)=>{
            val brokerInfo = m.asInstanceOf[Map[String, Any]]
            val host = brokerInfo.get("host").get.asInstanceOf[String]
            val port = brokerInfo.get("port").get.asInstanceOf[Int]
            val simpleconsumer: PartitionOffsetsResponse = new SimpleConsumer(host, port, 1000, 10000, "getMaxOffset")
              .getOffsetsBefore(request)
              .partitionErrorAndOffsets(tp)
            simpleconsumer.offsets.head
          }
          case None =>{
            throw new BrokerNotAvailableException("broker id %d does not exist ".format(brokerId))
          }
        }

      }
      case _ =>{
        throw new BrokerNotAvailableException(" not broker at this topic ")
      }
    }
  }

  def getMaxOffset(tp: TopicAndPartition): Long = {

    val requestMap = collection.immutable.Map(tp -> PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1))
    val request = OffsetRequest(requestMap)

    ZkUtils.getLeaderForPartition(zkClient, tp.topic, tp.partition) match {
      case Some(brokerId) => {
//        val maybeNull =  zkClient.readData[String](ZkUtils.BrokerIdsPath + "/" + brokerId)
        val maybeNull: (Option[String], Stat) = ZkUtils.readDataMaybeNull(zkClient,  ZkUtils.BrokerIdsPath + "/" + brokerId)
        maybeNull._1 match {
          case Some(brokerInfoString) => {
            Json.parseFull(brokerInfoString) match {
              case Some(m) => {
                val brokerInfo = m.asInstanceOf[Map[String, Any]]
                val host = brokerInfo.get("host").get.asInstanceOf[String]
                val port = brokerInfo.get("port").get.asInstanceOf[Int]
                val simpleconsumer: PartitionOffsetsResponse = new SimpleConsumer(host, port, 1000, 10000, "getMaxOffset")
                  .getOffsetsBefore(request)
                  .partitionErrorAndOffsets(tp)
                simpleconsumer.offsets.head
              }
              case None => {
                throw new BrokerNotAvailableException("broker id %d does not exist ".format(brokerId))
              }
            }
          }
          case _ =>
            throw new BrokerNotAvailableException("Broker id %d does not exist".format(brokerId))
        }
      }
      case _ => {
        throw new Exception("No broker for partition %s - %s\".format(tp.topic, tp.partition)")
      }
    }
  }

  def getKafkaParam(): Map[String, String] = {
    //    val brokers = "10.10.11.14:9092,10.10.11.15:9092,10.10.11.16:9092"
    //    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "serializer.class" -> "kafka.serializer.StringEncoder")
    val param = collection.mutable.Map[String, String]()
    param += ("bootstrap.servers" -> "10.10.11.14:9092,10.10.11.15:9092,10.10.11.16:9092")
    param += ("serializer.class" -> "kafka.serializer.StringEncoder")
    param += ("group.id" -> "test-spark")
    //    param += ("auto.offset.reset" -> "smallest")
    //    param += ("auto.offset.reset" -> "largest")
    param += ("enable.auto.commit" -> "false")
    //    param +("enable.auto.commit"->) // 是否提
    // 交到zk
    param.toMap
  }
}
