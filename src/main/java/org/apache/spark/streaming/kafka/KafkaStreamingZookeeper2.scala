package org.apache.spark.streaming.kafka

import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo}
import kafka.common.{BrokerNotAvailableException, TopicAndPartition}
import kafka.consumer.SimpleConsumer
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{Json, ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, TaskContext}
import org.slf4j.LoggerFactory

/**
  * Created by PC on 2016/12/28.
  */
object KafkaStreamingZookeeper2 {
  private val zkClient = new ZkClient("10.10.11.11:2181,10.10.11.12:2181,10.10.11.13:2181")
  private val logger = LoggerFactory.getLogger(this.getClass)

  def test(args: Array[String]): Unit = {

    val groupId = "test-Spark"
    val topics = List("SparkTest")
    // Kafka configurations
    val kafkaParams: Map[String, String] = getKafkaParam()
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("KafkaStreamingTest")
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "1000") // 控制上行 下行数据 rate
    sparkConf.set("spark.local.dir", "D:/SparkTmp")

    val kc = new KafkaCluster(kafkaParams)
    val reset = kafkaParams.get("auto.offset.reset").map(_.toLowerCase)
    var fromOffsets = Map[TopicAndPartition, Long]()
    val result = for {
      topicPartitions <- kc.getPartitions(topics.toSet).right
      leaderOffsets <- (if (reset == Some("smallest")) {
        kc.getEarliestLeaderOffsets(topicPartitions)
      } else {
        kc.getLatestLeaderOffsets(topicPartitions)
      }).right
    } yield {
      fromOffsets = leaderOffsets.map { case (tp, lo) =>
        (tp, lo.offset)
      }
    }
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")
    val messageHandler: (MessageAndMetadata[String, String]) => (String, String) = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)

    kafkaStream.foreachRDD { rdd =>
      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition { iter =>
        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
        val topicDir = new ZKGroupTopicDirs(kafkaParams.get("group.id").get, o.topic)
        val zkPath = s"${topicDir.consumerOffsetDir}/${o.partition}"
        ZkUtils.updatePersistentPath(zkClient, zkPath, o.untilOffset.toString)
        logger.info(s"Offset update: set offset of ${o.topic}/${o.partition} as ${o.untilOffset.toString}")
      }

      sys.ShutdownHookThread {
        ssc.stop(true, true)
        println("Application stopped")
      }
      ssc.start()
      ssc.awaitTermination()
    }
  }

  private def getMaxOffset(tp: TopicAndPartition): Long = {
    val requestMap = collection.immutable.Map(tp -> PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1))
    val request = OffsetRequest(requestMap)

    ZkUtils.getLeaderForPartition(zkClient, tp.topic, tp.partition) match {
      case Some(brokerId) =>
        ZkUtils.readDataMaybeNull(zkClient, ZkUtils.BrokerIdsPath + "/" + brokerId)._1 match {
          case Some(brokerInfoString) =>
            Json.parseFull(brokerInfoString) match {
              case Some(m) =>
                val brokerInfo = m.asInstanceOf[Map[String, Any]]
                val host = brokerInfo.get("host").get.asInstanceOf[String]
                val port = brokerInfo.get("port").get.asInstanceOf[Int]
                new SimpleConsumer(host, port, 1000, 10000, "getMaxOffset")
                  .getOffsetsBefore(request)
                  .partitionErrorAndOffsets(tp)
                  .offsets.head
              case None =>
                throw new BrokerNotAvailableException("broker id %d does not exist ".format(brokerId))
            }
          case None =>
            throw new BrokerNotAvailableException("Broker id %d does not exist".format(brokerId))
        }
      case _ =>
        throw new Exception("No broker for partition %s - %s\".format(tp.topic, tp.partition)")
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
    param += ("auto.offset.reset" -> "largest")
    param += ("enable.auto.commit" -> "false")
    //    param +("enable.auto.commit"->) // 是否提
    // 交到zk
    param.toMap
  }
}
