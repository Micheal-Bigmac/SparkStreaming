package streaming

import java.util
import java.util.Properties

import com.hsae.rdbms.db2.PreparedSqlAndValues
import com.sxp.task.bolt.GeographyDescriptionBolt
import com.sxp.task.bolt.db2._
import com.sxp.task.bolt.hbase.mapper._
import com.sxp.task.bolt.push.AlarmPushBolt
import com.sxp.task.spout.SpoutStreamTypeId
import com.typesafe.config.ConfigFactory
import kafka.serializer.DefaultDecoder
import org.apache.hadoop.hbase.client.Mutation
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaManager
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.storm.tuple.{Fields, TupleImp, Values}
import tool.{PropertiesUtil, PutHbase}

import scala.collection.mutable
import scala.reflect.ClassTag


/**
  * @author  BigMac Micheal
  * Created by PC on 2017/1/26.
  *
  * 该程序 故意留有两到三处
  *     1  配置文件在 driver 加载后通过 property 重新加载外部自定义的配置  可以做到 加载一次
  *     2  写入数据库(改Db2) 发生 GC 会频繁  通过SparkStreaming  调优 发现 延迟率高 下行写入速度 小于上行下发速度  (优化代码中已经有 并未在改程序中改写)  程序中做了 控制
  *     3  driver  中 Hbase 数据库相关操作 可以在一个foreachRDD  中完成业务处理（性能上可能会有影响 官网给出建议 和 在github 中看到的）
  *     4 有些线程中需要的缓存数据 个人只是按照个人方式实现 无法在程序暂停杀掉后 加载程序业务逻辑中的缓存 （缓存和Spark 中的缓存不一样 业务中的缓存 类似缓存上次结束或者  开始的数据 与当前每条数据比较 ）
  *     5  写入zookeeper  过时问题 已经修改
  * 上面提出的问题是发现留下的问题 代码中留有解决方案只是 没有在该主程序中更改  如果你发现其他问题 请留言 非常感谢
  *
  *
  *  该程序  一开始想写个SparkStreaming DirectKafka Zookeeper 的框架  外部加入一些配置文件  同时支持 Hbase 关系型数据库 的外部存储
  *  后来 写着写着 就把 公司Storm 项目改成sparkStreaming  同时在公司线下跑过测试
  *      改写思想：  几乎不更改原先的代码逻辑或者不动原先代码  自己对Strom bolt tuple 和 数据发送 重新抽象
  *                关于Storm 内部的netty 数据发送 变成手动去处理
  *                storm bolt 缓存部分 提到 driver 代码块区
  *                Java Scala 不同类型的转换 通过显式 或隐式调用 （耗时最长）
  *
  * 疑问   前提： scala  中 变量 在参数传递 故意设计不可变 在spark 中通过 transform  转换 返回的值变成RDD[Any]  在后面transform action 还要用到
  *        1 ： spark 中是在算子 转换过程中是对变量进行内存copy 生成新的变量（地址+内存空间）  还是使用原先的内存地址空间  涉及到GC 问题
  *
  *
  *  耗时 1-2周左右
  *
  */



object DirectKafkaStreamingDemo {

  var broadcast_One: Broadcast[Array[SpoutStreamTypeId]] = null
  var broadCast_Config: Broadcast[Properties] = null
  //  var GeographBolt = new GeographyDescriptionBolt()
  var alarmPush = new AlarmPushBolt()
  var gpsMapper = new GpsMapper()

  /**
    *
    * @param lines
    * @tparam V
    * @return
    * 这个函数处理自己业务 跟写入kafka的数据序列化有关（自己定义的序列化）
    * StringDecoder ---> StringDecoder.deserize
    * DefaultEncoder ---> 二进制(传入什么格式 就是什么格式)
    * notes java byte[]  Scala Array[Byte]
    */
  def dealLine[V: ClassTag](lines: V): List[Mutation] = {
    var list: String = null
    if (lines.isInstanceOf[Array[Byte]]) {
      val results: List[Mutation] = DirectHbase(lines)
      // 原始数据 需要对应的序列化反序列化
      //      val bytes: Array[Byte] = lines.asInstanceOf[Array[Byte]]
      //      list = (ZKStringSerializer.deserialize(bytes)).toString
      return results
    } else if (lines.isInstanceOf[String]) {
      list = lines.asInstanceOf[String]
      return List()
    } else {
      return List()
    }
  }


  def DirectHbase[V: ClassManifest](lines: V): List[Mutation] = {
    import scala.collection._
    val data: (String, String, List[Array[Byte]]) = analysisBinaryData(lines)
    //    JavaConversions.asJavaList(data._3)
    val transValue: Values = new Values(JavaConversions.asJavaList(data._3), data._1, data._2)
    val transFields: Fields = new Fields("Data", "SteamId", "VehicleId")
    val tup = new TupleImp(transFields, transValue)
    data._1 match {
      case Transform._100002 => {
        val GpsAppendMapper = new GpsAppendMapper()
        GpsAppendMapper.mutations(tup)
      }
      case Transform._100003 => {
        val canMapper = new CanMapper()

        canMapper.mutations(tup)
      }
      case Transform._100004 => {
        val alarmMapper = new AlarmMapper()
        alarmMapper.mutations(tup)
      }
      case Transform._100009 => {
        val oilMassMapper = new OilMassMapper()
        oilMassMapper.mutations(tup)
      }
      case Transform._100014 => {
        val youWenTempMapper = new YouWeiTempMapper()
        youWenTempMapper.mutations(tup)
      }
      case Transform._100043 => {
        val mediaMapper = new MediaMapper()
        mediaMapper.mutations(tup)
      }
      case Transform._100044 => {
        val mediaEventMapper = new MediaEventMapper()
        mediaEventMapper.mutations(tup)
      }
      case _ => println("not such stream id")
    }
    return List()
  }

  def analysisBinaryData[V: ClassManifest](lines: V): (String, String, List[Array[Byte]]) = {
    val bytes: Array[Byte] = lines.asInstanceOf[Array[Byte]]
    val Data_type = new Array[Byte](4)
    System.arraycopy(bytes, 0, Data_type, 0, 4)
    val TypeId = Bytes.toInt(Data_type)

    val vechileId = new Array[Byte](8)
    System.arraycopy(bytes, 4, vechileId, 0, 8)
    val Data_Content = new Array[Byte](bytes.length - 12)
    System.arraycopy(bytes, 12, Data_Content, 0, Data_Content.length)
    //    println("type is " + TypeId + " vehicle ID " + vechileId + "  ")
    (TypeId + "", Bytes.toLong(vechileId) + "", List(Data_Content))
  }

  /**
    * 不同的流ID 直接入hbase 除去缓存部分
    *
    * @param rows
    */
  def InsertHbase(rows: RDD[List[Mutation]]): Unit = {
    rows.foreachPartition(rows => {
//            val tt: Iterator[List[Mutation]] =rows
      val putHbase = new PutHbase(broadCast_Config.value)
      while (rows.hasNext) {
        putHbase.execute(rows.next())
      }
    })
  }

  def InsertDB(row: RDD[Object]): Unit = {

    row.foreachPartition(ele => {
      val tt: Iterator[Object] =ele
      while (ele.hasNext) {
        val next: Object = ele.next()
        println(" jieguo  hashCode "+ next.hashCode()  )
        if(next !=Nil){
          var valueses: util.List[PreparedSqlAndValues] = next.asInstanceOf[util.List[PreparedSqlAndValues]]
          println("sqls  size is " + valueses.size())
          //        DB2Excutor.excuteBatchs(valueses)
          valueses = null
        }
      }
    })
  }

  def main(args: Array[String]) {
    ConfigFactory.load()
    Logger.getLogger("org").setLevel(Level.WARN)
    //    val Array(brokers, topics, groupId) = Array("localhost:9092", "topic-online", "test-spark")
    // Create context with 2 second batch interval
    val properites: Properties = PropertiesUtil.getProperites("system.properties")
    //    val property: Map[String, String] = properites.getProperty("alarmType").asInstanceOf[Map[String,String]]


    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")
    val mode: String = properites.getProperty("spark-mode")
    sparkConf.setMaster(mode)
    //    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "5")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "1000")

    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val StreamId: Array[SpoutStreamTypeId] = SpoutStreamTypeId.values()
    broadcast_One = ssc.sparkContext.broadcast(StreamId)
    // Create direct kafka stream with brokers and topics


    val topicsSet = properites.getProperty("topic").split(",").toSet
    broadCast_Config = ssc.sparkContext.broadcast(properites)
    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> properites.getProperty("bootstrap.servers"),
      "serializer.class" -> "kafka.serializer.DefaultEncoder",
      "key.serializer.class" -> "kafka.serializer.StringEncoder",
      "group.id" -> properites.getProperty("group.id"),
      "zookeeper.connect" -> properites.getProperty("zookeeper.connect"),
      "auto.offset.reset" -> "smallest"
    )

    val km = new KafkaManager(kafkaParams)
    val messages: InputDStream[(Array[Byte], Array[Byte])] = km.createDirectStream[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder](
      ssc, kafkaParams, topicsSet)

    val lines: DStream[Array[Byte]] = messages.map(_._2)

    Db2Operation(lines)

//    HbaseOperation(ssc, lines)

    messages.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        //        val tt: RDD[Array[Byte]] =rdd
        // 先处理消息
        //                processRdd[Array[Byte]](rdd)
        // 再更新offsets
//        km.updateZKOffsets(rdd)
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }

  def Db2Operation(lines: DStream[Array[Byte]]): Unit = {
    val Db2Insert: DStream[Object] = lines.map(row => {
      //      val tt: Array[Byte] =row
      //("type is " + TypeId + " vehicle ID " + vechileId + "  ")
      val data: (String, String, List[Array[Byte]]) = analysisBinaryData(row)
//      println(" streamid "+data._1)
      val transValue: Values = new Values(scala.collection.JavaConversions.asJavaList(data._3), data._1, data._2)
      val transFields: Fields = new Fields("Data", "SteamId", "VehicleId")
      val tup = new TupleImp(transFields, transValue)
      data._1 match {
        case Transform._100001 => {
          val alarmaggr = new AlarmDB2Bolt()
          alarmaggr.execute(tup)
          //          val diagnose=new CFBDiagnoseDB2Bolt()
          //          diagnose.execute(tup)
          val result=   alarmaggr.preparedSqls
          println(result.hashCode())
          result
        }
        case Transform._100002 => {
          val append = new AppendDB2Bolt()
          append.execute(tup)
          val result= append.preparedSqls
          println(result.hashCode())
          result
        }
        case Transform._100003 => {
          val can = new CanDB2Bolt()
          can.execute(tup)
          val result= can.preparedSqls
          println(result.hashCode())
          result
        }
        case Transform._100005 => {
          val AlarmAggr = new AlarmAggrDB2Bolt()
          AlarmAggr.execute(tup)
          val result= AlarmAggr.preparedSqls
          println(result.hashCode())
          result
        }
        case Transform._100007 => {
          val AreaInout = new AreaInoutDB2Bolt()
          AreaInout.execute(tup)
          val result=  AreaInout.preparedSqls
          println(result.hashCode())
          result
        }
        case Transform._100008 => {
          val ExceptionAlarm = new ExceptionAlarmDB2Bolt()
          ExceptionAlarm.execute(tup)
          val result=   ExceptionAlarm.preparedSqls
          println(result.hashCode())
          result
        }
        case Transform._100009 => {
          val OilMass = new OilMassDB2Bolt()
          OilMass.execute(tup)
          val result=   OilMass.preparedSqls
          println(result.hashCode())
          result
        }
        case Transform._100010 => {
          val OverSpeed = new OverSpeedDB2Bolt()
          OverSpeed.execute(tup)
          val result=   OverSpeed.preparedSqls
          println(result.hashCode())
          result
        }
        case Transform._100011 => {
          val OverTime = new OverTimeDB2Bolt()
          OverTime.execute(tup)
          val result=   OverTime.preparedSqls
          println(result.hashCode())
          result
        }
        case Transform._100034 => {
          val OnLine = new OnLineDB2Bolt()
          OnLine.execute(tup)
          val result=OnLine.preparedSqls
          println(result.hashCode())
          result
        }
        case Transform._100035 => {
          val DeviceReg = new DeviceRegDB2Bolt()
          DeviceReg.execute(tup)
          val result= DeviceReg.preparedSqls
          println(result.hashCode())
          result
        }
        case Transform._100036 => {
          val DeviceLogOut = new DeviceLogOutDB2Bolt()
          DeviceLogOut.execute(tup)
          val result=DeviceLogOut.preparedSqls
          println(result.hashCode())
          result
        }
        case Transform._100037 => {
          val AuthorityLog = new AuthorityLogDB2Bolt()
          AuthorityLog.execute(tup)
          val result=AuthorityLog.preparedSqls
          println(result.hashCode())
          result
        }
        case Transform._100038 => {
          val DeviceParms = new DeviceParmsDB2Bolt()
          DeviceParms.execute(tup)
          val result=   DeviceParms.preparedSqls
          println(result.hashCode())
          result

        }
        case Transform._100043 => {
          val Media = new MediaDB2Bolt()
          Media.execute(tup)
          val result=  Media.preparedSqls
          println(result.hashCode())
          result
        }
        case Transform._100044 => {
          val MediaEvent = new MediaEventDB2Bolt()
          MediaEvent.execute(tup)
          val result= MediaEvent.preparedSqls
          println(result.hashCode())
          result
        }
        case Transform._100045 => {
          val MediaSearch = new MediaSearchDB2Bolt()
          MediaSearch.execute(tup)
           val result= MediaSearch.preparedSqls
          println(result.hashCode())
          result
        }
        case _ => {
          println(" this not such StreamId")
          val result=List()
          println("传递 "+result.hashCode())
          result
//          Nil
        }
      }
    })
    Db2Insert.print(1)
    Db2Insert.foreachRDD(row => {
      if (!row.isEmpty()) {
        val tt: RDD[Object] = row
        InsertDB(row)
      }
    })

  }

  //    def merge(y:List[(Array[Byte],Array[Byte])],z:(Array[Byte],Array[Byte])): List[(Array[Byte],Array[Byte])] ={
  //      y.+:(z)
  //    }
  //    def reduce(x:List[(Array[Byte],Array[Byte])],y:List[(Array[Byte],Array[Byte])]): List[(Array[Byte],Array[Byte])] ={
  //      x :::y
  //    }
  def HbaseOperation(ssc: StreamingContext, lines: DStream[Array[Byte]]): Unit = {
    val filter_100001: DStream[Array[Byte]] = lines.filter(row => {
      val data: (String, String, List[Array[Byte]]) = analysisBinaryData(row)
      if (data._1.equals(Transform._100001)) {
        true
      } else {
        false
      }
    })

    filter_100001.map(row => {
      import scala.collection.JavaConversions
      val data: (String, String, List[Array[Byte]]) = analysisBinaryData(row)
      val transValue: Values = new Values(JavaConversions.asJavaList(data._3), data._1, data._2)
      val transFields: Fields = new Fields("Data", "SteamId", "VehicleId")

      alarmPush.prepare(broadCast_Config.value) // 报警的配置文件需要另外转换成Map
      val tup = new TupleImp(transFields, transValue)
      alarmPush.execute(tup)
    })


    val gpsWithGeo: DStream[List[AnyRef]] = filter_100001.map(row => {
      import collection.JavaConversions
      val data: (String, String, List[Array[Byte]]) = analysisBinaryData(row)
      val transValue: Values = new Values(JavaConversions.asJavaList(data._3), data._1, data._2)
      val transFields: Fields = new Fields("Data", "SteamId", "VehicleId")
      val tup = new TupleImp(transFields, transValue)
      val GeographBolt = new GeographyDescriptionBolt()
      GeographBolt.prepare(broadCast_Config.value)
      GeographBolt.execute(tup)
      //      println(GeographBolt.toString)
      import collection.JavaConversions._
      GeographBolt.results.toList
    })

    gpsWithGeo.cache()
    val gpsWithGeoTuple: DStream[(Long, (AnyRef, AnyRef))] = gpsWithGeo.map(row => {
      val tt: List[AnyRef] = row
      val vehicle_Id: Long = row(0).asInstanceOf[Long]
      //      val ori: Array[Byte] = row(1).asInstanceOf[Array[Byte]]
      //      val oriWithGeo: Array[Byte] = row(2).asInstanceOf[Array[Byte]]
      (vehicle_Id, (row(1).asInstanceOf[util.ArrayList[AnyRef]].get(0), row(2).asInstanceOf[util.ArrayList[AnyRef]].get(0)))
    })

    val gpsWithGeoByVehicle: DStream[(Long, List[(AnyRef, AnyRef)])] = {
      gpsWithGeoTuple.combineByKey(x => List(x), (y: List[(AnyRef, AnyRef)], z: (AnyRef, AnyRef)) => y.+:(z), (r: List[(AnyRef, AnyRef)], q: List[(AnyRef, AnyRef)]) => r ::: (q), new HashPartitioner(ssc.sparkContext.defaultParallelism), true)
    }
    //val gpsWithGeoByVehicle: RDD[(Int, Iterable[Array[Byte]])] = gpsWithGeoTuple.groupByKey()  性能不高
    //    gpsWithGeoByVehicle.cache()
    gpsWithGeoByVehicle.foreach(row => {
      //      val tt: RDD[(Long, List[(AnyRef, AnyRef)])] = row
      row.foreach(row => {
        println("vehicle Id" + row._1 + " list size is " + row._2.length)
        //        row._2.foreach(tt => {
        //          val t: (AnyRef, AnyRef) = tt
        //          import GpsInfo.Gps
        //          val from: Gps = Gps.parseFrom(tt._1.asInstanceOf[Array[Byte]])
        //          println(from.getVehicleID + " 经度 " + from.getLongitude + " 纬度 " + from.getLatitude + " speed " + from.getDrSpeed)
        //        })
      })
    })

    val gpsWithGeoByVehicleToCache: DStream[List[Mutation]] = gpsWithGeoByVehicle.mapPartitions(row => {

      val tt: Iterator[(Long, List[(AnyRef, AnyRef)])] = row
      val result: List[List[Mutation]] = List[List[Mutation]]()
      row.foreach(ele => {
        //        val _one: (Long, List[(AnyRef, AnyRef)]) =ele
        val SourceBinary: mutable.MutableList[AnyRef] = scala.collection.mutable.MutableList[AnyRef]()
        val SourceListWithGeo: mutable.MutableList[AnyRef] = scala.collection.mutable.MutableList[AnyRef]()
        val _two: List[(AnyRef, AnyRef)] = ele._2
        _two.foreach(part => {
          SourceListWithGeo.+=(part._2)
          SourceBinary.+=(part._1)
        })
        import scala.collection.JavaConversions._
        val SourceBinaryJava: util.List[Object] = SourceBinary
        val SourceBinaryWithGeoJava: util.List[Object] = SourceListWithGeo
        val transValue: Values = new Values(ele._1.asInstanceOf[Object], SourceBinaryJava, SourceBinaryWithGeoJava)
        val transFields: Fields = new Fields("vehicleId", "SourceGps", "SourceGpsWithGeo")
        val tup = new TupleImp(transFields, transValue)

        val mutations: util.List[Mutation] = gpsMapper.mutations(tup)
        result.::(mutations)
      })
      result.toIterator
    })



    gpsWithGeoByVehicleToCache.print()

    val filter_Exclude_100001: DStream[Array[Byte]] = lines.filter(row => {
      val data: (String, String, List[Array[Byte]]) = analysisBinaryData(row)
      if (data._1.equals(Transform._100001)) {
        false
      } else {
        true
      }
    })

    val filter_Exclude_10001_Hbase: DStream[List[Mutation]] = filter_Exclude_100001.map(dealLine(_))
    //    filter_Exclude_10001_Hbase.foreachRDD(row=>{
    //       val tt: RDD[List[Mutation]] =row
    //      if(!row.isEmpty()){
    //        InsertHbase(row)
    //      }
    //    })
    //    InsertHbase(words)


    val all_Hbase: DStream[List[Mutation]] = gpsWithGeoByVehicleToCache.union(filter_Exclude_10001_Hbase)

    all_Hbase.foreachRDD(row => {
      if (!row.isEmpty()) {
        InsertHbase(row)
      }
    })
  }
}