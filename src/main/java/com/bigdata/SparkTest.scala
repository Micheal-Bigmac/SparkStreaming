package com.bigdata

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.StatCounter

import scala.collection.Map
import scala.util.Random

class SparkTest extends Serializable {
  def test1(sc: SparkContext) {
    val nums = sc.parallelize(List(Tuple2(1, 1), Tuple2(1, 3), Tuple2(2, 2), Tuple2(2, 8)));
    val results = nums.foldByKey(3)({ case (x, y) => x + y })
    results.foreach(println)
  }

  def test2(sparkContext: SparkContext): Unit = {
    val rdd1 = sparkContext.parallelize(List("this is a test", "how are you", "do you love me", "can you tell me"))
    rdd1.foreach {
      println
    }
    val words = rdd1.flatMap(line => line.split(" "))
    words.foreach {
      println
    }
    val results = words.map(word => (word, 1)).reduceByKey({ case (x, y) => x + y })
    results.foreach(println)
  }

  def test3(sc: SparkContext): Unit = {
    val nums = sc.parallelize(List(1, 2, 3, 4, 5, 6))
    val results: Int = nums.fold(0)({ case (x, y) => x + y })
    //    val results = nums.foldByK(3)({ case (x, y) => x + y })
    println(results)
  }

  def test4(sc: SparkContext): Unit = {

    val a = sc.parallelize(List(1, 2, 1, 3), 1)
    val b = a.map((_, "b"))
    val c = a.map((_, "c"))
    val d: RDD[(Int, (Iterable[String], Iterable[String]))] = b.cogroup(c)
    //    d.collect()

    d.foreach(row => {
      val _1: Int = row._1
      val _2: (Iterable[String], Iterable[String]) = row._2

      println("key ： " + _1 + " value : " + _2)
    })
  }

  /*
     combineByKey
   */
  def test5(sc: SparkContext): Unit = {
    val data = sc.parallelize(List((1, "www"), (1, "itblog"), (1, "com"), (2, "bbs"), (2, "iteblog"), (3, "good")), 3)
    val data1 = data mapPartitionsWithIndex ((x: Int, y: Iterator[(Int, String)]) => {
      println("partition is " + x)
      println(y.toList)
      y
    })

    data1.collect()

    val result: RDD[(Int, List[String])] = data.combineByKey(List(_), (x: List[String], y: String) => {
      println(x);
      println(y);
      y :: x
    }, (x: List[String], y: List[String]) => {
      println(x.length);
      println(y.length);
      x ::: y
    })
    result.foreach(row => {
      val key = row._1
      val valu = row._2
      println(" result is  key :" + key + " value: " + valu)
    })

    //    result.collect()
  }

  /*
     计算单词数量 类似于 reduceBykey 但性能高于reduceBykey 推荐使用
   */
  def test6(sc: SparkContext): Unit = {
    val data = sc.parallelize(List(("iteblog", 1), ("bbs", 1), ("iteblog", 3)), 2)
    val data1 = data mapPartitionsWithIndex ((x: Int, y: Iterator[(String, Int)]) => {
      println("partition is " + x)
      println(y.toList)
      y
    })
    data1.collect()
    val result = data.combineByKey(x => x, (x: Int, y: Int) => x + y, (x: Int, y: Int) => x + y)
    result.foreach(row => {
      val key = row._1
      val valu = row._2
      println("key is :" + key + " value is  :" + valu)
    })
  }

  def tt2: (Int, Iterator[(String, Int)]) => Iterator[(String, Int)] = {
    (x: Int, y: Iterator[(String, Int)]) => {
      println("partition is " + x)
      println(y.toList)
      y
    }
  }

  def tt: (Int, Int) => Int = {
    (x: Int, y: Int) => x + y
  }


  def test7(sc: SparkContext): Unit = {
    val rdd1 = sc.parallelize(List((3, "Gnu"), (3, "spark"), (5, "mouse"), (3, "dog")))
    val rdd2: Map[Int, Long] = rdd1.countByKey();
    rdd2.foreach((row: (Int, Long)) => {
      val key = row._1
      val value = row._2
      println("key is :" + key + " value is :" + value)
    })
  }

  //  def test8(sc:SparkContext): Unit ={
  //    val a = sc.parallelize(1 to 10000, 20)
  //    val b: RDD[Int] =a++a++a++a++a
  //    val c =b.countApproxDistinct(0.1l)
  //    println(c)
  //    val d = b.countApproxDistinct(0.05l)
  //    println(d)
  //    val e = b.countApproxDistinct(0.01l)
  //    println(e)
  //    val f= b.countApproxDistinct(0.001l)
  //    println(f)
  //  }
  def test9(sc: SparkContext): Unit = {
    val rdd1 = sc.parallelize(List("gnu", "cat", "dot", "rat", "gnu", "rat", "wes", "sfwe"))
    val rdd3 = rdd1.mapPartitionsWithIndex((x: Int, y: Iterator[String]) => {
      println("partition is  :" + x + " value is " + y.toList)
      y
    })
    rdd3.collect()
    val rdd2 = rdd1.distinct();
    rdd2.foreach(println)
  }

  def test10(sc: SparkContext): Unit = {
    val rdd1 = sc.parallelize(List((2, "cat"), (3, "mouse"), (1, "cup"), (5, "book"), (4, "tv"), (1, "screen"), (5, "healter")), 3)
    //    val rdd2=rdd1.mapPartitionsWithIndex((x:Int,y:Iterator[(Int,String)])=>{
    //      println("partition is "+x +" value is "+ y.toList)
    //      y
    //    })
    //    rdd2.collect()
    //    val rdd3: RDD[(Int, String)] =rdd1.sortByKey()
    //    rdd3.foreach(row=>{
    //      val key = row._1
    //      val value =row._2
    //      println("key is :"+key +" value is "+value)
    //    })
    val rdd4 = rdd1.filterByRange(1, 3)
    rdd4.foreach(row => {
      val key = row._1
      val value = row._2
      println("key is :" + key + " value is " + value)
    })
  }

  def test11(sc: SparkContext): Unit = {
    val rdd1 = sc.parallelize(1 to 10, 5)
    val rdd2 = rdd1.mapPartitionsWithIndex((x: Int, y: Iterator[(Int)]) => {
      println("partition is " + x + " value is " + y.toList)
      y
    })
    rdd2.collect()
    val rdd3: RDD[Int] = rdd1.flatMap((x: Int) => {
      List(x, x, x)
    })
    val rdd4 = rdd3.mapPartitionsWithIndex((x: Int, y: Iterator[(Int)]) => {
      println("partition is " + x + " value is " + y.toList)
      y
    })
    rdd4.collect()
    rdd3.foreach((x: Int) => {
      println(x)
    })
  }

  def test12(sc: SparkContext): Unit = {
    val rdd1 = sc.parallelize(List("dog", "tiger", "cat", "lione", "pandar", "eagle"))
    val rdd2: RDD[(Int, String)] = rdd1.map(x => (x.length, x))
    //    rdd2.foreach(row => {
    //      println("key is "+ row._1+ " value is "+row._2)
    //    })
    val rdd3 = rdd2.flatMapValues((x: String) => {
      println(x)
      x
    })
    rdd3.collect()

    //    val rdd3 =rdd2.flatMapValues("x" + _ + "x")
    rdd3.foreach(row => {
      println("key is " + row._1 + " value is " + row._2)
    })
  }

  def test13(sc: SparkContext): Unit = {
    val rdd1 = sc.parallelize(List(1, 2, 3, 4, 5), 3)
    val rdd2 = rdd1.fold(0)(_ + _)
    println(rdd2)
  }

  def test14(sc: SparkContext): Unit = {
    val rdd1 = sc.parallelize(List("dog", "catsdfasdfa", "owlsfads", "gu", "ant"), 2)
    val rdd2 = rdd1.map(x => (x.length, x))
    val rdd3 = rdd2.foldByKey("aaa-----aaaa-----")(_ + _)
    rdd3.foreach(row => {
      println("key is :" + row._1 + " value is :" + row._2)
    })
  }

  def test15(sc: SparkContext): Unit = {
    val rdd1 = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11), 3)
    rdd1.foreachPartition((x: Iterator[Int]) => {
      println(x.toList)
    })
  }

  def test16(sc: SparkContext): Unit = {
    sc.setCheckpointDir("d:/CheckPoint")
    val a = sc.parallelize(1 to 500, 5)
    val rdd1 = a ++ a ++ a ++ a ++ a
    val rdd3: Option[String] = rdd1.getCheckpointFile

    print(rdd3.toList)
    rdd1.checkpoint
    val rdd4: Option[String] = rdd1.getCheckpointFile

    rdd1.collect

    rdd1.getCheckpointFile
  }

  /*
    leftOuterJoin 类似于SQL中的左外关联left outer join，返回结果以前面的RDD为主，关联不上的记录为空
   */
  def test17(sc: SparkContext): Unit = {
    val rdd1 = sc.parallelize(List(("A", 1), ("B", 2), ("C", 3)), 2)
    val rdd2 = sc.parallelize(List(("A", "a"), ("C", "c"), ("D", "d")), 2)
    val rdd3 = rdd1.leftOuterJoin(rdd2)
    rdd3.foreach(println)
  }

  /*
    rightOuterJoin类似于SQL中的有外关联right outer join，返回结果以参数中的RDD为主，关联不上的记录为空
   */
  def test18(sc: SparkContext): Unit = {
    val rdd1 = sc.parallelize(List(("A", 1), ("B", 2), ("C", 3)), 2)
    val rdd2 = sc.parallelize(List(("A", "a"), ("C", "c"), ("D", "d")), 2)
    val rdd3 = rdd1.rightOuterJoin(rdd2)
    rdd3.foreach(println)
  }

  /*
    flullOuterJoin 两个表的join结果，左边在右边中没找到的结果（NULL），右边在左边没找到的结果
   */
  def test19(sc: SparkContext): Unit = {
    val pairRDD1 = sc.parallelize(List(("cat", 2), ("cat", 5), ("book", 4), ("cat", 12)))
    val pairRDD2 = sc.parallelize(List(("cat", 2), ("cup", 5), ("mouse", 4), ("cat", 12)))
    val rdd3 = pairRDD1.fullOuterJoin(pairRDD2)
    rdd3.foreach(println)
  }

  def test20(sc: SparkContext): Unit = {
    val a = sc.parallelize(1 to 10000, 2)
    a.persist(org.apache.spark.storage.StorageLevel.DISK_ONLY)
    val storage = a.getStorageLevel.description
    println(storage)
  }

  /*
    针对K的，返回在主RDD中出现，并且不在otherRDD中出现的元素。
   */
  def test21(sc: SparkContext): Unit = {
    val rdd1 = sc.makeRDD(Array(("A", "1"), ("B", "2"), ("C", "3")), 2)
    val rdd2 = sc.makeRDD(Array(("A", "a"), ("C", "c"), ("D", "d")), 2)
    val rdd3: RDD[(String, String)] = rdd1.subtract(rdd2)
    rdd3.foreach(println)
  }

  /*
  该函数是将RDD中每一个分区中类型为T的元素转换成Array[T]，这样每一个分区就只有一个数组元素
  */
  def test22(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(1 to 10, 3)
    rdd.foreachPartition(row => {
      println(row.toList)
    })
    val count = rdd.partitions.size
    println("partition is " + count)
    val rdd2: RDD[Array[Int]] = rdd.glom()
    rdd2.foreachPartition(row => {
      row.map(one => {
        for (x <- one) {
          println(x.toInt)
        }
      })

    })
  }

  /*
    groupby 按照自己条件进行分组
   */
  def test23(sc: SparkContext): Unit = {
    val rdd1 = sc.parallelize(1 to 9, 3)
    val rdd2: RDD[(String, Iterable[Int])] = rdd1.groupBy(x => {
      if (x % 2 == 0) "event"
      else "odd"
    })
    //返回结果为 Array((even,ArrayBuffer(2, 4, 6, 8)), (odd,ArrayBuffer(1, 3, 5, 7, 9)))
  }

  /*
   keyBy 按照给定的方法 进行分组 groupByKey 对 pair 类型数据 的key 进行分组 性能不好 建议使用aggrentBykey
   */
  def test24(sc: SparkContext): Unit = {
    val rdd1 = sc.parallelize(List("dog", "tiger", "lion", "cat", "spider", "eagle"))
    val rdd2: RDD[(Int, String)] = rdd1.keyBy(_.length)
    val rdd3: RDD[(Int, Iterable[String])] = rdd2.groupByKey()
    rdd3.foreach(row => {
      val key = row._1
      val value: Iterable[String] = row._2
      println("key is " + key + "value :" + value.toList)
    })
  }

  /**
    * id 查询分配给RDD的id号
    *
    * @param sc
    */
  def test25(sc: SparkContext): Unit = {
    val rdd1 = sc.parallelize(1 to 10, 1)
    val id: Int = rdd1.id
    println(id)
  }

  /**
    * 取两个RDD的交集
    *
    * @param sc
    */
  def test26(sc: SparkContext): Unit = {
    val rdd1 = sc.parallelize(1 to 15)
    val rdd2 = sc.parallelize(10 to 30)
    val rdd3: RDD[Int] = rdd1.intersection(rdd2)
    rdd3.foreach(println)
  }

  def test27(sc: SparkContext): Unit = {
    val rdd1 = sc.parallelize(1 to 15)
    sc.setCheckpointDir("d:/checkpoint")
    val is_checkPoint1 = rdd1.isCheckpointed
    println(is_checkPoint1)
    rdd1.collect()
    rdd1.persist()
    val is_checkPoint2 = rdd1.isCheckpointed
    println(is_checkPoint2)
  }

  /*
   对两个key-value的RDD执行内连接
   */
  def test28(sc: SparkContext): Unit = {
    val rdd1 = sc.parallelize(List("dog", "salmon", "rat", "elephant"), 3)
    val rdd2: RDD[(Int, String)] = rdd1.keyBy(_.length)
    val rdd3 = sc.parallelize(List("dog", "cat", "gnu", "rabbit", "turkey", "wolf", "bear", "bee"), 3)
    val rdd4: RDD[(Int, String)] = rdd3.keyBy(_.length)

    val rdd5: RDD[(Int, (String, String))] = rdd2.join(rdd4)
    rdd5.foreach(row => {
      val key = row._1
      val value = row._2
      println("key is " + key + " value is " + value)
    })
  }

  /**
    * keys 函数 从RDD中抽取所有元素的key并生成新的RDD
    *
    * @param sc
    */
  def test29(sc: SparkContext): Unit = {
    val rdd1 = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
    val rdd2 = rdd1.map((x: String) => {
      (x.length, 2)
    })

    val rdd3 = rdd2.keys
    rdd3.foreach(row => {
      println(row)
    })
  }

  /*
     lookup 遍历RDD中所有的key，找到符合输入key的value，并输出scala的seq数据
   */
  def test30(sc: SparkContext): Unit = {
    val rdd1 = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
    val rdd2 = rdd1.map(row => {
      (row.length, row)
    })
    val rdd3: Seq[String] = rdd2.lookup(3)
    for (tmp <- rdd3) {
      println(tmp)
    }
  }

  /*
      map  针对RDD中的每个数据元素执行map函数，并返回处理后的新RDD。
   */
  def test31(sc: SparkContext): Unit = {
    val rdd1: RDD[String] = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3)
    val rdd2: RDD[Int] = rdd1.map(_.length)
    val rdd3: RDD[(String, Int)] = rdd1.zip(rdd2)
  }

  /**
    * 特殊的map，它在每个数据分片上只执行一次。分片上的数据作为数据序列输入到处理函数，输入数据类型是迭代器(Iterarator[T])。处理函数返回另外一个迭代器。最终mapPartitions会调用合并程序将多个数据分片返回的数据合并成一个新的RDD。
    *
    * @param sc
    */
  def test32(sc: SparkContext): Unit = {
    val rdd1: RDD[Int] = sc.parallelize(1 to 9, 3)

    def myfunction: (Iterator[Int]) => Iterator[Int] = {
      (x: Iterator[Int]) => {
        println(x.toList)
        x
      }
    }

    def myfunction1(x: Iterator[Int]): Iterator[Int] = {
      println(x.toList)
      x
    }

    val rdd2: RDD[Int] = rdd1.mapPartitions(myfunction1)
    rdd2.collect()
  }


  def test33(sc: SparkContext): Unit = {
    val rdd1: RDD[Int] = sc.parallelize(1 to 10, 3)
    def myfunc(x: Iterator[Int]): Iterator[Int] = {
      val res = List[Int]()
      while (x.hasNext) {
        val cur = x.next()
        val rand = Random.nextInt(10)
        println(cur + "   " + rand)
        /**
          * List.fill()(x)  第一个参数 产生几个相同的x值
          */
        val chang_cur: List[Int] = List.fill(rand)(cur)
        println(chang_cur)
        res.:::(chang_cur)
      }
      res.toIterator
    }
    val rdd2 = rdd1.mapPartitions(myfunc)
    rdd2.collect()
  }

  /**
    * flatMap
    *
    * @param sc
    */
  def test34(sc: SparkContext): Unit = {
    val rdd1 = sc.parallelize(1 to 10, 3)
    val rdd2 = rdd1.flatMap(List.fill(Random.nextInt(10))(_))
    rdd2.foreach(println)
  }

  /**
    * mapPartitionsWithContext  但是处理函数会带有当前节点的相关信息。
    *
    * @param sc
    */
  def test35(sc: SparkContext): Unit = {
    val rdd1 = sc.parallelize(1 to 9, 3)
    val rdd2 = rdd1.mapPartitionsWithContext((tc: TaskContext, y: Iterator[Int]) => {
      tc.addOnCompleteCallback(() => println("partition :" + tc.partitionId() + " AttemptID :" + tc.taskAttemptId()))
      y.filter(_ % 2 == 0)
    })
    rdd2.collect()

  }

  /**
    * mapValues 是对value 进行函数操作
    *
    * @param sc
    */
  def test36(sc: SparkContext): Unit = {
    val rdd1 = sc.parallelize(List("dog", "tiger", "lion", "cat", "pather", "eagle"))
    val rdd2 = rdd1.map(x => (x.length, x))
    val rdd3 = rdd2.mapValues("xxxx" + _ + "xxxx")
    rdd3.foreach(row => {
      println(" key " + row._1 + " value " + row._2)
    })
    //    rdd3.collect()
  }

  def test37(sc: SparkContext): Unit = {
    val rdd1 = sc.parallelize(List("gnu", "cat", "rat", "dog", "Gnu", "Rat"), 2)
    val rdd2: Array[Partition] = rdd1.partitions
  }

  def test38(sc: SparkContext): Unit = {
    val rdd1 = sc.parallelize(1 to 9, 3)
    val rdd2 = rdd1.pipe("head -n 1")
    rdd2.foreach(println)
  }

  def test39(sc: SparkContext): Unit = {
    val rdd1 = sc.parallelize(1 to 10)
    val rdd2: Array[RDD[Int]] = rdd1.randomSplit(Array(0.6, 0.4))
    val training: RDD[Int] = rdd2(0)
    val test1 = rdd2(1)
    training.foreach(x => println(x))
    println("=====")
    test1.foreach(x => println(x))
  }

  /**
    * reduce 参数为 集合中的每个值 然后进行reduce 操作
    *
    * @param sc
    */
  def test40(sc: SparkContext): Unit = {
    val rdd1 = sc.parallelize(1 to 100, 3)
    val res: Int = rdd1.reduce(_ + _)
    println(res)
  }

  /**
    * 同一key下进行reduce操作
    *
    * @param sc
    */
  def test41(sc: SparkContext): Unit = {
    val rdd1 = sc.parallelize(List("dg", "cat", "ol", "gnu", "ants"), 2)
    val rdd2: RDD[(Int, String)] = rdd1.map(x => (x.length, x))
    val rdd3: RDD[(Int, String)] = rdd2.reduceByKey(_ + _)

    rdd3.foreach(x => {
      println("key is " + x._1 + " value is " + x._2)
    })
  }

  def test42(sc: SparkContext): Unit = {
    val a = sc.parallelize(1 to 1000, 3)
    val rdd1 = a.sample(false, 0.1, 0)
    println(rdd1.count)
    val rdd2 = sc.parallelize(1 to 1000, 3)
    val rdd3 = rdd2.sample(true, 0.1, 0)
    println(rdd3.count)
  }

  /*
    repartition
   */
  def test43(sc: SparkContext): Unit = {
    val rdd1 = sc.parallelize(List(1, 3, 10, 4, 5, 2, 1, 1, 1), 3)
    val res = rdd1.partitions.length
    println("partition's size " + res)
    val rdd2 = rdd1.repartition(5)
    val res1 = rdd2.partitions.length
    println("partition's size " + res1)
  }


  /**
    * repartitionAndSortWithinPartitions  按照指定的分区分区 并且按照 key  进行排序
    * partitioner对RDD进行分区，并且在每个结果分区中按key进行排序；通过对比sortByKey发现，这种方式比先分区，然后在每个分区中进行排序效率高，这是因为它可以将排序融入到shuffle阶段
    */
  def test44(sc: SparkContext): Unit = {
    val rdd1 = sc.parallelize(List((2, "cat"), (6, "mouse"), (7, "cup"), (3, "cup"), (4, "tv"), (1, "screen"), (5, "heater")), 3)
    val myPartition = new RangePartitioner(3, rdd1)
    val rdd2 = rdd1.partitionBy(myPartition) //重新分区
    val rdd3 = rdd2.mapPartitionsWithIndex((x: Int, y: Iterator[(Int, String)]) => {
        println("part ID " + x + " value is " + y.toList)
        y
      })
    rdd3.collect()

    val rdd4: RDD[(Int, String)] = rdd1.repartitionAndSortWithinPartitions(myPartition)
    val rdd5 = rdd4.mapPartitionsWithIndex((x: Int, y: Iterator[(Int, String)]) => {
      println("part ID " + x + " value is " + y.toList)
      y
    })
    rdd5.collect()
  }

  /**
    * sortByKey 按照key 进行排序
    *
    * @param sc
    */
  def test45(sc: SparkContext): Unit = {
    val rdd1 = sc.parallelize(List(1, 3, 5, 20, 10, 12, 7, 9, 10), 3)
    val rdd2 = rdd1.map(x => (x * 2, x))
    val rdd3 = rdd2.sortByKey(true)

    rdd3.foreach(x => {
      println("key is " + x._1 + " value " + x._2)
    })
  }

  /**
    * 将RDD存入二进制文件
    *
    * @param sc
    */
  def test46(sc: SparkContext): Unit = {
    val rdd1 = sc.parallelize(1 to 1000, 3)
    rdd1.saveAsObjectFile("objectFile")
  }

  /**
    * 保存RDD为一个Hadoop的sequence文件
    *
    * @param sc
    */
  def test48(sc: SparkContext): Unit = {
    val rdd1 = sc.parallelize(Array(("owl", 3), ("gun", 4), ("dog", 1)))
    rdd1.saveAsSequenceFile("hd_seq_file")
  }

  /**
    * 保存RDD为文本文件。一次一行
    *
    * saveAsTextFile 第二个参数是指定保存的格式
    *
    * @param sc
    */
  def test49(sc: SparkContext): Unit = {
    val rdd1 = sc.parallelize(1 to 1000, 3)
    rdd1.saveAsTextFile("textfile")
    //保存的文件是*.gz 格式
    rdd1.saveAsTextFile("compressTextFile", classOf[GzipCodec])
    //保持到hdfs
    //    x.saveAsTextFile("hdfs://localhost:8020/user/cloudera/test")
  }

  /**
    * 同时计算均值，方差，以及标准差。
    *
    * @param sc
    */
  def test50(sc: SparkContext): Unit = {
    val rdd1 = sc.parallelize(List(1.0, 2.0, 3.0, 5.0, 20.0, 19.02, 11.09, 21.0), 2)
    val rdd2: StatCounter = rdd1.stats()

  }

  def test51(sc: SparkContext): Unit = {
    val rdd1 = sc.parallelize(Array(5, 7, 1, 3, 2, 1))
    val rdd2 = rdd1.sortBy(x => x, true) // 从小到大
    //    rdd2.foreach(println)
    val rdd3 = rdd1.sortBy(x => x, false) // 从大到小
    //    rdd3.foreach(println)

    val rdd4 = sc.parallelize(Array(("H", 10), ("A", 26), ("Z", 1), ("L", 5)))
    val rdd5 = rdd4.sortBy(x => x._1, true)
    rdd5.foreach(println)

    val rdd6 = rdd4.sortBy(x => x._2, true)
    rdd6.foreach(println)
  }

  /**
    * take 将RDD中前n个数据单元抽取出来作为array返回
    *
    * @param sc
    */
  def test52(sc: SparkContext): Unit = {
    val rdd1 = sc.parallelize(List("dog", "cat", "ape", "samlon", "gnu"), 2)
    val rdd2: Array[String] = rdd1.take(2)
  }

  /**
    * takeOrdered 用RDD数据单元本身隐含的排序方法进行排序，排序后返回前n个元素组成的array。
    *
    * @param sc
    */
  def test53(sc: SparkContext): Unit = {
    val rdd1 = sc.parallelize(List("dog", "cat", "ape", "samon", "gun"), 2)
    val rdd2 = rdd1.takeOrdered(3)
  }

  def test54(sc: SparkContext): Unit = {
    val rdd1 = sc.parallelize(1 to 9, 3)
    val rdd2 = sc.parallelize(1 to 3, 3)
    val rdd3 = rdd1.subtract(rdd2)
    val rdd4: String = rdd2.toDebugString
    println(rdd4)
  }

  /**
    * union 执行集合合并操作
    *
    * @param sc
    */
  def test55(sc: SparkContext): Unit = {
    val rdd1 = sc.parallelize(1 to 3, 1)
    val rdd2 = sc.parallelize(5 to 7, 1)
    val rdd3: RDD[Int] = rdd1.union(rdd2)
    rdd3.foreach(println)
  }

  def test56(sc: SparkContext): Unit = {
    val rdd1 = sc.parallelize(1 to 100, 3)
    val rdd2 = sc.parallelize(101 to 200, 3)
    val rdd3 = sc.parallelize(201 to 300, 3)
    val rdd4: RDD[((Int, Int), Int)] = rdd1.zip(rdd2).zip(rdd3)
    val rdd5: RDD[(Int, Int, Int)] = rdd4.map(x => (x._1._1, x._1._2, x._2))
    rdd5.foreach(println)
  }

  /** *
    * zipParititions 功能与zip相近，但是可以对zip过程提供更多的控制
    *
    * @param sc
    */
  def test57(sc: SparkContext): Unit = {
    val rdd1 = sc.parallelize(0 to 9, 3);
    val rdd2 = sc.parallelize(10 to 19, 3)
    val rdd3 = sc.parallelize(100 to 109, 3)
    def myfunc(one: Iterator[Int], two: Iterator[Int], three: Iterator[Int]): Iterator[String] =
    {
      var res = List[String]()
      while (one.hasNext && two.hasNext && three.hasNext) {
        val x=one.next()+" "+two.next()+" "+three.next()
        res ::=x
      }
      res.iterator
    }
    val rdd4: RDD[String] = rdd1.zipPartitions(rdd2, rdd3)(myfunc)
    rdd4.foreach(println)
  }

  /**
    * zipWith Index
    * @param sc
    */
  def  test58(sc:SparkContext): Unit ={
    val rdd1=sc.parallelize(Array("a","b","c","d"))
    val rdd2: RDD[(String, Long)] = rdd1.zipWithIndex()
    val rdd3=sc.parallelize(100 to 120 ,5)
    val rdd4=rdd3.zipWithIndex()

  }
  def test59(sc:SparkContext): Unit ={

  }
}

object SparkTest {
  def main(args: Array[String]): Unit = {
    val tt = new SparkTest()
    val sparkConf = new SparkConf().setAppName("SparkLocal").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)

//    tt.test57(sparkContext)
    tt.test6(sparkContext)
  }
}
