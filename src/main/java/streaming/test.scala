package streaming

import kafka.utils.Json

/**
  * Created by PC on 2017/1/4.
  */
object test {

  def main(args: Array[String]) {
    val test="{\"jmx_port\":9393,\"timestamp\":\"1481509664681\",\"endpoints\":[\"PLAINTEXT://data1.cvnavi-test.com:9092\"],\"host\":\"data1.cvnavi-test.com\",\"version\":2,\"port\":9092}"
//    val test2: Map[String, String] =test.asInstanceOf[Map[String,String]]
    val test2=Json.parseFull(test)
//    println(test2.size)

//    val _100001: SpoutStreamTypeId = SpoutStreamTypeId._100001
//    val StreamId=new SpoutStreamTypeId(_100001)
    val _100001: String = Transform._100001
    //    println(StreamId)
    println(_100001)
  /*  val tt=Map[String,String]()
    tt.+("1"->"1")
    tt +("2"->"2")
    println(tt.size)

    val tt1=collection.mutable.Map[String,String]()
    tt1 +=("1"->"1")
    tt1 +=("2"->"2")
    tt1.put("3","3")
    println(tt1.size)*/
//    println(manOf(test))
  }
  def manOf[T: Manifest](t: T): Manifest[T] = manifest[T]
}
