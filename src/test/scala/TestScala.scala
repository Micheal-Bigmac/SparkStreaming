import java.util.Properties

/**
  * Created by PC on 2016/12/28.
  */
object TestScala {
  def main(args: Array[String]) {
    val a  ="nihao hello "
    val b:String=null ;
    val properties: Properties = System.getProperties
    println(properties.get("java.library.path"))
//    println(b.toLong)
//    print(a)
  }
}
