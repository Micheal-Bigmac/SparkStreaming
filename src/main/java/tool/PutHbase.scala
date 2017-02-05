package tool

import java.util
import java.util.Properties

import com.sxp.hbase.common.HBaseClient
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Mutation
import org.slf4j.LoggerFactory

/**
  * Created by PC on 2017/1/12.
  */
class PutHbase(pro: Properties) {
  @transient
  protected var hbaseClient: HBaseClient = null
  val logger = LoggerFactory.getLogger(this.getClass)
  val configuration = HBaseConfiguration.create()
  hbaseClient = new HBaseClient(new util.HashMap[String, Object](), configuration, pro.getProperty("hbase.table.name.history"))

/*  def prepare(pro: Properties): Unit = {
    val configuration = HBaseConfiguration.create()
    hbaseClient = new HBaseClient(new util.HashMap[String, Object](), configuration, pro.getProperty("hbase.table.name.history"))
  }*/

  def execute(rows: List[Mutation]): Unit = {
    import collection.JavaConversions._
    hbaseClient.batchMutate(rows)
  }
}
