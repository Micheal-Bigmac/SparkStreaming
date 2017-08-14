package tool

/**
  * Created by admin on 2017/8/7.
  */

import redis.clients.jedis.JedisPool
import org.apache.commons.pool2.impl.GenericObjectPoolConfig

object RedisClient extends Serializable {
  val redisHost = "10.10.13.251"
  val redisPort = 6389
  val redisTimeout = 30000
  lazy val pool = new JedisPool(new GenericObjectPoolConfig(), redisHost, redisPort, redisTimeout)

  @transient  val poo2 = new JedisPool(new GenericObjectPoolConfig(), redisHost, redisPort, redisTimeout)
  lazy val hook = new Thread {
    override def run = {
      println("Execute hook thread: " + this)
      pool.destroy()
    }
  }
  sys.addShutdownHook(hook.run)
}