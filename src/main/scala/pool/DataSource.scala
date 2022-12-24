package pool
import scala.util.control.Breaks._
import scala.collection.mutable.ArrayBuffer
import java.{util => ju}
import scala.collection.mutable.Buffer
import scala.util.control.Breaks

class DataSource(
    val driverName: String,
    val url: String,
    val user: String,
    val password: String,
    val minSize: Integer = 1,
    val maxSize: Integer = 10,
    val keepAliveTimeout: Long = 1000
) extends AutoCloseable {

  if (minSize < 0 || minSize > maxSize || keepAliveTimeout < 0) {
    throw new IllegalArgumentException("These arguments are Illegal")
  }

  Class.forName(driverName)
  private val pool: Buffer[DbConnection] = ArrayBuffer[DbConnection]()
  private val lock: ju.concurrent.locks.Lock = new ju.concurrent.locks.ReentrantLock(true)

  for (i <- 0 until minSize) {
    pool += new DbConnection(url, user, password)
  }

  def getConenction(): DbConnection = {
    val starEntry = System.currentTimeMillis()
    Breaks.breakable {
      while (true) {
        lock.lock()
        try {
          for (con <- pool) {
            if (!con.used) {
              con.used = true
              return con;
            }
          }
          if (pool.size < maxSize) {
            var con = new DbConnection(url, user, password) { used = true }
            pool.append(con)
            return con
          }
        } finally {
          lock.unlock()
        }
        if (System.currentTimeMillis() - starEntry > keepAliveTimeout) {
          break()
        }
      }
    }
    throw new IllegalArgumentException("Connection Pool is empty")
  }
  def close(): Unit = {
    lock.lock()
    try {
      if (pool != null) {
        pool.foreach(t => t.innerConnection.close())
        pool.clear()
      }
    } finally {
      lock.unlock()
    }

  }
}
