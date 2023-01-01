import pool.DataSource
import pool.DbCommand
import pool.DbParameter
import pool.DbTransaction
import pool.Dispose.using
import pool.IsolationLevel
import pool.ParameterDirection

import java.sql.Date
import java.sql.ResultSet
import java.sql.Types
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import javax.xml.crypto.Data
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Buffer
import scala.language.experimental.macros
import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}
object App {

  def main(args: Array[String]): Unit = {

    val pool = new DataSource(
      "com.microsoft.sqlserver.jdbc.SQLServerDriver",
      "jdbc:sqlserver://localhost:1433;databaseName=HighwaveDW;trustServerCertificate=true",
      "sa",
      "",
      minSize = 1,
      maxSize = 3,
      keepAliveTimeout = 1000
    )
    for (i <- 1 to 15) {
      val thread: Thread = new Thread(() => {
        using(pool.getConenction()) { con =>
          {
            using(new DbCommand(con)) { cmd =>
              {
                cmd.commandText = "select BrandKey,BrandName from dimbrand where brandkey=?"
                cmd.Parameters.append(new DbParameter("@id", i))
                cmd.ExecuteResultSet[data]().foreach(t => println(s"Thread:${Thread.currentThread().getName()},BrandName=${t.BrandName}"))
              }
            }
          }
        }
      })
      thread.start()
      Thread.sleep(100)
    }
  }
}
