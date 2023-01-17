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
import com.nimbusds.oauth2.sdk.util.date.SimpleDate
import java.text.SimpleDateFormat
object App {
  def main(args: Array[String]): Unit = {
    val pool = new DataSource(
      "com.microsoft.sqlserver.jdbc.SQLServerDriver",
      "jdbc:sqlserver://localhost:1433;databaseName=HighwaveDW;trustServerCertificate=true",
      "sa",
      "",
      minSize = 1,
      maxSize = 3,
      keepAliveTimeout = 3000
    )

    val formatter: SimpleDateFormat = new SimpleDateFormat("HH:mm:ss.SSS");
    for (i <- 1 to 15) {
      val thread: Thread = new Thread(() => {
        val date = new Date(System.currentTimeMillis())
        using(pool.getConenction()) { con =>
          {
            using(new DbCommand(con)) { cmd =>
              {
                // cmd.commandText = "Select BrandKey,BrandName From dimbrand where brandkey=?"
                cmd.commandText = "exec p_get_out ?,?"
                val p1 = new DbParameter("@id", i)
                val p2 = new DbParameter(parameterName = "@msg", parameterDirection = ParameterDirection.Output, sqlType = Types.VARCHAR)
                cmd.Parameters.append(p1)
                cmd.Parameters.append(p2)
                // cmd.ExecuteResultSet[data]().foreach(t => println(s"Thread:${Thread.currentThread().getName()},BrandName=${t.BrandName},time=${formatter.format(date)}"))
                val result = cmd.ExecuteScalar()
                println(s"value=${result},output=${p2.value},parameter=${i}")
              }
            }
          }
        }
      })
      thread.start()
    }
  }
}
