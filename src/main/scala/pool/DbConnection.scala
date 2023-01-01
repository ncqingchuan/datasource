package pool

import java.sql.Connection
import java.sql.DriverAction
import java.sql.DriverManager

class DbConnection(
    val url: String,
    val user: String,
    val password: String
) extends AutoCloseable {

  private[pool] var used: Boolean = false
  private[pool] val innerConnection: Connection = DriverManager.getConnection(url, user, password)

  def close(): Unit = {
    if (used) {
      used = false
    }
  }

  def BeginTransaction(isolationLevel: Int = IsolationLevel.TRANSACTION_READ_COMMITTED): DbTransaction = {
    if (innerConnection.getAutoCommit()) {
      innerConnection.setAutoCommit(false)
    }
    innerConnection.setTransactionIsolation(isolationLevel)
    new DbTransaction(this)
  }

  def CreateCommand(): DbCommand = {
    new DbCommand(this)
  }
}
