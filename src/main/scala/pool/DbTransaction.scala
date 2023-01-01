package pool

class DbTransaction(private val connection: DbConnection) {

  def Commit(): Unit = {
    connection.innerConnection.commit()
    if (!connection.innerConnection.getAutoCommit()) {
      connection.innerConnection.setAutoCommit(true);
    }
  }
  def RollBack(): Unit = {
    connection.innerConnection.rollback()
    if (!connection.innerConnection.getAutoCommit()) {
      connection.innerConnection.setAutoCommit(true)
    }
  }

  def getConnection(): DbConnection = {
    connection
  }

  def getTransactionIsolation() {
    connection.innerConnection.getTransactionIsolation()
  }
  
}

object IsolationLevel {

  val TRANSACTION_NONE = 0

  val TRANSACTION_READ_UNCOMMITTED = 1;

  val TRANSACTION_READ_COMMITTED = 2;

  val TRANSACTION_REPEATABLE_READ = 4;

  val TRANSACTION_SERIALIZABLE = 8;

}
