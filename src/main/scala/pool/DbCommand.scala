package pool

import java.sql.CallableStatement
import java.sql.ResultSet
import java.sql.SQLType
import java.sql.Statement
import java.sql.Types
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Buffer
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}

import Dispose.using

class DbCommand(val connection: DbConnection, var commandText: String = null, val queryTimeout: Integer = 30) extends AutoCloseable {

  val Parameters: Buffer[DbParameter] = new ArrayBuffer[DbParameter]()
  private val mirror = ru.runtimeMirror(getClass().getClassLoader())
  private var statement: CallableStatement = null
  if (queryTimeout < 0) {
    throw new IllegalArgumentException(s"timeout (${queryTimeout}) value is greater than 0.")
  }

  /** @author:qingchuan
    *
    * @return
    */
  def ExecuteScalar(): Any = {
    var obj: Any = None
    ExecuteResultSet(t => {
      if (t.next()) {
        if (t.getMetaData().getColumnCount() > 0)
          obj = t.getObject(1)
      }
    })
    obj
  }

  /** @author
    *   qingchuan
    * @version 1.0
    *
    * @param callBack
    */
  def ExecuteResultSet(callBack: ResultSet => Unit): Unit = {
    if (callBack == null) throw new IllegalArgumentException("The value of parameter callback is null.")
    statement = connection.innerConnection.prepareCall(commandText)
    statement.setQueryTimeout(queryTimeout)
    addParatemetrs()
    using(statement.executeQuery()) { t =>
      callBack(t)
      if (!t.isClosed())
        getOutParameterValue()
    }
  }

  def ExecuteResultSet[T: ru.TypeTag](): ArrayBuffer[T] = {
    val classSymbol = mirror.symbolOf[T].asClass
    val classMirror = mirror.reflectClass(classSymbol)
    val consMethodMirror = classMirror.reflectConstructor(classSymbol.primaryConstructor.asMethod)
    val fields = ru.typeOf[T].decls.filter(t => t.asTerm.isGetter && t.isPublic).map(t => t.asTerm)
    val result = new ArrayBuffer[T]()
    ExecuteResultSet(t => {
      while (t.next()) {
        var i = 1
        val values: Buffer[Any] = ArrayBuffer()
        for (f <- fields) {
          values += t.getObject(i)
          i += 1
        }
        result += consMethodMirror.apply(values: _*).asInstanceOf[T]
      }
    });
    result
  }

  def ExecuteBatch[T: ru.TypeTag: ClassTag](values: List[T]): Int = {
    statement = connection.innerConnection.prepareCall(commandText)
    var trans: DbTransaction = null
    val fields = ru.typeOf[T].decls.filter(t => t.asTerm.isGetter && t.isPublic).map(t => t.asTerm)
    for (t <- values) {
      var i = 1
      val filedMirror = mirror.reflect(t)
      for (f <- fields) {
        val instance = filedMirror.reflectField(f)
        statement.setObject(i, instance.get)
        i += 1
      }
      statement.addBatch()
    }

    try {
      trans = connection.BeginTransaction()
      val obj = statement.executeBatch()
      trans.Commit()
      statement.clearBatch()
      obj.sum
    } catch {
      case e: Exception => {
        trans.RollBack()
        throw e
      }
    }
  }

  def ExecuteNoneQuery(): Integer = {
    statement = connection.innerConnection.prepareCall(commandText)
    statement.setQueryTimeout(queryTimeout)
    addParatemetrs()
    val obj = statement.executeUpdate()
    getOutParameterValue()
    obj
  }

  def CreateParameter(): DbParameter = {
    new DbParameter();
  }

  private def getOutParameterValue(): Unit = {
    for (i <- 1 to Parameters.size) {
      val parameter: DbParameter = Parameters(i - 1);
      if (parameter.parameterDirection == ParameterDirection.Output || parameter.parameterDirection == ParameterDirection.InputOutput) {
        parameter.value = statement.getObject(i);
      }
    }
  }

  private def addParatemetrs(): Unit = {
    statement.clearParameters()
    for (i <- 1 to Parameters.size) {
      val p = Parameters(i - 1);
      if (p.parameterDirection == ParameterDirection.Input || p.parameterDirection == ParameterDirection.InputOutput) {
        statement.setObject(i, p.value)
      }
      if (p.parameterDirection == ParameterDirection.Output || p.parameterDirection == ParameterDirection.InputOutput) {
        statement.registerOutParameter(p.parameterName, p.sqlType, p.scale)
      }
    }
  }
  def close() {
    if (statement != null) {
      statement.close()
    }
  }
}

case class DbParameter(
    var parameterName: String = null,
    var value: Any = null,
    var parameterDirection: Integer = ParameterDirection.Input,
    var scale: Integer = 0,
    var sqlType: Integer = null
) {}

object ParameterDirection {
  val Input = 1
  val InputOutput = 2
  val Output = 3
}
