package pool

object Dispose {
  def using[T <: AutoCloseable](cls: T)(op: T => Unit): Unit = {
    try {
      op(cls)
    } catch {
      case e: Exception => throw e
    } finally {
      cls.close()
    }
  }
}
