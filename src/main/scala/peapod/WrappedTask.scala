package peapod

import scala.reflect.ClassTag

/**
  * A Task that can be automatically cached in a Peapod object, used internally for dependencies
  */
private class WrappedTask[+T: ClassTag] (val p: Peapod, val t: Task[T]) {
  def get() : T = {
    p(t).get()
  }
  def apply() : T = {
    get()
  }
}
