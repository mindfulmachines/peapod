package peapod

import scala.reflect.ClassTag

/**
  * Created by marcin.mejran on 5/26/16.
  */
class WrappedTask[+T: ClassTag] (val p: Peapod, val t: Task[T]) {
  def get() : T = {
    p(t).get()
  }
  def apply() : T = {
    get()
  }
}
