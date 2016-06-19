package peapod

import com.typesafe.config.{Config, ConfigValueFactory}

import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
  * Helpful implicits that makes life easier.
  */
object Implicits {
  implicit def getAnyTask[T: ClassTag](task: WrappedTask[T]): T =
    task.get()

  implicit def getAnyTask[T: ClassTag](pea: Pea[T]): T =
    pea.get()

  implicit class configWithSet(conf: Config) {
    def set(name: String, value: Any) = {
      conf.withValue(name, ConfigValueFactory.fromAnyRef(value))
    }
  }
}
