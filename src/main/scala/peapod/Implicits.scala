package peapod

import com.typesafe.config.{Config, ConfigValueFactory}

import scala.reflect.ClassTag

/**
  * Created by marcin.mejran on 4/24/16.
  */
object Implicits {
  implicit class configWithSet(conf: Config) {
    def set(name: String, value: Any) = {
      conf.withValue(name, ConfigValueFactory.fromAnyRef(value))
    }
  }
}
