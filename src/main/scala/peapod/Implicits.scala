package peapod

import com.typesafe.config.{Config, ConfigValueFactory}

/**
  * Helpful implicits that makes life easier.
  */
object Implicits {
  implicit class configWithSet(conf: Config) {
    def set(name: String, value: Any) = {
      conf.withValue(name, ConfigValueFactory.fromAnyRef(value))
    }
  }
}
