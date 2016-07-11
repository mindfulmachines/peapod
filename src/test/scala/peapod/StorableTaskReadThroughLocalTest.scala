package peapod

import generic.PeapodGenerator
import Implicits._
import com.typesafe.config.ConfigFactory

class StorableTaskReadThroughLocalTest extends StorableTaskTest{
  override def generatePeapod(): Peapod = PeapodGenerator.peapod(ConfigFactory.load().set("peapod.storable.readthrough",true))
  override def generatePeapodNonRecursive(): Peapod = PeapodGenerator.peapodNonRecursive(ConfigFactory.load().set("peapod.storable.readthrough",true))

}
