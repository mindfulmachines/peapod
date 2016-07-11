package peapod

import com.typesafe.config.ConfigFactory
import generic.PeapodGeneratorS3
import Implicits._

class StorableTaskReadThroughS3Test extends StorableTaskTest {
  override def generatePeapod(): Peapod = PeapodGeneratorS3.peapod(ConfigFactory.load().set("peapod.storable.readthrough",true))
  override def generatePeapodNonRecursive(): Peapod = PeapodGeneratorS3.peapodNonRecursive(ConfigFactory.load().set("peapod.storable.readthrough",true))
}
