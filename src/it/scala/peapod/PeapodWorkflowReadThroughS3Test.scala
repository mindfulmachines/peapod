package peapod

import generic.PeapodGeneratorS3
import Implicits._
import com.typesafe.config.ConfigFactory

class PeapodWorkflowReadThroughS3Test extends PeapodWorkflowTest {
  def generatePeapod() = {PeapodGeneratorS3.peapod( ConfigFactory.load().set("peapod.storable.readthrough",true))}

}
