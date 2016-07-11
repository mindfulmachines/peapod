package peapod

import generic.PeapodGeneratorS3

class PeapodWorkflowS3Test extends PeapodWorkflowTest {
  def generatePeapod() = {PeapodGeneratorS3.peapod()}

}
