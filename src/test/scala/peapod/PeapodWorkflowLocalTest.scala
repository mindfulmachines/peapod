package peapod

import generic.PeapodGenerator

class PeapodWorkflowLocalTest extends PeapodWorkflowTest {
  def generatePeapod() = {PeapodGenerator.peapod()}
}
