package peapod

import generic.PeapodGenerator

class StorableTaskLocalTest extends StorableTaskTest{
  override def generatePeapod(): Peapod = PeapodGenerator.peapod()
  override def generatePeapodNonRecursive(): Peapod = PeapodGenerator.peapodNonRecursive()
}
