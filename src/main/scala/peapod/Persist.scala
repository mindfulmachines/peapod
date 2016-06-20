package peapod

/**
  *
  */
sealed trait Persist
case object Always extends Persist
case object Never extends Persist
case object Auto extends Persist