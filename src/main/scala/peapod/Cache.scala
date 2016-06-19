package peapod

/**
  *
  */
sealed trait Cache
case object Always extends Cache
case object Never extends Cache
case object Auto extends Cache