package peapod

/**
  *
  */
trait Param extends Task[Any] {
  override lazy val baseName = this.getClass.getName + "_" + params.map(_.toString.hashCode).mkString("_")
  var params = List[Any]()
  def param(v: Any) = {
    params = v :: params
  }
}
