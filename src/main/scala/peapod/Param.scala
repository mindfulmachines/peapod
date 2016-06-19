package peapod

/**
  * Provides support for parametrized Tasks, each parameter is created as an instance of Param and then Tasks inherit
  * those classes. This class ensures that the naming of Task's is unique for each combination of parameters.
  * For example:
  *
  *   trait ParamA extends Param {
  *     val a: String
  *     param(a)
  *   }
  *   trait ParamB extends Param {
  *     val b: String
  *     param(b)
  *   }
  *   class Test(val a: String, val b: String)(implicit val p: Peapod)
  *     extends EphemeralTask[Double] with ParamA with ParamB {
  *     def generate = 1
  *   }
  *
  */
trait Param extends Task[Any] {
  override lazy val baseName = this.getClass.getName + "_" + params.map(_.toString.hashCode).mkString("_")
  var params = List[Any]()
  def param(v: Any) = {
    params = v :: params
  }
}
