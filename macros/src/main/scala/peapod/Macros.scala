package peapod

import scala.annotation.{StaticAnnotation, compileTimeOnly}
import scala.language.experimental.macros
import scala.reflect.macros.whitebox

/**
  * Created by lrohde on 5/20/16.
  */
object VersionMacro {


  def codeVersionImpl(c: whitebox.Context)(annottees: c.Expr[Any]*): c.Tree = {
    import c.universe._

    annottees.map(_.tree).toList match {
      case (classDec: ClassDef) :: Nil =>
        val versionVal = q"""
           override val version = ${classDec.hashCode().toString}
          """
        classDec match {
          case q"$mods class $tpname[..$tparams] $ctorMods(...$paramss) extends { ..$earlydefns } with ..$parents { $self => ..$stats }" =>
            q"""$mods class $tpname[..$tparams] $ctorMods(...$paramss) extends { ..$earlydefns } with ..$parents { $self =>
               $versionVal
               ..$stats }"""
        }
      case _ => c.abort(c.enclosingPosition, "Invalid annotation usage, but be on a class definition")
    }

  }
}
@compileTimeOnly("enable macro paradise to expand macro annotations")
class GenVersion extends StaticAnnotation {
  def macroTransform(annottees: Any*): Any = macro VersionMacro.codeVersionImpl
}

trait Versioned {
  val version: String = ""
}