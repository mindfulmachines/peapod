package peapod

object DotFormatter {
  def format(links: List[(Task[_], Task[_])]): String = {
    val leftNodes = links.map(_._1).toSet
    val rightNodes = links.map(_._2).toSet
    val nodes = leftNodes.union(rightNodes)
    "digraph G {" +
      "node [shape=box]" +
      nodes.filter(_.exists())
        .map("\"" + _.name + "\" [style=filled];").mkString("\n")+
      nodes.filter(_.isInstanceOf[EphemeralTask[_]])
        .map("\"" + _.name + "\" [style=dotted];").mkString("\n")+
      links.map(
        kv => "\"" + kv._2.name + "\"->\"" + kv._1.name + "\";"
      )
        .mkString("\n") +
      "{ rank=same;" + links.filter(kv => ! rightNodes.contains(kv._1)).map("\"" + _._1.name + "\"").mkString(" ") + "}" +
      "{ rank=same;" + links.filter(kv => ! leftNodes.contains(kv._2)).map("\"" + _._2.name + "\"").mkString(" ") + "}" +
      "}"
  }
}
