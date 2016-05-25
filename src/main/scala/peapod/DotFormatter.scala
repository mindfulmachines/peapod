package peapod

/**
  *
  */
object DotFormatter {
  def format(links: List[(Task[_], Task[_])]): String = {
    val leftNodes = links.map(_._1).toSet
    val rightNodes = links.map(_._2).toSet
    val nodes = leftNodes.union(rightNodes)
    "digraph G {" +
      "node [shape=box]" +
      nodes.filter(_.exists())
        .map("\"" + _ + "\" [style=filled];").mkString("\n")+
      nodes.filter(! _.storable)
        .map("\"" + _ + "\" [style=dotted];").mkString("\n")+
      links.map(
        kv => "\"" + kv._2 + "\"->\"" + kv._1 + "\";"
      )
        .mkString("\n") +
      "{ rank=same;" + links.filter(kv => ! rightNodes.contains(kv._1)).map("\"" + _._1 + "\"").mkString(" ") + "}" +
      "{ rank=same;" + links.filter(kv => ! leftNodes.contains(kv._2)).map("\"" + _._2 + "\"").mkString(" ") + "}" +
      "}"
  }
}
