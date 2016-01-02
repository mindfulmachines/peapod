package dependency

/**
 * Created by marcin.mejran on 1/2/16.
 */
object DotFormatter {
  def format(links: List[(String, String)]): String = {
    val leftNodes = links.map(_._1).toSet
    val rightNodes = links.map(_._2).toSet
    "digraph G {" +
      "node [shape=box];" +
      links.map(
        kv => "\"" + kv._2 + "\"->\"" + kv._1 + "\";"
      )
        .mkString("\n") +
      "{ rank=same;" + links.filter(kv => ! rightNodes.contains(kv._1)).map("\"" + _._1 + "\"").mkString(" ") + "}" +
      "{ rank=same;" + links.filter(kv => ! leftNodes.contains(kv._2)).map("\"" + _._2 + "\"").mkString(" ") + "}" +
      "}"
  }
}
