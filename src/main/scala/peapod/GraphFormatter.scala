package peapod

import java.io.StringWriter

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.json4s.jackson.Json

case class Node(name: String,
                ephemeral: Boolean,
                exists: Boolean
                )

case class Edge(nodeA: String,
                nodeB: String
                )

case class Graph(
                nodes: List[Node],
                edges: List[Edge]
                )


/**
  * Provides utilities for formatting a DAG of Tasks into a DOT format graph.
  */
object GraphFormatter {
  def dot(links: List[(Task[_], Task[_])]): String = {
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
        kv => "\"" + kv._1 + "\"->\"" + kv._2 + "\";"
      )
        .mkString("\n") +
      "{ rank=same;" + links.filter(kv => ! rightNodes.contains(kv._1)).map("\"" + _._1 + "\"").mkString(" ") + "}" +
      "{ rank=same;" + links.filter(kv => ! leftNodes.contains(kv._2)).map("\"" + _._2 + "\"").mkString(" ") + "}" +
      "}"
  }

  def json(links: List[(Task[_], Task[_])]): String = {
    val leftNodes = links.map(_._1).toSet
    val rightNodes = links.map(_._2).toSet
    val nodes = leftNodes.union(rightNodes).map(
      n => new Node(n.toString,! n.storable, n.exists())
    ).toList

    val edges = links.map{case (l,r) => new Edge(l.toString, r.toString)}

    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val out = new StringWriter
    mapper.writeValue(out, new Graph(nodes,edges))
    out.toString
  }
}
