package peapod

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import org.eclipse.jetty.server.{Handler, Request, Server}
import org.eclipse.jetty.server.handler.{AbstractHandler, ContextHandler, ContextHandlerCollection, ResourceHandler}

import scala.concurrent.Future

/**
  * Experimental and minimal web server for showing the DAG graph of a Peapod instance. To use extend the Peapod
  * instance with this trait and go to "localhost:8080" to see the graph. This uses an external service for generating
  * the graph itself so the web browser needs web access for this to function. In the future this will be converted to
  * use D3 to generate the graph rather than the Graphiz based implementation that exists currently.
  */
trait Web {
  self: Peapod =>

  import scala.concurrent.ExecutionContext.Implicits.global

  val server = Future(new WebServer(this))

  def stop() = {
    server.map(_.server.stop())
  }
}

class WebServer(p: Peapod, port: Int = 8080) {
  val server = new Server(port)

  val resource_handler = new ResourceHandler()
  val url = this.getClass.getClassLoader.getResource("web")
  resource_handler.setResourceBase(url.toString)

  val contextWeb = new ContextHandler("/")
  contextWeb.setHandler(resource_handler)


  val context = new ContextHandler("/graph")
  context.setHandler(new WebHandler(p))

  val contexts = new ContextHandlerCollection()
  contexts.setHandlers(Array(contextWeb, context))

  server.setHandler(contexts)
  server.start()
  server.join()
}

class WebHandler(p: Peapod) extends AbstractHandler {
  override def handle(s: String, request: Request,
                      httpServletRequest: HttpServletRequest,
                      httpServletResponse: HttpServletResponse): Unit = {
    httpServletResponse.setContentType("text/html;charset=utf-8")
    httpServletResponse.setStatus(HttpServletResponse.SC_OK)
    request.setHandled(true)

    httpServletResponse.getWriter.println(
      if(httpServletRequest.getQueryString != null && request.getQueryString == "active") {
        GraphFormatter.json(p.activeTasks())
      } else {
        GraphFormatter.json(p.allTasks())
      }
    )
  }
}