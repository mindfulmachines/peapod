package peapod
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import org.eclipse.jetty.server.{Request, Server}
import org.eclipse.jetty.server.handler.AbstractHandler

import scala.concurrent.Future

/**
  * Experimental and minimal web server for showing the DAG graph of a Peapod instance. To use extend the Peapod
  * instance with this trait and go to "localhost:8080" to see the graph. This uses an external service for generating
  * the graph itself so the web browser needs web access for this to function. In the future this will be converted to
  * use D3 to generate the graph rather than the Graphiz based implementation that exists currently.
  */
trait Web{
  self: Peapod =>
  import scala.concurrent.ExecutionContext.Implicits.global
  val server = Future(new WebServer(this))
}

class WebServer(p: Peapod, port: Int= 8080) {
  val server = new Server(port)
  server.setHandler(new WebHandler(p))
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
      <img src={Util.mindfulmachinesDotLink(p.dotFormatDiagram())}></img>
    )
  }
}