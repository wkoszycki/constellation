package org.constellation.util

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, PredefinedFromEntityUnmarshallers, Unmarshal}
import akka.stream.ActorMaterializer
import org.constellation.primitives.Schema.Id
import org.json4s.native.Serialization
import org.json4s.{Formats, native}

import scala.concurrent.{ExecutionContextExecutor, Future}

// TODO : Implement all methods from RPCInterface here for a client SDK
// This should also probably use scalaj http because it's bettermore standard.

class APIClient(val host: String = "127.0.0.1", val port: Int)(
  implicit val system: ActorSystem,
  implicit val materialize: ActorMaterializer,
  implicit val executionContext: ExecutionContextExecutor
) {

  import constellation.EasyFutureBlock

  var udpPort: Int = 16180
  var id: Id = null

  def udpAddress: String = host + ":" + udpPort

  def setExternalIP(): Boolean = postSync("ip", host + ":" + udpPort).status == StatusCodes.OK

  val baseURI = s"http://$host:$port"

  def base(suffix: String) = Uri(s"$baseURI/$suffix")

  private val http = Http()

  def get(suffix: String, queryParams: Map[String,String] = Map()): Future[HttpResponse] = {
    http.singleRequest(
      HttpRequest(uri = base(suffix).withQuery(Query(queryParams)))
    )
  }

  def getSync(suffix: String, queryParams: Map[String,String] = Map()): HttpResponse = {
    http.singleRequest(
      HttpRequest(uri = base(suffix).withQuery(Query(queryParams)))
    ).get()
  }

  def addPeer(remote: String): HttpResponse = postSync("peer", remote)

  def getBlocking[T](suffix: String, queryParams: Map[String,String] = Map(), timeout: Int = 5)
                              (implicit m : Manifest[T], f : Formats = constellation.constellationFormats): T = {

    val req = HttpRequest(uri = base(suffix).withQuery(Query(queryParams)))
    http.singleRequest(req).flatMap {
      httpResponse => Unmarshal(httpResponse.entity).to[String].map { r => Serialization.read[T](r) }
    }.get()
  }

  def getBlockingStr[T](suffix: String, queryParams: Map[String,String] = Map(), timeout: Int = 5)
                                 (implicit m : Manifest[T], f : Formats = constellation.constellationFormats): String = {
    val httpResponse = http.singleRequest(
      HttpRequest(uri = base(suffix).withQuery(Query(queryParams)))
    ).get(timeout)
    Unmarshal(httpResponse.entity).to[String].get()
  }

  def post(suffix: String, t: AnyRef)(implicit f : Formats = constellation.constellationFormats): Future[HttpResponse] = {

    val ser = Serialization.write(t)
    http.singleRequest(
      HttpRequest(uri = base(suffix), method = HttpMethods.POST, entity = HttpEntity(
        ContentTypes.`application/json`, ser)
      ))
  }

  def postSync[T <: AnyRef](suffix: String, t: T)(
    implicit f : Formats = constellation.constellationFormats
  ): HttpResponse = {
    post(suffix, t).get()
  }

  implicit val serialization: Serialization.type = native.Serialization
  implicit val stringUnmarshaller: FromEntityUnmarshaller[String] = PredefinedFromEntityUnmarshallers.stringUnmarshaller

  def read[T <: AnyRef](httpResponse: HttpResponse)(
    implicit m : Manifest[T], f : Formats = constellation.constellationFormats
  ): Future[T] =
    Unmarshal(httpResponse.entity).to[String].map{r => Serialization.read[T](r)}

  def postRead[Q <: AnyRef](suffix: String, t: AnyRef, timeout: Int = 5)(
    implicit m : Manifest[Q], f : Formats = constellation.constellationFormats
  ): Q = {
    import constellation.EasyFutureBlock

    read[Q](post(suffix, t).get(timeout)).get()
  }

}