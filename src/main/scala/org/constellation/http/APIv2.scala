package org.constellation.http

import cats.effect.IO
import org.http4s.rho.RhoService
import org.http4s.rho.bits._
import org.http4s.rho.swagger.{SwaggerFileResponse, SwaggerSyntax}
import org.http4s.{EntityDecoder, Headers, HttpDate, HttpRoutes, HttpService, Request, Uri, headers}
import org.http4s.dsl.Http4sDsl

class APIv2 {
  val dsl = new Http4sDsl[IO]{}
  import dsl._

  val service = new RhoService[IO] {

    case GET / "hello" / 'world +? param[Int]("fav") |>> {
      (world: String, fav: Int) => Ok(s"Recei ed $fav, $world")
    }

  }
}
