import javax.inject.{Inject, Provider, Singleton}

import com.mindscapehq.raygun4java.play2.RaygunPlayClient
import play.api.http.DefaultHttpErrorHandler
import play.api._
import play.api.mvc._
import play.api.mvc.Results._
import play.api.routing.Router

import scala.collection.JavaConverters._
import scala.concurrent._
import scala.concurrent._
import scala.util.Try

/**
  * Sends exceptions to raygun
  */
@Singleton
class ErrorHandler @Inject()(
    env: Environment,
    config: Configuration,
    sourceMapper: OptionalSourceMapper,
    router: Provider[Router]
) extends DefaultHttpErrorHandler(env, config, sourceMapper, router) {
  val ApiKey: String = "qVMngezq6tGAqOrgsmnFwg=="

  override protected def onProdServerError(
      request: RequestHeader,
      exception: UsefulException): Future[Result] = {
    Try {
      val rg = new RaygunPlayClient(ApiKey, request)
      rg.Send(exception, Seq("sensors", "production", "play").asJava)
    }
    super.onProdServerError(request, exception)
  }
}
