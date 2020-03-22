package controllers

import javax.inject.Inject
import com.iofficecorp.appinfoclient._
import play.api.mvc._

import scala.concurrent.ExecutionContext.Implicits.global

class Health @Inject()(val appInfo: ApplicationInfoClient) extends Controller {
  def index = Action { request =>
    Ok("true")
  }
}
