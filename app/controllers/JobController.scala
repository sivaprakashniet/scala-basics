package controllers

import javax.inject.Inject

import play.api.Configuration
import play.api.libs.json.{JsValue, Json}
import play.api.mvc.{Action, Controller}
import services.SparkJobExecutor
import utilities.{JSONReader, MapReader}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class JobController @Inject()(play_configuration: Configuration, map_reader: MapReader, json_reader: JSONReader, spark_job_executor: SparkJobExecutor) extends Controller  {

  def jsonToMap(json: JsValue) = json_reader.jsonToMap(json)

  def sparkJobExecutor = Action.async(parse.json) { implicit request =>
    val json = request.body
    val request_map = jsonToMap(json)

    Future{
      val response = spark_job_executor.execute_spark_job(request_map)
      Ok(Json.parse(response))
    }
  }
}