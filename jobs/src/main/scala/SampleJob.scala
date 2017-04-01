/* SampleJob.scala */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import scala.util.parsing.json.JSON
import utilities.Utility._

object SampleJob {

  var request_id: String = null
  var value: Any = null
  var value2: Any = null

  def main(args: Array[String]) {
    try {
      val arguments = args.toList
      request_id = arguments(0)
      set_parameters(arguments(1))
      val conf = new SparkConf().setAppName("SampleJob")
      val sc = new SparkContext(conf)
      val response = run_job(sc)
      store_status("completed", request_id)
      store_result(response, request_id)
    }
    catch {
      case exception: Exception => {
        println(exception)
        val exception_string = create_error_response(exception.toString)
        store_status("error", request_id)
        store_result(exception_string, request_id)
      }
    }
  }

  def run_job(sc: SparkContext): String = {
    val rdd_count = sc.parallelize("r_count").count()
    val response = create_ok_response(rdd_count)
    response
  }

  def create_ok_response(rdd_count: Long): String = {
    """ {"rdd_count": """ + rdd_count + "}"
  }

  def set_parameters(argument_string: String) = {
    val argument_json = JSON.parseFull(argument_string).get
    value = argument_json.asInstanceOf[Map[String, Any]]("string")
    value2 = argument_json.asInstanceOf[Map[String, Any]]("array")
  }
}