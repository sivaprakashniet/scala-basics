package services

import java.io.{File, PrintWriter}
import java.util.{Scanner, UUID}

import utilities.MapReader

import scala.sys.process._
import scala.util.parsing.json.JSON

class SparkJobExecutor {
  val map_reader = new MapReader
  val logging_file_path = "logs/spark_logs_" + System.currentTimeMillis() + ".log"
  val logging_file = new File(logging_file_path)
  val data_writer = new PrintWriter(logging_file, "UTF-8")
  var request_id: String = null
  logging_file.getParentFile().mkdirs()

  def execute_spark_job(job_details: Map[String, Any]): String = {
    val jar_path = job_details("jar-path").toString
    val job_name = job_details("job-name").toString
    val spark_master = job_details("master").toString
    val deploy_mode = job_details("deploy-mode").toString
    val parameter_json_string = mapToJson(job_details("parameters").asInstanceOf[Map[String, Any]]).toString()
    val parameter_json = parameter_json_string.replaceAll("\\s","")
    execute_spark_job(jar_path, job_name, spark_master, deploy_mode, parameter_json)
  }

  def execute_spark_job(jar_path: String, job_name: String, spark_master: String, deploy_mode: String, parameters: String): String = {
    request_id = create_unique_id()
    val spark_submit_command = create_spark_submit_command(jar_path, job_name, spark_master, deploy_mode, parameters)
    val process_results = run_spark_submit_command(spark_submit_command)
    create_result(process_results)
  }

  def create_result(process_results: Map[String, String]): String = {
    if (process_results("exit-value") == "0") {
      val status = retrieve_status()
      val result = retrieve_result()
      if (status == "completed") {
        mapToJson(Map("status" -> status, "result" -> JSON.parseFull(result).get)).toString()
      }
      else {
        mapToJson(Map("status" -> status, "error" -> result)).toString()
      }
    } else {
      create_error_response(process_results("error-response"))
    }
  }

  def create_error_response(error_response: String): String = {
    mapToJson(Map("status" -> "error", "error" -> error_response.replace("\n", "\\n").replace("\t", "\\t"))).toString()
  }

  def mapToJson(map: Map[String, Any]) = map_reader.mapToJson(map)

  def retrieve_result(): String = {
    val result_file = new File("results/result_" + request_id + ".txt")
    val result = new Scanner(result_file).useDelimiter("\\Z").next()
    result_file.delete()
    result
  }

  def retrieve_status(): String = {
    val status_file = new File("results/status_" + request_id + ".txt")
    val status = new Scanner(status_file).useDelimiter("\\Z").next()
    status_file.delete()
    status
  }

  def run_spark_submit_command(spark_submit_command: String): Map[String, String] = {
    println("Executing: "+spark_submit_command)
    data_writer.append("Executing: "+spark_submit_command)

    var ok_response = ""
    var error_response = ""

    val io = new ProcessIO(
      stdin => {
        stdin.close()
      },
      stdout => {
        scala.io.Source.fromInputStream(stdout).getLines foreach { line =>
          println(line)
          data_writer.append(line + "\n")
          ok_response = ok_response + line
        }
        data_writer.flush()
        stdout.close()
      },
      stderr => {
        scala.io.Source.fromInputStream(stderr).getLines foreach { line =>
          println(line)
          data_writer.append(line + "\n")
          error_response = error_response + line
        }
        data_writer.flush()
        stderr.close()
      })
    val process = spark_submit_command.run(io)

    Map("exit-value" -> process.exitValue().toString,
      "ok-response" -> ok_response,
      "error-response" -> error_response)
  }

  def create_unique_id() = {
    UUID.randomUUID.toString
  }

  def create_spark_submit_command(jar_path: String, job_name: String, spark_master: String, deploy_mode: String, parameters: String): String = {
    // val spark_submit_command = "spark-submit --class SimpleJob --deploy-mode cluster --master yarn /home/admin1/play-codes/jobs_2.10-1.0-SNAPSHOT.jar 10"
    var spark_submit_command = "spark-submit --class " + job_name
    if (spark_master != "") {
      spark_submit_command = spark_submit_command + " --master " + spark_master
    }
    if (deploy_mode != "") {
      spark_submit_command = spark_submit_command + " --deploy-mode " + deploy_mode
    }
    spark_submit_command = spark_submit_command + " " + jar_path + " " + request_id + " " + parameters

    spark_submit_command
  }
}