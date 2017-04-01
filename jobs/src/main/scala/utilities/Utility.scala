package utilities

import java.io.{BufferedWriter, File, FileWriter}

object Utility {

  def store_result(result: String, request_id: String) = {
    val result_file = new File("results/result_"+ request_id +".txt")
    result_file.getParentFile.mkdirs()
    val bw = new BufferedWriter(new FileWriter(result_file))
    bw.write(result)
    bw.close()
  }

  def store_status(status: String, request_id: String) = {
    val status_file = new File("results/status_"+ request_id +".txt")
    status_file.getParentFile.mkdirs()
    val bw = new BufferedWriter(new FileWriter(status_file))
    bw.write(status)
    bw.close()
  }

  def create_error_response(exception: String): String = {
    exception.replace("\n","\\n").replace("\t","\\t")
  }

}