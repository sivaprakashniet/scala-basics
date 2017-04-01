package utilities

import play.api.libs.json.{JsNull, JsObject, JsValue, Json}

class MapReader {
  def mapToJson(x: Any): JsValue = {
    x match {
      case j: JsValue => x.asInstanceOf[JsValue]
      case m: Map[String, Any] => Json.toJson(JsObject(x.asInstanceOf[Map[String, Any]].map(y => (y._1 -> mapToJson(y._2)))))
      case l: List[Any] => Json.toJson(x.asInstanceOf[List[Any]].map(y => mapToJson(y)))
      case s: String => Json.toJson(x.asInstanceOf[String])
      case b: Boolean => Json.toJson(x.asInstanceOf[Boolean])
      case i: Int => Json.toJson(x.asInstanceOf[Int])
      case lo: Long => Json.toJson(x.asInstanceOf[Long])
      case d: Double => Json.toJson(x.asInstanceOf[Double])
      case List() => Json.toJson(JsNull)
      case seq: List[List[Any]] => Json.toJson(x.asInstanceOf[List[List[Any]]].map(y => mapToJson(y)))
      //    case null => Json.toJson(JsNull)
      case _ => Json.toJson(x.toString())
    }}

  def mapToJson(m: Map[String,Any]): JsValue = {
    mapToJson(m.asInstanceOf[Any])
  }

  def mapToJsString(m:Map[String, Any]): String = {
    mapToJson(m.asInstanceOf[Any]).toString
  }
}
