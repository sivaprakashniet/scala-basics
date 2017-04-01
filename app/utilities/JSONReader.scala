package utilities

import play.api.libs.json.{JsObject, JsValue}

class JSONReader {
  def jsvalueToMap(jsvalue: JsValue): Any = jsvalue.getClass.getSimpleName match {
    case "JsString" => jsvalue.as[String]
    case "JsNumber" => jsvalue.as[Int]
    case "JsArray" => jsvalue.as[List[JsValue]].map(x => jsvalueToMap(x))
    case "JsObject" => {
      val m = jsvalue.as[JsObject].value
      m map { case (key, value) => (key, jsvalueToMap(value)) }
    }
    case "JsBoolean" => jsvalue.as[Boolean]
    case "JsNull" => null
    case _ => jsvalue
  }

  def jsonToMap(jsvalue: JsValue): Map[String,Any] = {
    jsvalueToMap(jsvalue).asInstanceOf[Map[String,Any]]
  }

}
