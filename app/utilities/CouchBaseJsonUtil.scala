package com.db.util

import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.{JsonArray, JsonObject}
import com.couchbase.client.java.query.Select._
import com.couchbase.client.java.query.{Statement, N1qlQuery => Query}
import com.couchbase.client.java.query.dsl.Expression
import com.couchbase.client.java.query.dsl.Expression._
import play.api.libs.json.JsValue
import play.api.Logger

import scala.collection.immutable.Map

/**
  * Created by Prakash Perumal on 6/08/21.
  */

trait CouchBaseJsonUtils {

  def toCBJsonObject(project : JsValue) : JsonObject = {
    JsonObject.fromJson(project.toString())
  }

  def CBJsonDocument(id : String, jsonObject: JsonObject) : JsonDocument = {
    JsonDocument.create(id, jsonObject)
  }

  def toCBJsonArray : Seq[String] => JsonArray = {seq => seq.foldLeft(JsonArray.empty())((a, s) => a.add(s))}

}

object QueryBuilder extends CouchBaseJsonUtil {
  trait Op
  case object Equal extends Op
  case object Like extends Op
  case object GreaterThan extends Op
  case object GreaterThanOrEqual extends Op
  case object LessThan extends Op
  case object LessThanOrEqual extends Op
  case object In extends Op
  case object InList extends Op
  case object NotInList extends Op
  case class AndOr(status: String) extends Op
  case class Between(toField: String) extends Op
  case class HasCurrentTask(other: String) extends Op
  case object HasTags extends Op
  case object Ignore extends Op
  case class HasAccessGroups(dsId: String) extends Op
  case object ProjectTree extends Op
  case class CaseThenEnd(caseThenElseEnd: CaseThenElseEnd) extends Op
  case class CaseThenEndHistory(caseThenElseEnd: CaseThenElseEnd) extends Op
  case object Unspecified extends Op
  case object UnspecifiedOrTrue extends Op
  case object WithInList extends Op
  case object WithNotInList extends Op
  case object DeletedProj extends Op



  def convert(key: String): String = key.replace('.','_')

  def convertLikeValue(value: Any, cluase: Op): Any =  {
    (value, cluase) match {
      case (_, Like) if value.isInstanceOf[String] => "%" + value.toString.toLowerCase + "%"
      case (value, _) if value.isInstanceOf[Seq[String]] =>
        toCBJsonArray(value.asInstanceOf[Seq[String]])
      case _ =>
        value
    }

  }

  def getNonEmpty(parameters: Map[String, Option[(Any, Op)]]): Map[String, (Any, Op)] =
    parameters.filter(pair => pair._2.isDefined).map( pair => (pair._1, pair._2.get))


  def getWhereClause(nonEmptyParameters: Map[String, (Any, Op)],
                     favByUserOptSeq: Option[Seq[String]]): Expression = {
    val expressions = nonEmptyParameters.keys.map(
      key => nonEmptyParameters(key)._2 match {
        case HasAccessGroups(dsId) => x(s" ( " +
          s"(projectTeam.accessGroups IS MISSING OR ARRAY_LENGTH(projectTeam.accessGroups) = 0) " +
          s"OR (ANY accessGroup IN projectTeam.accessGroups SATISFIES accessGroup.id in $$${convert(key)} END) " +
          s"OR (${dsId} IN projectTeam.members)" +
          s")" )
        case Like => x("LOWER(" + key +")").like(x("$" + convert(key)))
        case GreaterThan => x(key).gt(x("$" + convert(key)))
        case GreaterThanOrEqual => x(key).gte(x("$" + convert(key)))
        case LessThan => x(key).lt(x("$" + convert(key)))
        case LessThanOrEqual => x(key).lte(x("$" + convert(key)))
        case In => x("$" + convert(key)).in(x(key))
        case InList =>  x(s"($key IN $$$key)")
        case NotInList =>  x(s"($key NOT IN $$$key)")
        case Between(toField) =>  x(key).between(x("$"+convert(key))).and(x("$"+convert(toField)))
        case HasCurrentTask(other) => x(s"ANY task IN tasks SATISFIES task.`from` <= $$${key} AND task.`to` >= $$${key} AND task.name = $$${other} END")
        case ProjectTree => x(s"( id = $$${key} OR parent = $$${key} OR (ANY project IN info.linkedProjects SATISFIES project = $$$key END) )")
        case Ignore => Expression.x("Ignore")
        case HasTags => x(s"ANY tag IN info.tags SATISFIES tag in $$${convert(key)} END")
        case AndOr(status) =>x(s"( status = $$${status} OR projEndDt >= $$${key})")
        case CaseThenEnd(params) => x(s" CASE WHEN status = '"+params.status+"' THEN (CASE WHEN projLiveDt IS MISSING THEN (projEndDt >= STR_TO_MILLIS('"+params.startPeriod+"') AND projEndDt < STR_TO_MILLIS('"+params.endPeriod+"')) ELSE (projLiveDt >= STR_TO_MILLIS('"+params.startPeriod+"') AND projLiveDt < STR_TO_MILLIS('"+params.endPeriod+"')) END) ELSE projEndDt >= "+params.projEndDate+" END")
        case CaseThenEndHistory(params) => x(s" CASE WHEN status = '"+params.status+"' THEN (CASE WHEN projLiveDt IS MISSING THEN (projEndDt >= STR_TO_MILLIS('"+params.startPeriod+"') AND projEndDt < STR_TO_MILLIS('"+params.endPeriod+"')) ELSE (projLiveDt >= STR_TO_MILLIS('"+params.startPeriod+"') AND projLiveDt < STR_TO_MILLIS('"+params.endPeriod+"')) END) END")
        case WithInList =>  x(s"($key IN $$${convert(key)})")
        case WithNotInList =>  x(s"($key NOT IN $$${convert(key)})")
        case Unspecified => x(s" ($key IS NOT VALUED OR $key = '')")
        case UnspecifiedOrTrue => x(s" ($key IS NOT VALUED OR $key =true)")
        case DeletedProj => x(s" ((type='PROJECT_REVISION' AND status='DELETED') OR type='PROJECT')")
        case _ => x(key).eq(x("$" + convert(key)))


      }
    ).toList ++ favByUserOptSeq.toList.map( _ => x("id").in(x("$favByUser")))

    expressions match {
      case Nil => Expression.TRUE()
      case _ =>
        expressions.reduce((e1, e2) => {
          (e1, e2) match {
            case (e1, e2) if e1.toString.equals("Ignore") => e2
            case (e1, e2) if e2.toString.equals("Ignore") => e1
            case _ =>  e1.and(e2)
          }
        })
    }
  }
  def getValueMap(nonEmptyParameters: Map[String, (Any, Op)],
                  favByUserOptSeq: Option[Seq[String]]): JsonObject = {
    val valueMap : JsonObject = JsonObject.create()
    nonEmptyParameters.map(pair => valueMap.put(convert(pair._1), convertLikeValue(pair._2._1, pair._2._2)))
    if (favByUserOptSeq.isDefined)
      valueMap.put("favByUser", toCBJsonArray(favByUserOptSeq.get))
    else valueMap
  }


  def createGetQuery(parameters: Map[String, Option[(Any, Op)]],
                     favByUserOptSeq: Option[Seq[String]],
                     bucketName: String,
                     limit: String): Query = {

    val nonEmptyParameters: Map[String, (Any, Op)] = getNonEmpty(parameters)
    val whereClause: Expression = getWhereClause(nonEmptyParameters, favByUserOptSeq)
    val valueMap = getValueMap(nonEmptyParameters,favByUserOptSeq)

      val stmt : Statement = select(i(bucketName) + ".*").from(i(bucketName)).where(whereClause + s" limit ${limit}")
      Query.parameterized(stmt, valueMap)
      
    }
  

  def createStatusQuery(parameters: Map[String, Option[(Any, Op)]],
                        favByUserOptSeq: Option[Seq[String]],
                        bucketName: String,
                        limit: String): Query = {
    val nonEmptyParameters: Map[String, (Any, Op)] = getNonEmpty(parameters)
    val whereClause: Expression = getWhereClause(nonEmptyParameters, favByUserOptSeq)
    val valueMap = getValueMap(nonEmptyParameters,favByUserOptSeq)

        val stmt : Statement =
      select(s"COUNT(*) as count, status")
        .from(i(bucketName))
        .where(whereClause)
        .groupBy(i("status"))
    Query.parameterized(stmt, valueMap)
  }

}
