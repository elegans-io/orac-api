package io.elegans.orac.serializers

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 27/06/16.
  */

import io.elegans.orac.entities._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json._

trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val updateActionFormat = jsonFormat4(UpdateAction)
  implicit val actionFormat = jsonFormat5(Action)
  implicit val actionsFormat = jsonFormat1(Actions)
  implicit val numericalPropertiesFormat = jsonFormat2(NumericalProperties)
  implicit val timestampPropertiesFormat = jsonFormat2(TimestampProperties)
  implicit val stringPropertiesFormat = jsonFormat2(StringProperties)
  implicit val itemPropertiesFormat = jsonFormat4(ItemProperties)
  implicit val updateItemFormat = jsonFormat4(UpdateItem)
  implicit val itemFormat = jsonFormat5(Item)
  implicit val itemListFormat = jsonFormat1(Items)
  implicit val updateOracUserFormat = jsonFormat6(UpdateOracUser)
  implicit val oracUserFormat = jsonFormat7(OracUser)
  implicit val returnMessageDataFormat = jsonFormat2(ReturnMessageData)
  implicit val updateDocumentResultFormat = jsonFormat5(UpdateDocumentResult)
  implicit val indexManagementResponseFormat = jsonFormat1(IndexManagementResponse)
  implicit val indexDocumentResultFormat = jsonFormat3(IndexDocumentResult)
  implicit val deleteDocumentResultFormat = jsonFormat3(DeleteDocumentResult)
  implicit val failedShardFormat = jsonFormat4(FailedShard)
  implicit val refreshIndexResultFormat = jsonFormat5(RefreshIndexResult)
  implicit val refreshIndexResultsFormat = jsonFormat1(RefreshIndexResults)
  implicit val recommendationFormat = jsonFormat6(Recommendation)
  implicit val recommendationsFormat = jsonFormat1(Recommendations)
  implicit val updateRecommendation = jsonFormat5(UpdateRecommendation)

  implicit object PermissionsJsonFormat extends JsonFormat[Permissions.Value] {
    def write(obj: Permissions.Value): JsValue = JsString(obj.toString)
    def read(json: JsValue): Permissions.Value = json match {
      case JsString(str) => Permissions.withName(str)
      case _ => throw DeserializationException("Permission string expected")
    }
  }
  implicit val userFormat = jsonFormat4(User)
  implicit val userUpdateFormat = jsonFormat3(UserUpdate)
}
