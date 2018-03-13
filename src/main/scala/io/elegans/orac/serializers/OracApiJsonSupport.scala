package io.elegans.orac.serializers

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 27/06/16.
  */

import io.elegans.orac.entities._
import spray.json._

trait OracApiJsonSupport extends OracJsonSupport {
  implicit object PermissionsJsonFormat extends JsonFormat[Permissions.Value] {
    def write(obj: Permissions.Value): JsValue = JsString(obj.toString)
    def read(json: JsValue): Permissions.Value = json match {
      case JsString(str) => Permissions.withName(str)
      case _ => throw DeserializationException("Permission string expected")
    }
  }

  implicit object ReconcileTypeJsonFormat extends JsonFormat[ReconcileType.Value] {
    def write(obj: ReconcileType.Value): JsValue = JsString(obj.toString)
    def read(json: JsValue): ReconcileType.Value = json match {
      case JsString(str) => ReconcileType.withName(str)
      case _ => throw DeserializationException("ReconcileType string expected")
    }
  }

  implicit object ForwardTypeJsonFormat extends JsonFormat[ForwardType.Value] {
    def write(obj: ForwardType.Value): JsValue = JsString(obj.toString)
    def read(json: JsValue): ForwardType.Value = json match {
      case JsString(str) => ForwardType.withName(str)
      case _ => throw DeserializationException("ForwardType string expected")
    }
  }

  implicit object ForwardOperationTypeJsonFormat extends JsonFormat[ForwardOperationType.Value] {
    def write(obj: ForwardOperationType.Value): JsValue = JsString(obj.toString)
    def read(json: JsValue): ForwardOperationType.Value = json match {
      case JsString(str) => ForwardOperationType.withName(str)
      case _ => throw DeserializationException("ForwardOperationType string expected")
    }
  }

  implicit val userFormat = jsonFormat4(User)
  implicit val userUpdateFormat = jsonFormat3(UserUpdate)
  implicit val forwardFormat = jsonFormat7(Forward)
  implicit val itemInfoFormat = jsonFormat7(ItemInfo)
  implicit val updateItemInfoFormat = jsonFormat6(UpdateItemInfo)
  implicit val itemInfoRecordsFormat = jsonFormat1(ItemInfoRecords)
  implicit val reconcileFormat = jsonFormat7(Reconcile)
  implicit val reconcileHistoryFormat = jsonFormat8(ReconcileHistory)
  implicit val updateReconcileFormat = jsonFormat6(UpdateReconcile)
  implicit val openCloseIndexFormat = jsonFormat5(OpenCloseIndex)
}
