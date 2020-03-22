package models

import play.api.libs.json._

case class SensorDataEvent(readDateStart: Option[String],
                           readDateEnd: Option[String],
                           value: Int
                          )

object SensorDataEvent {
  implicit val jsonReads: Reads[SensorDataEvent] = Json.reads[SensorDataEvent]
  implicit val jsonWrites: Writes[SensorDataEvent] = Json.writes[SensorDataEvent]
}

case class SensorDataChange(sensorUID: String,
                            month: Int,
                            day: Int,
                            year: Int,
                            readType: String,
                            roomId: Long,
                            events: Seq[SensorDataEvent])

object SensorDataChange {
  implicit val jsonReads: Reads[SensorDataChange] = Json.reads[SensorDataChange]
  implicit val jsonWrites: Writes[SensorDataChange] = Json.writes[SensorDataChange]
}
