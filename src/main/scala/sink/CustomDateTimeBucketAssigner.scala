package sink

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId}

import datatypes.ResultVO
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner

/*
* Class to override partition directory
*/
case class CustomDateTimeBucketAssigner(formatString: String, zoneId: ZoneId) extends DateTimeBucketAssigner[ResultVO](formatString, zoneId) {

  @transient
  lazy val dateTimeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(this.formatString).withZone(zoneId)

  override def getBucketId(element: ResultVO, context: BucketAssigner.Context): String = {
    dateTimeFormatter.format(Instant.ofEpochMilli(element.ingestionTime))
  }
}
