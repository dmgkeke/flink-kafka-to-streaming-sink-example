package pb.serde

import com.google.protobuf.Message
import org.apache.commons.lang3.SerializationException
import org.apache.flink.api.common.serialization.SerializationSchema

import scala.util.{Failure, Success, Try}

class ProtoBufSerializationSchema[T <: Message] extends SerializationSchema[T] {

  override def serialize(element: T): Array[Byte] = {
    Try(element.asInstanceOf[Message].toByteArray) match {
      case Success(bytes) =>
        bytes
      case Failure(e) =>
        throw new SerializationException(s"Unable to serialize bytes for $element", e)
    }
  }
}