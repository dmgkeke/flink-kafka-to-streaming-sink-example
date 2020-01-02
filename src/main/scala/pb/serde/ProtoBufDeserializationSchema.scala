package pb.serde

import com.google.protobuf.Message
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.types.DeserializationException

import scala.util.{Failure, Success, Try}

class ProtoBufDeserializationSchema[T <: Message](parser: Array[Byte] => T) extends DeserializationSchema[T] {

  override def deserialize(bytes: Array[Byte]): T = {
    Try(parser.apply(bytes)) match {
      case Success(element) =>
        element
      case Failure(e) =>
        val className = getClass.asInstanceOf[Class[T]].getName
        throw new DeserializationException(s"Unable to de-serialize bytes to $className", e)
    }
  }

  override def isEndOfStream(nextElement: T): Boolean = false

  override def getProducedType: TypeInformation[T] = TypeExtractor.getForClass(getClass).asInstanceOf[TypeInformation[T]]
}