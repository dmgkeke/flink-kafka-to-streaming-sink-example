package sink

import java.io.IOException

import datatypes.ResultVO
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.reflect.ReflectData
import org.apache.flink.formats.parquet.{ParquetBuilder, ParquetWriterFactory}
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.OutputFile

/*
* Refer to @ParquetAvroWriters
* The above class does not have many options of ParquetWriter.
* https://github.com/apache/flink/pull/7547
*/
object CustomParquetAvroWriters {
  def forReflectRecord(): ParquetWriterFactory[ResultVO] = {
    val schemaString = ReflectData.get.getSchema(classOf[ResultVO]).toString
    val builder = new ParquetBuilder[ResultVO] {
      override def createWriter(out: OutputFile): ParquetWriter[ResultVO] = createAvroParquetWriter(schemaString, ReflectData.get, out)
    }
    new ParquetWriterFactory[ResultVO](builder)
  }

  @throws[IOException]
  private def createAvroParquetWriter[RoutesVO](schemaString: String, dataModel: GenericData, out: OutputFile):ParquetWriter[RoutesVO] = {
    val schema = new Schema.Parser().parse(schemaString)
    AvroParquetWriter.builder[RoutesVO](out).withSchema(schema).withDataModel(dataModel).withCompressionCodec(CompressionCodecName.SNAPPY).build
  }
}
