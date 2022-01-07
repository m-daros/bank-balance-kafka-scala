package mdaros.labs.kafka.model.serde

import java.io.{ ByteArrayInputStream, ObjectInputStream }
import java.util.Map
import org.apache.kafka.common.serialization.Deserializer

class TypedDeserializer [T] extends Deserializer [T] {

  override def configure ( configs: Map [ String, _ ], isKey: Boolean ): Unit = {}

  override def deserialize ( topic: String, data: Array [ Byte ] ): T = {

    val byteIn = new ByteArrayInputStream ( data )
    val objIn = new ObjectInputStream ( byteIn )
    val obj = objIn.readObject ().asInstanceOf [T]
    byteIn.close ()
    objIn.close ()

    obj
  }

  override def close (): Unit = {

  }
}