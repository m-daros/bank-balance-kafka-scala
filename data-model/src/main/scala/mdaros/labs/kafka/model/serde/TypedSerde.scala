package mdaros.labs.kafka.model.serde

import org.apache.kafka.common.serialization.{ Deserializer, Serde, Serializer }

class TypedSerde [T] extends Serde [T] {

  override def serializer (): Serializer [ T ] = new TypedSerializer [T]

  override def deserializer (): Deserializer [ T ] = new TypedDeserializer [T]
}