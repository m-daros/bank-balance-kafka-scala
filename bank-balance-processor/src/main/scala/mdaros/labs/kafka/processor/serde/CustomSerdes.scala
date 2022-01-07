package mdaros.labs.kafka.processor.serde

import mdaros.labs.kafka.model.serde.{ TypedDeserializer, TypedSerde, TypedSerializer }
import mdaros.labs.kafka.processor.model.BankBalance

object CustomSerdes {

  class BankBalanceDeserializer extends TypedDeserializer [BankBalance]
  class BankBalanceSerializer extends TypedSerializer [BankBalance]
  class BankBalanceSerde extends TypedSerde [BankBalance]
}