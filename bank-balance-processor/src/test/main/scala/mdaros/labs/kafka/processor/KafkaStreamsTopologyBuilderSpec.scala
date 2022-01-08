package mdaros.labs.kafka.processor

import mdaros.labs.kafka.model.BankTransaction
import mdaros.labs.kafka.processor.model.BankBalance
import mdaros.labs.kafka.model.Topics
import mdaros.labs.kafka.model.serde.BankTransactionSerializer
import mdaros.labs.kafka.processor.serde.CustomSerdes.BankBalanceDeserializer
import org.apache.kafka.common.serialization.{ StringDeserializer, StringSerializer }
import org.apache.kafka.streams.TopologyTestDriver
import org.scalatest.{ BeforeAndAfter, GivenWhenThen }
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should.Matchers

import java.time.ZonedDateTime

class KafkaStreamsTopologyBuilderSpec extends AnyFeatureSpec
  with GivenWhenThen
  with BeforeAndAfter
  with Matchers {

  var testDriver: TopologyTestDriver = null

  before {

    val streamsBuilder = new KafkaStreamsTopologyBuilder ()
    val topology = streamsBuilder.buildTopology ()

    testDriver = new TopologyTestDriver ( topology )
  }

  after {

    testDriver.close ()
  }

  Feature ( "Grouping bank transactions by user" ) {

    Scenario ( "Valid topology" ) {

      Given ( s"I have an inpout topic ${Topics.BANK_BALANCE_MONEY_TRANSACTIONS} and an output topic ${Topics.BANK_BALANCE_BY_USER}" )

      val input = testDriver.createInputTopic [ String, BankTransaction ] ( Topics.BANK_BALANCE_MONEY_TRANSACTIONS, new StringSerializer (), new BankTransactionSerializer () )
      val output = testDriver.createOutputTopic [ String, BankBalance ] ( Topics.BANK_BALANCE_BY_USER, new StringDeserializer (), new BankBalanceDeserializer () )

      val bankTransaction1 = new BankTransaction ( "John", 100, ZonedDateTime.now () )
      val bankTransaction2 = new BankTransaction ( "Marc", 120, ZonedDateTime.now () )
      val bankTransaction3 = new BankTransaction ( "Angela", 500, ZonedDateTime.now () )
      val bankTransaction4 = new BankTransaction ( "Ronda", 124, ZonedDateTime.now () )
      val bankTransaction5 = new BankTransaction ( "Sam", 250, ZonedDateTime.now () )
      val bankTransaction6 = new BankTransaction ( "John", 130, ZonedDateTime.now () )
      val bankTransaction7 = new BankTransaction ( "Angela", 130, ZonedDateTime.now () )

      val expectedBalance1 = new BankBalance ( "John", 100, 1, ZonedDateTime.now () )
      val expectedBalance2 = new BankBalance ( "Marc", 120, 1, ZonedDateTime.now () )
      val expectedBalance3 = new BankBalance ( "Angela", 500, 1, ZonedDateTime.now () )
      val expectedBalance4 = new BankBalance ( "Ronda", 124, 1, ZonedDateTime.now () )
      val expectedBalance5 = new BankBalance ( "Sam", 250, 1, ZonedDateTime.now () )
      val expectedBalance6 = new BankBalance ( "John", 230, 2, ZonedDateTime.now () )
      val expectedBalance7 = new BankBalance ( "Angela", 630, 2, ZonedDateTime.now () )

      When ( s"I send ( key, value ) ${bankTransaction1.name},${bankTransaction1} to input topic" )
      input.pipeInput ( bankTransaction1.name, bankTransaction1 )

      Then ( s"I expect to read from output topic ( key, value ) ${expectedBalance1.user},${expectedBalance1}" )
      val keyValue1 = output.readKeyValue ()
      keyValue1.key shouldBe ( expectedBalance1.user )
      keyValue1.value.user shouldBe ( expectedBalance1.user )
      keyValue1.value.balance shouldBe ( expectedBalance1.balance )
      keyValue1.value.transactionCount shouldBe ( expectedBalance1.transactionCount )
      keyValue1.value.lastUpdate should not be null

      When ( s"I send ( key, value ) ${bankTransaction2.name},${bankTransaction2} to input topic" )
      input.pipeInput ( bankTransaction2.name, bankTransaction2 )

      Then ( s"I expect to read from output topic ( key, value ) ${expectedBalance2.user},${expectedBalance2}" )
      val keyValue2 = output.readKeyValue ()
      keyValue2.key shouldBe ( expectedBalance2.user )
      keyValue2.value.user shouldBe ( expectedBalance2.user )
      keyValue2.value.balance shouldBe ( expectedBalance2.balance )
      keyValue2.value.transactionCount shouldBe ( expectedBalance2.transactionCount )
      keyValue2.value.lastUpdate should not be null

      When ( s"I send ( key, value ) ${bankTransaction3.name},${bankTransaction3} to input topic" )
      input.pipeInput ( bankTransaction3.name, bankTransaction3 )

      Then ( s"I expect to read from output topic ( key, value ) ${expectedBalance3.user},${expectedBalance3}" )
      val keyValue3 = output.readKeyValue ()
      keyValue3.key shouldBe ( expectedBalance3.user )
      keyValue3.value.user shouldBe ( expectedBalance3.user )
      keyValue3.value.balance shouldBe ( expectedBalance3.balance )
      keyValue3.value.transactionCount shouldBe ( expectedBalance3.transactionCount )
      keyValue3.value.lastUpdate should not be null

      When ( s"I send ( key, value ) ${bankTransaction4.name},${bankTransaction4} to input topic" )
      input.pipeInput ( bankTransaction4.name, bankTransaction4 )

      Then ( s"I expect to read from output topic ( key, value ) ${expectedBalance4.user},${expectedBalance4}" )
      val keyValue4 = output.readKeyValue ()
      keyValue4.key shouldBe ( expectedBalance4.user )
      keyValue4.value.user shouldBe ( expectedBalance4.user )
      keyValue4.value.balance shouldBe ( expectedBalance4.balance )
      keyValue4.value.transactionCount shouldBe ( expectedBalance4.transactionCount )
      keyValue4.value.lastUpdate should not be null

      When ( s"I send ( key, value ) ${bankTransaction5.name},${bankTransaction5} to input topic" )
      input.pipeInput ( bankTransaction5.name, bankTransaction5 )

      Then ( s"I expect to read from output topic ( key, value ) ${expectedBalance5.user},${expectedBalance5}" )
      val keyValue5 = output.readKeyValue ()
      keyValue5.key shouldBe ( expectedBalance5.user )
      keyValue5.value.user shouldBe ( expectedBalance5.user )
      keyValue5.value.balance shouldBe ( expectedBalance5.balance )
      keyValue5.value.transactionCount shouldBe ( expectedBalance5.transactionCount )
      keyValue5.value.lastUpdate should not be null

      When ( s"I send ( key, value ) ${bankTransaction6.name},${bankTransaction6} to input topic" )
      input.pipeInput ( bankTransaction6.name, bankTransaction6 )

      Then ( s"I expect to read from output topic ( key, value ) ${expectedBalance6.user},${expectedBalance6}" )
      val keyValue6 = output.readKeyValue ()
      keyValue6.key shouldBe ( expectedBalance6.user )
      keyValue6.value.user shouldBe ( expectedBalance6.user )
      keyValue6.value.balance shouldBe ( expectedBalance6.balance )
      keyValue6.value.transactionCount shouldBe ( expectedBalance6.transactionCount )
      keyValue6.value.lastUpdate should not be null

      When ( s"I send ( key, value ) ${bankTransaction7.name},${bankTransaction7} to input topic" )
      input.pipeInput ( bankTransaction7.name, bankTransaction7 )

      Then ( s"I expect to read from output topic ( key, value ) ${expectedBalance7.user},${expectedBalance7}" )
      val keyValue7 = output.readKeyValue ()
      keyValue7.key shouldBe ( expectedBalance7.user )
      keyValue7.value.user shouldBe ( expectedBalance7.user )
      keyValue7.value.balance shouldBe ( expectedBalance7.balance )
      keyValue7.value.transactionCount shouldBe ( expectedBalance7.transactionCount )
      keyValue7.value.lastUpdate should not be null
    }
  }
}