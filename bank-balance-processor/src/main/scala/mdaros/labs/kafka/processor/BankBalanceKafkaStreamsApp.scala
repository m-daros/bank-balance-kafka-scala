package mdaros.labs.kafka.processor

import mdaros.labs.kafka.processor.model.BankBalance
import mdaros.labs.kafka.model.serde.BankTransactionSerde
import org.apache.kafka.streams.{ KafkaStreams, StreamsConfig, Topology }
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.slf4j.LoggerFactory

import java.time.{ Duration, ZonedDateTime }
import java.util.Properties
import mdaros.labs.kafka.model.{ BankTransaction, Topics }
import mdaros.labs.kafka.processor.serde.CustomSerdes.BankBalanceSerde

import scala.sys.ShutdownHookThread

object BankBalanceKafkaStreamsApp {

  val logger = LoggerFactory.getLogger ( BankBalanceKafkaStreamsApp.getClass )

  def main ( args: Array [String] ) = {

    val props = new Properties ()
    props.put ( StreamsConfig.APPLICATION_ID_CONFIG, "bank-balance-application" )
    props.put ( StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092" )
    props.put ( StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass )
    props.put ( StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass )

    // Disabling the cache to show all the steps during the transformations
    // DO NOT IN PRODUCTION !!
    props.put ( StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0" )

    // Enable Exactly-Once guarantees
    props.put ( StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE )

    val topology: Topology = buildTopology ()

    val _ = startStreams ( topology, props )
  }

  private def buildTopology (): Topology = {

    implicit val bankTransactionSerde = new BankTransactionSerde ()
    implicit val bankBalanceSerde = new BankBalanceSerde ()

    val builder = new StreamsBuilder ()

    // We expect that the topic will contains already the user as a key
    builder.stream [ String, BankTransaction ]( Topics.BANK_BALANCE_MONEY_TRANSACTIONS )
      .groupByKey
      .aggregate ( this.newBankBalance () ) ( ( user, transaction, accum ) => aggregate ( user, transaction, accum ) )
      .toStream
      .peek ( (user, bankBalance) => logger.info ( s"AGGEGATED balance for user ${ user } ${ bankBalance }" ) ) // TODO TMP for DEBUG purpose
      .to ( Topics.BANK_BALANCE_BY_USER )

    builder.build ()
  }

  private def startStreams ( topology: Topology, props: Properties ): ShutdownHookThread = {

    logger.info ( topology.describe ().toString )

    val application: KafkaStreams = new KafkaStreams ( topology, props )
    application.start ()

    // Setup a shutdown hook
    sys.ShutdownHookThread {

      logger.info ( "Closing Kafka Stream app" )
      application.close ( Duration.ofSeconds ( 10 ) )
    }
  }

  private def newBankBalance (): BankBalance = {

    BankBalance ( "", 0, 0, ZonedDateTime.now () )
  }

  private def aggregate ( user: String, transaction: BankTransaction, accum: BankBalance ): BankBalance = {

    BankBalance ( user, accum.balance + transaction.amount, accum.transactionCount + 1, max ( accum.lastUpdate, transaction.time ) )
  }

  private def max ( date1: ZonedDateTime, date2: ZonedDateTime ): ZonedDateTime = {

    import java.time.Instant

    val epoch = math.max ( date1.toEpochSecond * 1000, date2.toEpochSecond * 1000 )

    ZonedDateTime.ofInstant ( Instant.ofEpochMilli ( epoch ), date1.getZone )
  }
}