package mdaros.labs.kafka.processor

import mdaros.labs.kafka.model.{ BankTransaction, Topics }
import mdaros.labs.kafka.model.serde.BankTransactionSerde
import mdaros.labs.kafka.processor.model.BankBalance
import mdaros.labs.kafka.processor.serde.CustomSerdes.BankBalanceSerde
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.slf4j.LoggerFactory

import java.time.ZonedDateTime

class KafkaStreamsTopologyBuilder {

  def buildTopology (): Topology = {

    val logger = LoggerFactory.getLogger ( getClass () )

    implicit val bankTransactionSerde: BankTransactionSerde = new BankTransactionSerde ()
    implicit val bankBalanceSerde: BankBalanceSerde = new BankBalanceSerde ()

    val builder = new StreamsBuilder ()

    // We expect that the topic will contains already the user as a key
    builder.stream [ String, BankTransaction ] ( Topics.BANK_BALANCE_MONEY_TRANSACTIONS )
      .groupByKey
      .aggregate ( this.newBankBalance () ) ( ( user, transaction, accum ) => aggregate ( user, transaction, accum ) )
      .toStream
      .peek ( (user, bankBalance) => logger.info ( s"AGGEGATED balance for user ${ user } ${ bankBalance }" ) ) // TODO TMP for DEBUG purpose
      .to ( Topics.BANK_BALANCE_BY_USER )

    builder.build ()
  }

  private def aggregate ( user: String, transaction: BankTransaction, accum: BankBalance ): BankBalance = {

    BankBalance ( user, accum.balance + transaction.amount, accum.transactionCount + 1, max ( accum.lastUpdate, transaction.time ) )
  }

  private def newBankBalance (): BankBalance = {

    BankBalance ( "", 0, 0, ZonedDateTime.now () )
  }

  private def max ( date1: ZonedDateTime, date2: ZonedDateTime ): ZonedDateTime = {

    import java.time.Instant

    val epoch = math.max ( date1.toEpochSecond * 1000, date2.toEpochSecond * 1000 )

    ZonedDateTime.ofInstant ( Instant.ofEpochMilli ( epoch ), date1.getZone )
  }
}