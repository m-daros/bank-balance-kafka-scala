package mdaros.labs.kafka.producer

import akka.actor.ActorSystem
import akka.stream.scaladsl.{ Sink, Source }
import mdaros.labs.kafka.model.{ BankTransaction, Topics }

import java.time.{ Duration, ZonedDateTime }
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps
import scala.util.Random
import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerConfig, ProducerRecord }
import org.slf4j.LoggerFactory

import java.util.Properties
import scala.concurrent.ExecutionContext

object BankTransactionsProducerApp {

  val logger = LoggerFactory.getLogger ( BankTransactionsProducerApp.getClass )

  def main ( args: Array [String] ) = {

    implicit val actorSystem = ActorSystem ( "bank-transactions-producer" )
    implicit val executionContext = ExecutionContext.Implicits.global

    val producer = buildKafkaProducer ()

    val users = Seq ( "John", "Andrew", "Sandra", "Robert", "Philip", "Sam", "Maggie", "Rebecca", "Mary", "Arthur" )

    val randomGenerator = new Random

    // Manage an Akka Streams flow to generate a random BankTransaction and send it to a Kafka topic
    Source.tick [ ZonedDateTime ] ( 1 seconds, 5 seconds, ZonedDateTime.now () )
      .map ( _ => randomBankTransaction ( users, randomGenerator ) )
      .runWith ( Sink.foreach [BankTransaction] ( transaction => producer.send ( producerRecord ( Topics.BANK_BALANCE_MONEY_TRANSACTIONS, transaction ) ) ) )

    val _ = sys.ShutdownHookThread {

      logger.info ( "Closing consumer" )
      producer.close ( Duration.ofSeconds ( 10 ) )
    }
  }

  private def buildKafkaProducer (): KafkaProducer [String, BankTransaction] = {

    val props = new Properties ()
    props.put ( ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092" )
    props.put ( ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer" )
    props.put ( ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "mdaros.labs.kafka.model.serde.BankTransactionSerializer" )
    props.put ( ProducerConfig.ACKS_CONFIG, "all" )
    props.put ( ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true" )

    new KafkaProducer [ String, BankTransaction ] ( props )
  }

  private def randomBankTransaction ( users: Seq [String], randomGenerator: Random ): BankTransaction = {

    val index = randomGenerator.between ( 0, users.length - 1 )
    val amount = randomGenerator.between ( 1, 501 )

    BankTransaction ( users ( index ), amount, ZonedDateTime.now () )
  }

  private def producerRecord ( topic: String, bankTransaction: BankTransaction ) = {

    new ProducerRecord [String, BankTransaction] ( topic, bankTransaction.name, bankTransaction )
  }
}