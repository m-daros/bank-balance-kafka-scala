package mdaros.labs.kafka.processor

import org.apache.kafka.streams.{ KafkaStreams, StreamsConfig, Topology }
import org.apache.kafka.streams.scala.serialization.Serdes
import org.slf4j.LoggerFactory

import java.time.Duration
import java.util.Properties

import scala.sys.ShutdownHookThread

object BankBalanceKafkaStreamsApp {

  private val logger = LoggerFactory.getLogger ( BankBalanceKafkaStreamsApp.getClass )

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

    val topologyBuilder = new KafkaStreamsTopologyBuilder ()
    val topology: Topology = topologyBuilder.buildTopology ()

    val _ = startStreams ( topology, props )
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
}