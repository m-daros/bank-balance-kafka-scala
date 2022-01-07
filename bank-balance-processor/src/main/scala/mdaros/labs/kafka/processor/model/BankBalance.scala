package mdaros.labs.kafka.processor.model

import java.time.ZonedDateTime

case class BankBalance ( user: String, balance: Int, transactionCount: Int, lastUpdate: ZonedDateTime )