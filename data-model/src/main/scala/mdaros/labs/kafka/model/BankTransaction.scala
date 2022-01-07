package mdaros.labs.kafka.model

import java.time.ZonedDateTime

case class BankTransaction ( name: String, amount: Int, time: ZonedDateTime )