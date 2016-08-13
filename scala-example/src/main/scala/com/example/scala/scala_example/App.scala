package com.example.scala.scala_example

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords

/**
 * @author ${user.name}
 */
object App {
  // Sample Kafka Publish Subscribe demo with an infinite loop of 
  // a Kafka Producer publishing data, and a Kafka Consumer consuming data

  def main(args: Array[String]) {

    val producer = new ExampleKafkaProducer("newTopic")
    val consumer = new ExampleKafkaConsumer("newTopic")

    var timeouts = 0
    var msgCount = 0

    while (true) {
      // Testing the ExampleKafkaProducer
      msgCount += 1
      val returnFuture = producer.sendWithTimestamp(s"Testing Kafka Producer msg No.: $msgCount");
      val meta = returnFuture.get() // blocking!
      val msgLog =
        s"""
       |Producer publishing data
       |offset    = ${meta.offset()}
       |partition = ${meta.partition()}
       |topic     = ${meta.topic()}
       """.stripMargin
      println(msgLog)

      // Testing the ExampleKafkaConsumer
      val records: ConsumerRecords[String, String] = consumer.consume(100)
      val recordCount = records.count()
      if (recordCount == 0) {
        timeouts = timeouts + 1
      } else {
        println(s"Got $recordCount records after $timeouts timeouts\n")
        timeouts = 0
      }
      val it = records.iterator()
      while (it.hasNext()) {
        val record: ConsumerRecord[String, String] = it.next()
        val offset = record.offset()
        val key = String.valueOf(record.key())
        val value = record.value()
        println("Consumer received data")
        println(s"offset = $offset")
        println(s"value = $value \n")
      }

    }

  }

}
