package com.example.scala.kafka_pubsub

import java.util.concurrent.Future
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

case class ExampleKafkaProducer (topic: String) {
  
  println(s"Connecting to $topic")
  val props = new java.util.Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("client.id", "KafkaProducer")
  props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  
  def sendWithTimestamp(value: String): Future[RecordMetadata] = {
    val producer = new KafkaProducer[Integer, String](props)
    val polish = java.time.format.DateTimeFormatter.ofPattern("dd.MM.yyyy H:mm:ss")
    val now = java.time.LocalDateTime.now().format(polish)
    val record = new ProducerRecord[Integer, String](topic, 1, value + s" at $now")
    val returnFuture = producer.send(record)
    producer.close()
    returnFuture
  }

  
  

  


}