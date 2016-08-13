package com.example.scala.scala_example

import java.util.concurrent.Future
import java.util.Properties
import java.util.Arrays

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerRecords

case class ExampleKafkaConsumer(topic: String) {

  println(s"Consumer connecting to $topic")

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("group.id", "test-consumer-group")
  props.put("enable.auto.commit", "true")
  props.put("auto.commit.interval.ms", "1000")
  props.put("session.timeout.ms", "30000")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  val consumer = new KafkaConsumer[String, String](props)
  consumer.subscribe(Arrays.asList(topic))

  def consume(value: Long): ConsumerRecords[String, String] = {
    consumer.poll(200)
  }

}