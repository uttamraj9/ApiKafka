package kafka

import org.apache.kafka.clients.producer._
import org.apache.log4j.PropertyConfigurator
import scalaj.http._

import java.util.Properties
class SendAPIToKafka {
  val logProps = new Properties()
  logProps.load(getClass.getClassLoader.getResourceAsStream("log4j.properties"))
  PropertyConfigurator.configure(logProps)
  def readFromApiAndProduceToKafka(topic: String, brokers: String, headers: Map[String, String], url: String, country: String): Unit = {
    val response = Http(url)
      .headers(headers)
      .param("country", country)
      .asString

    if (response.isSuccess) {
      val data = response.body

      val props = new Properties()
      props.put("bootstrap.servers", brokers)
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

      val producer = new KafkaProducer[String, String](props)

      val message = new ProducerRecord[String, String](topic, data)

      producer.send(message)

      producer.close()
    } else {
      println(s"Error: ${response.code} ${response.statusLine}")
    }
  }
}
