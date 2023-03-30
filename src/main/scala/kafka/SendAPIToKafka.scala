package kafka
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import requests._


class SendAPIToKafka {
  def readFromApiAndProduceToKafka(): Unit = {
    val spark = SparkSession.builder()
      .appName("MyApp")
      .master("local[*]") // Use all available cores on the local machine
      .getOrCreate()
    import spark.implicits._
    val apiUrl = "https://covid-193.p.rapidapi.com/statistics?country=UK"
    val headers = Map(
      "X-RapidAPI-Host" -> "covid-193.p.rapidapi.com",
      "X-RapidAPI-Key" -> "9a99f8903cmsh9e882a8f6e4b0cbp1f2fefjsn53f7c6e89019"
    )
    val response = get(apiUrl, headers = headers)
    val total = response.text()
    val dfFromText = spark.read.json(Seq(total).toDS)
    val kafka_msg = dfFromText.select(to_json(struct($"response"))).toDF("value")

    kafka_msg.selectExpr("value").write.format("kafka").option("kafka.bootstrap.servers", "ip-172-31-3-80.eu-west-2.compute.internal:9092").option("topic", "topic1").save()

  }
}