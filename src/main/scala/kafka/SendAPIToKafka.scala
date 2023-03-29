package kafka
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
class SendAPIToKafka {
  def readFromApiAndProduceToKafka(): Unit = {
    import org.apache.spark.sql.SparkSession
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.types._

    val spark = SparkSession.builder()
      .appName("API Reader")
      .master("local[*]")
      .getOrCreate()

    val endpoint = "https://covid-193.p.rapidapi.com/statistics?country=UK"
    val headers = Map(
      "X-RapidAPI-Host" -> "covid-193.p.rapidapi.com",
      "X-RapidAPI-Key" -> "9a99f8903cmsh9e882a8f6e4b0cbp1f2fefjsn53f7c6e89019"
    )

    val options = Map(
      "delimiter" -> ":",
      "url" -> endpoint,
      "header_x_rapidapi" -> s"${headers("X-RapidAPI-Host")}: ${headers("X-RapidAPI-Key")}"
    )

    // Define the schema for the COVID-19 statistics data
    val covidSchema = StructType(Array(
      StructField("continent", StringType),
      StructField("country", StringType),
      StructField("population", LongType),
      StructField("cases", StructType(Array(
        StructField("new", StringType),
        StructField("active", LongType),
        StructField("critical", LongType),
        StructField("recovered", LongType),
        StructField("total", LongType)
      ))),
      StructField("deaths", StructType(Array(
        StructField("new", StringType),
        StructField("total", LongType)
      ))),
      StructField("tests", StructType(Array(
        StructField("total", LongType)
      ))),
      StructField("day", StringType),
      StructField("time", StringType)
    ))

    // Read the data and parse the JSON response into a struct with the specified schema
    val apiData = spark.read
      .format("csv")
      .options(options)
      .load()
      .select(from_json(col("value"), covidSchema).alias("data"))
      .selectExpr("data.*")

    // Write the data to Kafka
    val kafkaData = apiData
      .select(to_json(struct(col("*"))).alias("value"))
      .selectExpr("CAST(value AS STRING)")

    val kafkaSink = kafkaData
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "ip-172-31-3-80.eu-west-2.compute.internal:9092")
      .option("topic", "covid_data")
      .option("checkpointLocation", "/tmp/checkpoints")
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .start()

    kafkaSink.awaitTermination()

  }
}
