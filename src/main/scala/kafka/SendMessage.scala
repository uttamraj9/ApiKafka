package kafka

object SendMessage {
  def main(args: Array[String]): Unit = {
    val topic = "topic1"
    val brokers = "ip-172-31-3-80.eu-west-2.compute.internal:9092,ip-172-31-5-217.eu-west-2.compute.internal:9092"
    val headers = Map("X-RapidAPI-Host" -> "covid-193.p.rapidapi.com", "X-RapidAPI-Key" -> "9a99f8903cmsh9e882a8f6e4b0cbp1f2fefjsn53f7c6e89019")
    val url = "https://covid-193.p.rapidapi.com/statistics?country=uk"
    val sc = new SendAPIToKafka()
    sc.readFromApiAndProduceToKafka(topic, brokers, headers, url)
    println("Test of jenkins webhook completed11111222iufgihdfighfdg")
    println("Test of jenkins webhook completed11111222iufgihdfighfdg")
  }
}
