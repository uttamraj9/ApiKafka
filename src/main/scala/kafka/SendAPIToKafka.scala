package kafka

import com.github.blemale.requests._
import com.github.blemale.requests.dsl._
import java.util.Properties
class SendAPIToKafka {
  def readFromApiAndProduceToKafka(): Unit = {
    val headers = Map(
      "X-RapidAPI-Host" -> "covid-193.p.rapidapi.com",
      "X-RapidAPI-Key" -> "9a99f8903cmsh9e882a8f6e4b0cbp1f2fefjsn53f7c6e89019"
    )

    val response = get("https://covid-193.p.rapidapi.com/statistics")
      .params("country" -> "UK")
      .headers(headers)
      .send()

    println(response.text)
  }
}
