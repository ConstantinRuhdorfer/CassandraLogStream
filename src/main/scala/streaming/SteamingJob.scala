package streaming

import domain.LogDataPoint
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import utils.SparkUtils._
import com.datastax.spark.connector.streaming._

object SteamingJob extends App {

    // setup spark context
    val sc = getSparkContext("Lambda with Spark")
    val sqlContext = getSQLContext(sc)


    val batchDuration = Seconds(4)

    val ssc = getStreamingContext(streamingApp, sc, batchDuration)

    ssc.start()
    ssc.awaitTermination()

    /**
     * The actual streaming application.
     *
     * @param sc The current spark context.
     * @param batchDuration The batch duration for the streaming context.
     * @return
     */
    def streamingApp(sc: SparkContext, batchDuration: Duration): StreamingContext = {

        val ssc = new StreamingContext(sc, batchDuration)

        val inputPath = "src/main/resources/input/"

        val textDStream = ssc.textFileStream(inputPath)
        val logStream = textDStream.transform(input => {
            input.flatMap { line =>
                val record = line.split(";")
                val MS_IN_HOUR = 1000 * 60 * 60

                if (record.length == 6)
                    Some(LogDataPoint(record(0).toLong / MS_IN_HOUR * MS_IN_HOUR,
                        record(1),
                        record(2),
                        record(3),
                        record(4).toInt,
                        record(5)))
                else
                    None
            }
        }).cache()

        logStream.saveToCassandra("LogStreamCassandra", "masterlogdata")

        ssc
    }
}
