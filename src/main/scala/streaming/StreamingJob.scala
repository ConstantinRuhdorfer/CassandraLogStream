package streaming

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.writer.WriteConf
import domain.LogDataPoint
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import utils.CassandraUtils._
import utils.SparkUtils._

import scala.language.postfixOps

object StreamingJob extends App {

    // setup spark context
    val sc = getSparkContext("LogStreamCassandra")
    val sqlContext = getSQLContext(sc)

    createCassandraSetupIfNotExists(sc)

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
        val writeConf = WriteConf(ifNotExists = true)

        implicit val c: CassandraConnector = getOrCreateCassandraConnector(sc)

        val textDStream = ssc.textFileStream(inputPath)
        textDStream.transform(input => {
            input.flatMap { line =>

                val record = line.split(";")

                if (record.length == 6)
                    Some(LogDataPoint(record(0).toLong,
                        record(1),
                        record(2),
                        record(3),
                        record(4).toInt,
                        record(5)))
                else
                    None
            }
        }).foreachRDD(rdd => {
            rdd.saveToCassandra("logstreamcassandra", "masterlogdata",
                AllColumns, writeConf = writeConf)
        })

        ssc
    }
}
