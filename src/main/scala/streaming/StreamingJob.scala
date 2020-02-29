package streaming

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.writer.WriteConf
import domain.{LogDataPoint, PageView}
import domainTypes.{HTTPMethod, HTTPVersion}
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

    println("Streaming data ...")
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

        // Writes the logs.
        val textDStream = ssc.textFileStream(inputPath)
        textDStream.transform(input => {
            input.flatMap { line =>
                val record = line.split(";")
                if (record.length == 9)
                    Some(LogDataPoint(
                        record(0),
                        record(1).toLong,
                        record(2),
                        record(3),
                        HTTPMethod.customWithName(record(4)),
                        record(5),
                        HTTPVersion.customWithName(record(6)),
                        record(7).toInt,
                        record(8)))
                else
                    None
            }
        }).foreachRDD(rdd => {
            rdd.saveToCassandra(s"""${wlc.defaultKeySpace}""",
                s"""${wlc.defaultMasterLogDataTableName}""",
                AllColumns, writeConf = writeConf)
        })

        // Writes the page infos with view.
        textDStream.transform(input => {
            input.flatMap { line =>
                val record = line.split(";")
                if (record.length == 9)
                    Some(PageView(
                        record(5).split("/").last,
                        record(5),
                        record(1).toLong,
                        record(2),
                        record(3)))
                else
                    None
            }
        }).foreachRDD(rdd => {
            rdd.saveToCassandra(s"""${wlc.defaultKeySpace}""",
                s"""${wlc.defaultPageViewTableName}""",
                AllColumns, writeConf = writeConf)
        })

        ssc
    }
}
