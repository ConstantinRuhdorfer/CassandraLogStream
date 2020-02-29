package streaming

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.writer.WriteConf
import config.Settings
import domain.{LogDataPoint, PageView, Visitor}
import domainTypes.{HTTPMethod, HTTPVersion}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import utils.CassandraUtils._
import utils.SparkUtils._

import scala.language.postfixOps
import scala.reflect.ClassTag

object StreamingJob extends App {

    val wlc: Settings.WebLogGen.type = Settings.WebLogGen

    // setup spark context
    val sc = getSparkContext
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
        textDStream
            .transform(transformStream(_, mapToLogData))
            .foreachRDD(
                _.saveToCassandra(wlc.defaultKeySpace, wlc.defaultMasterLogDataTableName,
                    AllColumns, writeConf = writeConf)
            )

        // Writes the page views table.
        textDStream
            .transform(transformStream(_, mapToPageView))
            .foreachRDD(
                _.saveToCassandra(wlc.defaultKeySpace, wlc.defaultPageViewTableName,
                    AllColumns, writeConf = writeConf)
            )

        // Writes the visitors table.
        textDStream
            .transform(transformStream(_, mapToVisitor))
            .foreachRDD(
                _.saveToCassandra(wlc.defaultKeySpace, wlc.defaultVisitorTableName,
                    AllColumns, writeConf = writeConf)
            )

        ssc
    }

    /**
     * Takes a stream and transforms it to an RDD of type T.
     *
     * @param input       The starting RDD.
     * @param mapToDomain A function that maps a line of Array[String] to type T.
     * @tparam T Type of the resulting rdd.
     * @return RDD[T].
     */
    private def transformStream[T: ClassTag](input: RDD[String],
                                             mapToDomain: Array[String] => T): RDD[T] = {
        input.flatMap { line =>
            val record = line.split(";")
            if (record.length == 9)
                Some(mapToDomain(record))
            else
                None
        }
    }

    /**
     * Maps an Array[String] to an LogDataPoint.
     *
     * @param record The array.
     * @return LogDataPoint
     */
    private def mapToLogData(record: Array[String]): LogDataPoint = {
        LogDataPoint(
            record(0),
            record(1).toLong,
            record(2),
            record(3),
            HTTPMethod.customWithName(record(4)),
            record(5),
            HTTPVersion.customWithName(record(6)),
            record(7).toInt,
            record(8))
    }

    /**
     * Maps an Array[String] to an PageView.
     *
     * @param record The array.
     * @return PageView
     */
    private def mapToPageView(record: Array[String]): PageView = {
        PageView(
            record(5).split("/").last,
            record(5),
            record(1).toLong,
            record(2),
            record(3))
    }

    /**
     * Maps an Array[String] to an Visitor.
     *
     * @param record The array.
     * @return Visitor
     */
    private def mapToVisitor(record: Array[String]): Visitor = {
        Visitor(
            record(2),
            record(3),
            record(1).toLong,
            record(5))
    }
}
