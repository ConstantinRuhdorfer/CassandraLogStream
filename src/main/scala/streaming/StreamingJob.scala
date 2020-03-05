package streaming

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.writer.WriteConf
import config.Settings
import domain.{IpByVisitorId, LogByLogId, PageDetailsByPageId}
import domainTypes.{HTTPMethod, HTTPVersion, LogLevel}
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
     * @param sc            The current spark context.
     * @param batchDuration The batch duration for the streaming context.
     * @return
     */
    private def streamingApp(sc: SparkContext, batchDuration: Duration): StreamingContext = {

        val ssc = new StreamingContext(sc, batchDuration)
        val inputPath = "src/main/resources/input/"
        val writeConf = WriteConf(ifNotExists = true)

        implicit val c: CassandraConnector = getOrCreateCassandraConnector(sc)

        // Writes the logs.
        val textDStream = ssc.textFileStream(inputPath)
        textDStream
            .transform(transformStream(_, mapToLogByLogId))
            .foreachRDD(
                _.saveToCassandra(wlc.defaultKeySpace, wlc.defaultMasterLogDataTableName,
                    AllColumns, writeConf = writeConf)
            )

        textDStream
            .transform(transformStream(_, mapToPageDetailsByPageId))
            .foreachRDD(
                _.saveToCassandra(wlc.defaultKeySpace, wlc.defaultPageDetailsByPageId,
                    AllColumns, writeConf = writeConf)
            )

        textDStream
            .transform(transformStream(_, mapToIpByVisitorId))
            .foreachRDD(
                _.saveToCassandra(wlc.defaultKeySpace, wlc.defaultIPsByVisitorId,
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
            if (record.length == 8)
                Some(mapToDomain(record))
            else
                None
        }
    }

    /**
     * Maps an Array[String] to an LogByLogId.
     *
     * @param record The array.
     * @return LogDataPoint
     */
    private def mapToLogByLogId(record: Array[String]): LogByLogId = {
        LogByLogId(
            record(0).toLong,
            record(4),
            record(1),
            record(2),
            LogLevel.customWithName(record(7)),
            HTTPMethod.customWithName(record(3)),
            HTTPVersion.customWithName(record(5)),
            record(6).toInt)
    }

    /**
     * Maps an Array[String] to an PageDetailsByPageId.
     *
     * @param record The array.
     * @return LogDataPoint
     */
    private def mapToPageDetailsByPageId(record: Array[String]): PageDetailsByPageId = {
        PageDetailsByPageId(
            record(4),
            record(4).split("/").last,
            record(4).split("/")(1))
    }

    /**
     * Maps an Array[String] to an IpByVisitorId.
     *
     * @param record The array.
     * @return LogDataPoint
     */
    private def mapToIpByVisitorId(record: Array[String]): IpByVisitorId = {
        IpByVisitorId(
            record(1),
            record(0).toLong,
            record(2))
    }
}
