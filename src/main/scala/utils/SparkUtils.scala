package utils

import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkUtils {

    def getSparkContext(appName: String): SparkContext = {
        var checkpointDirectory = ""

        // get spark configuration
        val conf = new SparkConf()
            .setAppName(appName)
            .set("spark.casandra.connection.host", "localhost")


        conf.setMaster("local[*]")
        checkpointDirectory = "src/main/resources/checkpointDir/"

        // setup spark context
        val sc = SparkContext.getOrCreate(conf)
        sc.setCheckpointDir(checkpointDirectory)
        sc
    }

    def getSQLContext(sc: SparkContext): SQLContext = {
        val sqlContext = SQLContext.getOrCreate(sc)
        sqlContext
    }

    def getStreamingContext(streamingApp : (SparkContext, Duration)
            => StreamingContext, sc : SparkContext, batchDuration: Duration): StreamingContext = {

        val creatingFunc = () => streamingApp(sc, batchDuration)
        val ssc = sc.getCheckpointDir match {
            case Some(checkpointDir) => StreamingContext
                .getActiveOrCreate(checkpointDir,
                    creatingFunc,
                    sc.hadoopConfiguration,
                    createOnError = true)
            case None => StreamingContext.getActiveOrCreate(creatingFunc)
        }
        sc.getCheckpointDir.foreach(cp => ssc.checkpoint(cp))
        ssc
    }
}
