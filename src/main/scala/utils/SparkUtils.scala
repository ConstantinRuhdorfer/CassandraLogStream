package utils

import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkUtils {

    val isWindows: Boolean = System.getProperty("os.name").toLowerCase().contains("win")

    /**
     * Creates or a spark context that is OS agnostic.
     *
     * @param appName Name of the app.
     * @return
     */
    def getSparkContext(appName: String): SparkContext = {
        var checkpointDirectory = ""

        // get spark configuration
        val conf = new SparkConf()
            .setAppName(appName)
            .set("spark.casandra.connection.host", "localhost")


        if (isWindows)
            System.setProperty("hadoop.home.dir", "C:\\libraries\\WinUtils")

        conf.setMaster("local[*]")
        checkpointDirectory = "src/main/resources/checkpointDir/"

        // setup spark context
        val sc = SparkContext.getOrCreate(conf)
        sc.setCheckpointDir(checkpointDirectory)
        sc
    }

    /**
     * Returns the sql context.
     *
     * @param sc The spark context.
     * @return
     */
    def getSQLContext(sc: SparkContext): SQLContext = {
        val sqlContext = SQLContext.getOrCreate(sc)
        sqlContext
    }

    /**
     * Given a streaming app function executes that,
     * sets checkpoints and returns the streaming context.
     *
     * @param streamingApp  The function to execute.
     * @param sc            The spark context.
     * @param batchDuration The batch duration for the straming app.
     * @return The Streaming context.
     */
    def getStreamingContext(streamingApp: (SparkContext, Duration)
        => StreamingContext, sc: SparkContext, batchDuration: Duration): StreamingContext = {

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
