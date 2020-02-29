package utils

import config.Settings
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkUtils {

    val wlc: Settings.WebLogGen.type = Settings.WebLogGen
    val isWindows: Boolean = System.getProperty("os.name").toLowerCase().contains("win")


    /**
     * Gets or creates a spark context that is OS agnostic with default app name.
     *
     * @return The Spark context.
     */
    def getSparkContext: SparkContext = {
        getSparkContext(wlc.sparkAppName)
    }

    /**
     * Gets or creates a spark context that is OS agnostic.
     *
     * @param appName Name of the app.
     * @return The Spark context.
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

        Logger.getLogger("org").setLevel(Level.WARN)
        Logger.getLogger("akka").setLevel(Level.WARN)

        sc.setLogLevel("WARN")
        sc.setCheckpointDir(checkpointDirectory)
        sc
    }

    /**
     * Returns the sql context.
     *
     * @param sc The spark context.
     * @return The SQL context.
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
     * @param batchDuration The batch duration for the streaming app.
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
