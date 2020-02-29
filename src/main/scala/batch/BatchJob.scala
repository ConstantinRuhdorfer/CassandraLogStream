package batch

import config.Settings
import utils.SparkUtils.{getSQLContext, getSparkContext}

object BatchJob {

    val wlc: Settings.WebLogGen.type = Settings.WebLogGen

    def main(args: Array[String]): Unit = {

        val sc = getSparkContext
        val sqlContext = getSQLContext(sc)

        val resultDf = sqlContext
            .read
            .format("org.apache.spark.sql.cassandra")
            .options(Map("table" -> wlc.defaultMasterLogDataTableName,
                "keyspace" -> wlc.defaultKeySpace))
            .load
            .filter("loglevel == 'error'")

        resultDf.show(20)
    }
}
