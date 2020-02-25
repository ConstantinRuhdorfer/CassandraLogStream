package batch

import utils.SparkUtils.{getSQLContext, getSparkContext}

object BatchJob {

    def main(args: Array[String]): Unit = {

        val sc = getSparkContext("LogStreamCassandra")
        val sqlContext = getSQLContext(sc)

        val resultDf = sqlContext
            .read
            .format("org.apache.spark.sql.cassandra")
            .options(Map("table" -> "masterlogdata", "keyspace" -> "logstreamcassandra"))
            .load
            .filter("loglevel == 'error'")

        resultDf.show(20)
    }
}
