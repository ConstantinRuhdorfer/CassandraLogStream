package query

import config.Settings
import utils.SparkUtils.{getSQLContext, getSparkContext}

object QueryJob {

    val wlc: Settings.WebLogGen.type = Settings.WebLogGen

    def main(args: Array[String]): Unit = {

        val sc = getSparkContext
        val sqlContext = getSQLContext(sc)

        /**
         * Equivalent to:
         *
         * use logstreamcassandra;
         * SELECT * FROM masterlogdata
         * WHERE loglevel == 'error' LIMIT 10;
         *
         */
        sqlContext
            .read
            .format("org.apache.spark.sql.cassandra")
            .options(Map("table" -> wlc.defaultMasterLogDataTableName,
                "keyspace" -> wlc.defaultKeySpace))
            .load
            .filter("loglevel == 'error'")
            .show(10)

        /**
         * Equivalent to:
         *
         * use logstreamcassandra;
         * SELECT count(*) FROM masterlogdata;
         *
         */
        print("Number of entries in master database: ")
        println(sqlContext
            .read
            .format("org.apache.spark.sql.cassandra")
            .options(Map("table" -> wlc.defaultMasterLogDataTableName,
                "keyspace" -> wlc.defaultKeySpace))
            .load
            .count)

        /**
         * Equivalent to:
         *
         * use logstreamcassandra;
         * SELECT count(*) FROM masterlogdata
         * WHERE httpversion == 'HTTP/1.0';
         *
         */
        print("Number of visitors with an old HTTP version: ")
        println(sqlContext
            .read
            .format("org.apache.spark.sql.cassandra")
            .options(Map("table" -> wlc.defaultMasterLogDataTableName,
                "keyspace" -> wlc.defaultKeySpace))
            .load
            .filter("httpversion = 'HTTP/1.0'")
            .count)
    }
}
