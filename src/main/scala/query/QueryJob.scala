package query

import config.Settings
import utils.CassandraUtils.getOrCreateCassandraConnector
import utils.SparkUtils.{getSQLContext, getSparkContext}

object QueryJob {

    val wlc: Settings.WebLogGen.type = Settings.WebLogGen

    def main(args: Array[String]): Unit = {

        // Setup...
        val sc = getSparkContext
        val sqlContext = getSQLContext(sc)
        val session = getOrCreateCassandraConnector(sc).openSession()

        session.execute("use logstreamcassandra;")
        /*session.execute(
            s"""SELECT * FROM masterlogdata
               |WHERE loglevel = 'error'
               |LIMIT 10
               |ALLOW FILTERING;""".stripMargin)
            .all
            .asScala
            .foreach(println)

        Thread sleep 2000

        print("Number of entries in master database: ")
        session.execute(
            s"""SELECT COUNT(*) FROM masterlogdata;""".stripMargin)
            .all
            .asScala
            .foreach(println)

        Thread sleep 2000

        print("Number of visits with an old HTTP version: ")
        session.execute(
            s"""SELECT COUNT(*) FROM masterlogdata
               |WHERE httpversion = 'HTTP/1.0'
               |ALLOW FILTERING;""".stripMargin)
            .all
            .asScala
            .foreach(println)

        Thread sleep 2000

        /**
         * Alternative way of accessing cassandra using the cassandra-spark-connector:
         *
         * SELECT pagepath, COUNT(visitorid) AS num_visitor
         * FROM pageviews
         * GROUP BY pagepath;
         */
        import sqlContext.implicits._
        sqlContext
            .read
            .format("org.apache.spark.sql.cassandra")
            .options(Map("table" -> "pageviews", "keyspace" -> "logstreamcassandra"))
            .load
            .groupBy("pagepath")
            .agg(
                count("visitorid").alias("num_visitor")
            )
            .sort($"num_visitor".desc)
            .show(10)

        session.close()*/
    }
}
