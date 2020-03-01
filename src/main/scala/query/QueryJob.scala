package query

import config.Settings
import utils.CassandraUtils.getOrCreateCassandraConnector
import utils.SparkUtils.{getSQLContext, getSparkContext}

import scala.collection.JavaConverters._

object QueryJob {

    val wlc: Settings.WebLogGen.type = Settings.WebLogGen

    def main(args: Array[String]): Unit = {

        val sc = getSparkContext
        val sqlContext = getSQLContext(sc)
        val session = getOrCreateCassandraConnector(sc).openSession()

        session.execute("use logstreamcassandra;")
        session.execute(
            s"""SELECT * FROM masterlogdata
               |WHERE loglevel = 'error'
               |LIMIT 10
               |ALLOW FILTERING;""".stripMargin)
            .all
            .asScala
            .foreach(println)

        print("Number of entries in master database: ")
        session.execute(
            s"""SELECT count(*) FROM masterlogdata;""".stripMargin)
            .all
            .asScala
            .foreach(println)

        print("Number of visits with an old HTTP version: ")
        session.execute(
            s"""SELECT count(*) FROM masterlogdata
               |WHERE httpversion = 'HTTP/1.0'
               |ALLOW FILTERING;""".stripMargin)
            .all
            .asScala
            .foreach(println)

        session.close()
    }
}
