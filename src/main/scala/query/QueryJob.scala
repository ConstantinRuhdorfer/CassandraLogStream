package query

import com.datastax.driver.core.{Row, Session}
import config.Settings
import domainTypes.LogLevel
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import utils.CassandraUtils.getOrCreateCassandraConnector
import utils.SparkUtils.{getSQLContext, getSparkContext}

import scala.collection.JavaConverters._
import scala.collection.mutable

object QueryJob {

    val wlc: Settings.WebLogGen.type = Settings.WebLogGen

    val sc: SparkContext = getSparkContext
    val sqlContext: SQLContext = getSQLContext(sc)
    val session: Session = getOrCreateCassandraConnector(sc).openSession()

    var usedIps: Int = 0

    /**
     * Determines if to execute the queries.
     *
     * @param numberOfIps Number of current Ips.
     */
    def trigger(numberOfIps: Int): Unit = {

        val difference = numberOfIps - usedIps

        if (difference > 5000) {

            executeStats()
            usedIps += difference
        }
        else {
            println("Skip.")
        }
    }

    /**
     * Executes a list of queries against
     * cassandra for presentation purposes.
     */
    def executeStats(): Unit = {

        session.execute("USE logstreamcassandra;")

        val q1: mutable.Seq[Row] = session
            .execute(
                s"""
                   |SELECT * FROM logs_by_timestamp
                   |LIMIT 10;
                   |""".stripMargin)
            .all
            .asScala

        val timestamp: Long = q1.head.getLong(0)
        val q2: mutable.Seq[Row] = session
            .execute(
                s"""
                   |SELECT * FROM logs_by_timestamp
                   |WHERE timestamp = $timestamp
                   |LIMIT 20;
                   |""".stripMargin)
            .all
            .asScala

        val q3: mutable.Seq[Row] = session
            .execute(
                s"""
                   |SELECT * FROM logs_by_loglevel
                   |WHERE loglevel = '${LogLevel.ERROR}'
                   |LIMIT 20;
                   |""".stripMargin)
            .all
            .asScala

        val someVisitorWhoCausedAnError = q3.head.getString(3)
        val q4: mutable.Seq[Row] = session
            .execute(
                s"""
                   |SELECT * FROM ips_by_visitorid
                   |WHERE visitorid = '$someVisitorWhoCausedAnError';
                   |""".stripMargin)
            .all
            .asScala


        val q5: mutable.Seq[Row] = session
            .execute(
                s"""
                   |SELECT * FROM viewed_pages_by_visitorid
                   |WHERE visitorid = '$someVisitorWhoCausedAnError';
                   |""".stripMargin)
            .all
            .asScala

        val viewdPageBySomeVisitorWhoCausedAnError = q5.head.getString(2)
        val q6: mutable.Seq[Row] = session
            .execute(
                s"""
                   |SELECT * FROM page_details_by_pageid
                   |WHERE pageid = '$viewdPageBySomeVisitorWhoCausedAnError';
                   |""".stripMargin)
            .all
            .asScala

        val q7: mutable.Seq[Row] = session
            .execute(
                s"""
                   |SELECT visitorid FROM visitors_by_pageid
                   |WHERE pageid = '$viewdPageBySomeVisitorWhoCausedAnError';
                   |""".stripMargin)
            .all
            .asScala

        // TODO: Correct
        println(q1)
        println(q2)
        println(q3)
        println(q4)
        println(q5)
        println(q6)
        println(q7)

        session.close()
    }
}
