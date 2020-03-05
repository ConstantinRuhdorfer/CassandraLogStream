package utils

import com.datastax.driver.core.exceptions._
import com.datastax.spark.connector.cql.CassandraConnector
import config.Settings
import org.apache.spark.SparkContext

object CassandraUtils {

    val wlc: Settings.WebLogGen.type = Settings.WebLogGen
    private val cassandraConnector: Option[CassandraConnector] = None

    /**
     * Creates the cassandra table and keyspace setup that is needed for this project.
     *
     * @param sc The spark context.
     */
    def createCassandraSetupIfNotExists(sc: SparkContext): Unit = {
        createCassandraSetupIfNotExists(sc,
            wlc.defaultKeySpace,
            wlc.defaultMasterLogDataTableName,
            wlc.defaultPageDetailsByPageId,
            wlc.defaultIPsByVisitorId)
    }

    /**
     * Creates the cassandra table and keyspace setup that is needed for this project.
     *
     * @param sc       The spark context.
     * @param keySpace A keyspace name. Should default to defaultKeySpace.
     */
    def createCassandraSetupIfNotExists(sc: SparkContext,
                                        keySpace: String,
                                        masterLogDataTableName: String,
                                        pageDetailsByPageIdTableName: String,
                                        iPsByVisitorIdTableName: String): Unit = {

        val session = getOrCreateCassandraConnector(sc).openSession()

        try {
            // NOTE: For development only
            session.execute(
                s"""
                   |DROP KEYSPACE IF EXISTS $keySpace;
                   |""".stripMargin)

            session.execute(
                s"""
                   |CREATE KEYSPACE $keySpace
                   |WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
                   |""".stripMargin)

            session.execute(
                s"""
                   |USE $keySpace;
                   |""".stripMargin)

            session.execute(
                s"""
                   |CREATE TABLE IF NOT EXISTS $masterLogDataTableName (
                   |timestamp bigint,
                   |pageid text,
                   |visitorid text,
                   |ip text,
                   |loglevel text,
                   |httpmethod text,
                   |httpversion text,
                   |statuscode int,
                   |PRIMARY KEY((timestamp, pageid, visitorid))
                   |);
                   |""".stripMargin)

            session.execute(
                s"""
                   |CREATE MATERIALIZED VIEW
                   |IF NOT EXISTS logs_by_loglevel
                   |AS SELECT timestamp, pageid, visitorid, loglevel
                   |FROM $masterLogDataTableName
                   |WHERE timestamp IS NOT NULL
                   |AND pageid IS NOT NULL
                   |AND visitorid IS NOT NULL
                   |AND loglevel IS NOT NULL
                   |PRIMARY KEY ((loglevel), timestamp, pageid, visitorid);
                   |""".stripMargin)

            session.execute(
                s"""
                   |CREATE MATERIALIZED VIEW
                   |IF NOT EXISTS logs_by_timestamp
                   |AS SELECT timestamp, pageid, visitorid
                   |FROM $masterLogDataTableName
                   |WHERE timestamp IS NOT NULL
                   |AND pageid IS NOT NULL
                   |AND visitorid IS NOT NULL
                   |PRIMARY KEY ((timestamp), visitorid, pageid);
                   |""".stripMargin)

            session.execute(
                s"""
                   |CREATE MATERIALIZED VIEW
                   |IF NOT EXISTS visitors_by_pageid
                   |AS SELECT pageid, visitorid, timestamp
                   |FROM $masterLogDataTableName
                   |WHERE pageid IS NOT NULL
                   |AND timestamp IS NOT NULL
                   |AND visitorid IS NOT NULL
                   |PRIMARY KEY ((pageid), visitorid, timestamp);
                   |""".stripMargin)

            session.execute(
                s"""
                   |CREATE MATERIALIZED VIEW
                   |IF NOT EXISTS viewed_pages_by_visitorid
                   |AS SELECT pageid, visitorid, timestamp
                   |FROM $masterLogDataTableName
                   |WHERE pageid IS NOT NULL
                   |AND timestamp IS NOT NULL
                   |AND visitorid IS NOT NULL
                   |PRIMARY KEY ((visitorid), pageid, timestamp);
                   |""".stripMargin)

            session.execute(
                s"""
                   |CREATE TABLE
                   |IF NOT EXISTS $pageDetailsByPageIdTableName (
                   |pageid text,
                   |pagename text,
                   |service text,
                   |PRIMARY KEY((pageid))
                   |);
                   |""".stripMargin)

            session.execute(
                s"""
                   |CREATE TABLE
                   |IF NOT EXISTS $iPsByVisitorIdTableName (
                   |visitorid text,
                   |ip text,
                   |timestamp bigint,
                   |PRIMARY KEY((visitorid), timestamp, ip)
                   |);
                   |""".stripMargin)
        }
        catch {
            case _: InvalidQueryException => println("Query was invalid. Please check the table configuration.")
        }

        session.close()
    }

    /**
     * Given the spark context gets or returns a Cassandra Connector.
     * Avoids more than one connection to the cluster.
     *
     * @param sc The spark context.
     * @return The CassandraConnector.
     */
    def getOrCreateCassandraConnector(sc: SparkContext): CassandraConnector = {
        cassandraConnector match {
            case Some(cassandraConnector) => cassandraConnector
            case _ => CassandraConnector.apply(sc.getConf)
        }
    }
}

