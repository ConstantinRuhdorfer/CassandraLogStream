package utils

import com.datastax.driver.core.ResultSet
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
            wlc.defaultPageViewTableName)
    }

    /**
     * Creates the cassandra table and keyspace setup that is needed for this project.
     *
     * @param sc       The spark context.
     * @param keySpace A keyspace name. Should default to defaultKeySpace.
     */
    def createCassandraSetupIfNotExists(sc: SparkContext, keySpace: String,
                                        masterLogDataTableName: String, pageViewTableName: String): Unit = {

        val session = getOrCreateCassandraConnector(sc).openSession()

        val result: ResultSet = session.execute(
            s"""SELECT * FROM system_schema.keyspaces WHERE keyspace_name='$keySpace';
            """.stripMargin)


        if (result.iterator().hasNext) {
            // NOTE: For development only
            session.execute(s"""DROP KEYSPACE $keySpace;""")

        }

        session.execute(
            s"""CREATE KEYSPACE $keySpace
               |WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
                """.stripMargin)

        session.execute(s"""use $keySpace;""")

        try {
            session.execute(s"""SELECT * FROM $masterLogDataTableName;""")
        }
        catch {
            case _: InvalidQueryException =>
                session.execute(
                    s"""
                       |CREATE TABLE $masterLogDataTableName(
                       |id text,
                       |timestamp bigint,
                       |visitor text,
                       |ip text,
                       |httpmethod text,
                       |page text,
                       |httpversion text,
                       |statusCode int,
                       |loglevel text,
                       |PRIMARY KEY(id, timestamp));
                       |""".stripMargin)
        }
        try {
            session.execute(s"""SELECT * FROM $pageViewTableName;""")
        }
        catch {
            case _: InvalidQueryException =>
                session.execute(
                    s"""
                       |CREATE TABLE $pageViewTableName(
                       |pagename text,
                       |pagepath text,
                       |timestamp bigint,
                       |visitorid text,
                       |visitorip text,
                       |PRIMARY KEY(pagepath, timestamp, visitorid));
                       |""".stripMargin)
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

