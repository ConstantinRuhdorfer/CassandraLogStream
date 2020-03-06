package query

import com.datastax.driver.core.Session
import config.Settings
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import utils.CassandraUtils.getOrCreateCassandraConnector
import utils.SparkUtils.{getSQLContext, getSparkContext}

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
     * Executes a list of queries against cassandra.
     */
    def executeStats(): Unit = {

        session.execute("use logstreamcassandra;")

        session.close()
    }
}
