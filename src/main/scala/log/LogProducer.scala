package log

import java.io.FileWriter

import config.Settings
import domainTypes.HTTPMethod.HTTPMethod
import domainTypes.{HTTPMethod, HTTPStatusCode, HTTPVersion, LogLevel}
import org.apache.commons.io.FileUtils

import scala.io.Source.fromInputStream
import scala.util.Random

object LogProducer extends App {

    val wlc = Settings.WebLogGen

    val LogIPAddresses = getLinesInFile("/LogIPAddresses.csv")
    val LogPages = getLinesInFile("/LogPages.csv")
    val Visitors = (0 to wlc.visitors).map("Visitor-" + _)

    val rnd = new Random()

    for (_ <- 1 to wlc.numberOfFiles) {

        val fw = new FileWriter(wlc.filePath, true)
        val incrementTimeEvery = rnd.nextInt(wlc.records - 1) + 1
        var timestamp = System.currentTimeMillis()

        for (iteration <- 1 to wlc.records) {

            timestamp = System.currentTimeMillis()
            val line = generateNewDataPoint(iteration, timestamp)
            fw.write(line)

            if (iteration % incrementTimeEvery == 0) {
                println(s"Sent $iteration messages!")
                val sleeping = rnd.nextInt(incrementTimeEvery * 10)
                println(s"Sleeping for $sleeping ms")
                Thread sleep sleeping
            }
        }
        fw.close()

        val outputFile = FileUtils.getFile(s"${wlc.destPath}data_$timestamp")
        println(s"Moving produced data to $outputFile")
        FileUtils.moveFile(FileUtils.getFile(wlc.filePath), outputFile)
        val sleeping = 5000
        println(s"Sleeping for $sleeping ms")
    }

    /**
     * Generates a new data point for the fake log messages.
     *
     * @param iteration The current iteration for pseudo randomness.
     * @param timestamp The current timestamp.
     * @return The new data point.
     */
    private def generateNewDataPoint(iteration: Int, timestamp: Long): String = {

        val statusCode = iteration % (rnd.nextInt(100) + 1) match {
            case 0 => HTTPStatusCode.INTERNAL_SERVER_ERROR
            case 1 => HTTPStatusCode.NOT_FOUND
            case 2 => HTTPStatusCode.FORBIDDEN
            case 3 => HTTPStatusCode.CREATED
            case _ => HTTPStatusCode.OK
        }

        val logLevel = statusCode match {
            case HTTPStatusCode.INTERNAL_SERVER_ERROR => LogLevel.ERROR
            case HTTPStatusCode.NOT_FOUND => LogLevel.WARNING
            case HTTPStatusCode.FORBIDDEN => LogLevel.WARNING
            case HTTPStatusCode.CREATED => LogLevel.EVENT
            case _ =>
                iteration % (rnd.nextInt(3) + 1) match {
                    case 1 => LogLevel.DEBUG
                    case _ => LogLevel.INFO
                }
        }

        val httpVersion = iteration % (rnd.nextInt(3) + 1) match {
            case 0 => HTTPVersion.HTTP1
            case 1 => HTTPVersion.HTTP1_1
            case _ => HTTPVersion.HTTP2
        }

        implicit val iter: Int = iteration
        val httpMethod = statusCode match {
            case HTTPStatusCode.INTERNAL_SERVER_ERROR => getRandomHTTPMethod
            case HTTPStatusCode.NOT_FOUND => getRandomHTTPMethod
            case HTTPStatusCode.FORBIDDEN => getRandomHTTPMethod
            case HTTPStatusCode.CREATED => HTTPMethod.PUT
            case _ => get200CompatibleHTTPMethod
        }

        val visitor = Visitors(rnd.nextInt(Visitors.length - 1))
        val pagePath = LogPages(rnd.nextInt(1000 - 1))
        val ip = LogIPAddresses(rnd.nextInt(500 - 1))

        s"$timestamp;$visitor;$ip;$httpMethod;$pagePath;$httpVersion;$statusCode;$logLevel\n"
    }

    /**
     * Given a path to a csv file returns its content as a array string.
     *
     * @param filePath The path.
     * @return The array.
     */
    private def getLinesInFile(filePath: String): Array[String] = {
        fromInputStream(getClass.getResourceAsStream(filePath))
            .getLines()
            .toArray
    }

    /**
     * Get a random HTTP Method that is compatible with a code 200.
     *
     * @param iteration The current iteration for pseudo randomness.
     * @return The HTTPMethod
     */
    private def get200CompatibleHTTPMethod(implicit iteration: Int): HTTPMethod = {
        iteration % (rnd.nextInt(6) + 1) match {
            case 1 => HTTPMethod.HEAD
            case 2 => HTTPMethod.DELETE
            case _ => HTTPMethod.GET
        }
    }

    /**
     * Get some HTTP Method were GET is 5 more likely than any other
     *
     * @param iteration The current iteration for pseudo randomness.
     * @return The HTTPMethod
     */
    private def getRandomHTTPMethod(implicit iteration: Int): HTTPMethod = {
        iteration % (rnd.nextInt(10) + 1) match {
            case 1 => HTTPMethod.HEAD
            case 2 => HTTPMethod.DELETE
            case 3 => HTTPMethod.PATCH
            case 4 => HTTPMethod.POST
            case 5 => HTTPMethod.PUT
            case _ => HTTPMethod.GET
        }
    }
}
