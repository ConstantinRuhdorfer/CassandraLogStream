package log

import java.io.FileWriter

import config.Settings
import org.apache.commons.io.FileUtils

import scala.io.Source.fromInputStream
import scala.util.Random

object LogProducer extends App {

    val wlc = Settings.WebLogGen

    val LogIPAddresses = getLinesInFile("/LogIPAddresses.csv")
    val LogMessages = getLinesInFile("/LogMessages.csv")
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
     * @return
     */
    def generateNewDataPoint(iteration: Int, timestamp: Long): String = {

        val statusCode = iteration % (rnd.nextInt(200) + 1) match {
            case 0 => "500"
            case 1 => "404"
            case 2 => "403"
            case 3 => "201"
            case _ => "200"
        }

        val logLevel = statusCode match {
            case "500" => "error"
            case "404" => "warning"
            case "403" => "warning"
            case "201" => "event"
            case _ => iteration % (rnd.nextInt(3) + 1) match {
                case 1 => "debug"
                case _ => "info"
            }
        }

        val id = java.util.UUID.randomUUID().toString
        val visitor = Visitors(rnd.nextInt(Visitors.length - 1))
        val message = LogMessages(rnd.nextInt(2000 - 1))
        val ip = LogIPAddresses(rnd.nextInt(500 - 1))

        s"$id;$timestamp;$visitor;$ip;$message;$statusCode;$logLevel\n"
    }

    /**
     * Given a path to a csv file returns its content as a array string.
     *
     * @param filePath The path.
     * @return The array.
     */
    def getLinesInFile(filePath: String): Array[String] = {
        fromInputStream(getClass.getResourceAsStream(filePath))
            .getLines()
            .toArray
    }
}
