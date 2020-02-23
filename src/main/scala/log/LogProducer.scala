package log

import java.io.FileWriter

import config.Settings
import org.apache.arrow.vector.types.pojo.ArrowType.Timestamp
import org.apache.commons.io.FileUtils

import scala.io.Source.fromInputStream
import scala.util.Random

object LogProducer extends App {

    val wlc = Settings.WebLogGen

    val LogIPAddresses = getLinesInFile("/LogIPAddresses.csv")
    val LogMessages = getLinesInFile("/LogMessages.csv")
    val Visitors = (0 to wlc.visitors).map("Visitor-" + _)

    val rnd = new Random()
    val filePath = wlc.filePath
    val destPath = wlc.destPath

    for (fileCount <- 1 to wlc.numberOfFiles) {

        val fw = new FileWriter(filePath, true)

        val incrementTimeEvery = rnd.nextInt(200 - 1) + 1

        var timestamp = System.currentTimeMillis()
        var adjustedTimestamp = timestamp

        for (iteration <- 1 to wlc.records) {

            adjustedTimestamp = adjustedTimestamp + ((System.currentTimeMillis() - timestamp) * wlc.timeMultiplier)
            timestamp = System.currentTimeMillis()

            val line = generateNewDataPoint(iteration, adjustedTimestamp, timestamp)
            fw.write(line)

            if (iteration % incrementTimeEvery == 0) {
                println(s"Sent $iteration messages!")
                val sleeping = rnd.nextInt(incrementTimeEvery * 60)
                println(s"Sleeping for $sleeping ms")
                Thread sleep sleeping
            }

        }
        fw.close()

        val outputFile = FileUtils.getFile(s"${destPath}data_$timestamp")
        println(s"Moving produced data to $outputFile")
        FileUtils.moveFile(FileUtils.getFile(filePath), outputFile)
        val sleeping = 5000
        println(s"Sleeping for $sleeping ms")
    }

    /**
     * Generates a new data point for the fake log message.
     *
     * @param iteration The current iteration for pseudo randomness.
     * @param adjustedTimeStamp The current adjusted timestamp.
     * @param timestamp The current timestamp.
     * @return
     */
    def generateNewDataPoint(iteration: Int, adjustedTimeStamp: Long, timestamp: Long): String = {

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

        val visitor = Visitors(rnd.nextInt(Visitors.length - 1))
        val message = LogMessages(rnd.nextInt(2000 - 1))
        val ip = LogIPAddresses(rnd.nextInt(500 - 1))

        s"$adjustedTimeStamp;$visitor;$ip;$message;$statusCode;$logLevel\n"
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
