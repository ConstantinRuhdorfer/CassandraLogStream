package config

import com.typesafe.config.ConfigFactory

object Settings {

    private val config = ConfigFactory.load()

    object WebLogGen {
        private val weblogGen = config.getConfig("randomHTTPLogs")

        lazy val records: Int = weblogGen.getInt("records")
        lazy val timeMultiplier: Int = weblogGen.getInt("time_multiplier")
        lazy val visitors: Int = weblogGen.getInt("visitors")
        lazy val filePath: String = weblogGen.getString("file_path")
        lazy val destPath: String = weblogGen.getString("dest_path")
        lazy val numberOfFiles: Int = weblogGen.getInt("number_of_files")
    }

}
