package config

import com.typesafe.config.ConfigFactory

object Settings {

    private val config = ConfigFactory.load()

    object WebLogGen {
        private val weblogGen = config.getConfig("randomHTTPLogs")

        lazy val records: Int = weblogGen.getInt("records")
        lazy val visitors: Int = weblogGen.getInt("visitors")
        lazy val filePath: String = weblogGen.getString("file_path")
        lazy val destPath: String = weblogGen.getString("dest_path")
        lazy val numberOfFiles: Int = weblogGen.getInt("number_of_files")
        lazy val sparkAppName: String = weblogGen.getString("spark_app_name")
        lazy val defaultKeySpace: String = weblogGen.getString("default_key_space")
        lazy val defaultMasterLogDataTableName: String = weblogGen.getString("default_master_log_data_table_name")
        lazy val defaultPageDetailsByPageId: String = weblogGen.getString("default_page_details_by_pageid")
        lazy val defaultIPsByVisitorId: String = weblogGen.getString("default_ips_by_visitorid")
    }

}
