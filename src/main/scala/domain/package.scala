package object domain {
    case class LogDataPoint(timestamp: Long,
                            visitor: String,
                            ip: String,
                            message: String,
                            statuscode: Int,
                            loglevel: String)
}
