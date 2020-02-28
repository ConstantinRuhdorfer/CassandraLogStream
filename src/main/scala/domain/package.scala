package object domain {

    case class LogDataPoint(id: String,
                            timestamp: Long,
                            visitor: String,
                            ip: String,
                            message: String,
                            statuscode: Int,
                            loglevel: String)

}
