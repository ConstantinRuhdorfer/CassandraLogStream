import domainTypes.HTTPMethod.HTTPMethod
import domainTypes.HTTPVersion.HTTPVersion
import domainTypes.LogLevel.LogLevel

package object domain {

    case class LogByLogId(timestamp: Long,
                          pageid: String,
                          visitorid: String,
                          ip: String,
                          loglevel: LogLevel,
                          httpmethod: HTTPMethod,
                          httpversion: HTTPVersion,
                          statuscode: Int)


    case class PageDetailsByPageId(pageid: String,
                                   pagename: String,
                                   service: String)

    case class IpByVisitorId(visitorid: String,
                             timestamp: Long,
                             ip: String)

}
