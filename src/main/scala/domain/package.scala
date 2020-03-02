import domainTypes.HTTPMethod.HTTPMethod
import domainTypes.HTTPVersion.HTTPVersion

package object domain {

    case class LogDataPoint(id: String,
                            timestamp: Long,
                            visitor: String,
                            ip: String,
                            httpmethod: HTTPMethod,
                            pagepath: String,
                            httpversion: HTTPVersion,
                            statuscode: Int,
                            loglevel: String)

    case class PageView(pagename: String,
                        pagepath: String,
                        service: String,
                        timestamp: Long,
                        visitorid: String,
                        visitorip: String)

    case class Visitor(visitorid: String,
                       visitorip: String,
                       timestamp: Long,
                       pagepath: String)
}
