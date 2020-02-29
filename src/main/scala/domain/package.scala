import domainTypes.HTTPMethod.HTTPMethod
import domainTypes.HTTPVersion.HTTPVersion

package object domain {

    case class LogDataPoint(id: String,
                            timestamp: Long,
                            visitor: String,
                            ip: String,
                            httpmethod: HTTPMethod,
                            page: String,
                            httpversion: HTTPVersion,
                            statuscode: Int,
                            loglevel: String)

    case class PageView(pagename: String,
                        pagepath: String,
                        timestamp: Long,
                        visitorid: String,
                        visitorip: String)

    case class Visitor(visitorid: String,
                       visitorip: String,
                       timestamp: Long,
                       pagepath: String)
}
