import domain.HTTPMethod.HTTPMethod
import domain.HTTPVersion.HTTPVersion

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

    object HTTPVersion extends Enumeration {
        type HTTPVersion = Value
        val HTTP1: domain.HTTPVersion.Value = Value("HTTP/1.0")
        val HTTP1_1: domain.HTTPVersion.Value = Value("HTTP/1.1")
        val HTTP2: domain.HTTPVersion.Value = Value("HTTP/2.0")

        private val map = Map(
            "HTTP/1.0" -> "HTTP/1.0",
            "HTTP/1.1" -> "HTTP/1.1",
            "HTTP/2.0" -> "HTTP/2.0")

        def customWithName(str: String): domain.HTTPVersion.Value = withName(map(str))
    }

    object HTTPMethod extends Enumeration {
        type HTTPMethod = Value
        val GET: domain.HTTPMethod.Value = Value("GET")
        val PUT: domain.HTTPMethod.Value = Value("PUT")
        val PATCH: domain.HTTPMethod.Value = Value("PATCH")
        val DELETE: domain.HTTPMethod.Value = Value("DELETE")
        val POST: domain.HTTPMethod.Value = Value("POST")
        val HEAD: domain.HTTPMethod.Value = Value("HEAD")

        private val map = Map(
            "GET" -> "GET",
            "PUT" -> "PUT",
            "PATCH" -> "PATCH",
            "DELETE" -> "DELETE",
            "POST" -> "POST",
            "HEAD" -> "HEAD")

        def customWithName(str: String): domain.HTTPMethod.Value = withName(map(str))
    }

}
