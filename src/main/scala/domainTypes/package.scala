package object domainTypes {

    object HTTPVersion extends Enumeration {
        type HTTPVersion = Value
        val HTTP1: domainTypes.HTTPVersion.Value = Value("HTTP/1.0")
        val HTTP1_1: domainTypes.HTTPVersion.Value = Value("HTTP/1.1")
        val HTTP2: domainTypes.HTTPVersion.Value = Value("HTTP/2.0")

        private val map = Map(
            "HTTP/1.0" -> "HTTP/1.0",
            "HTTP/1.1" -> "HTTP/1.1",
            "HTTP/2.0" -> "HTTP/2.0")

        def customWithName(str: String): domainTypes.HTTPVersion.Value = withName(map(str))
    }

    object HTTPMethod extends Enumeration {
        type HTTPMethod = Value
        val GET: domainTypes.HTTPMethod.Value = Value("GET")
        val PUT: domainTypes.HTTPMethod.Value = Value("PUT")
        val PATCH: domainTypes.HTTPMethod.Value = Value("PATCH")
        val DELETE: domainTypes.HTTPMethod.Value = Value("DELETE")
        val POST: domainTypes.HTTPMethod.Value = Value("POST")
        val HEAD: domainTypes.HTTPMethod.Value = Value("HEAD")

        private val map = Map(
            "GET" -> "GET",
            "PUT" -> "PUT",
            "PATCH" -> "PATCH",
            "DELETE" -> "DELETE",
            "POST" -> "POST",
            "HEAD" -> "HEAD")

        def customWithName(str: String): domainTypes.HTTPMethod.Value = withName(map(str))
    }

    object HTTPStatusCode extends Enumeration {
        type HTTPStatusCode = Value
        val OK: domainTypes.HTTPStatusCode.Value = Value("200")
        val CREATED: domainTypes.HTTPStatusCode.Value = Value("201")
        val FORBIDDEN: domainTypes.HTTPStatusCode.Value = Value("403")
        val NOT_FOUND: domainTypes.HTTPStatusCode.Value = Value("404")
        val INTERNAL_SERVER_ERROR: domainTypes.HTTPStatusCode.Value = Value("500")

        private val map = Map(
            "OK" -> "200",
            "CREATED" -> "201",
            "FORBIDDEN" -> "403",
            "NOT_FOUND" -> "404",
            "INTERNAL_SERVER_ERROR" -> "500")

        def customWithName(str: String): domainTypes.HTTPStatusCode.Value = withName(map(str))
    }

}
