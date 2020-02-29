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

}
