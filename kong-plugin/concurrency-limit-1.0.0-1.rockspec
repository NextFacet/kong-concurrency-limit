package = "concurrency-limit"
version = "1.0.0-1"

source = {
  url = "git+https://github.com/NextFacet/kong-concurrency-limit.git",
}

description = {
  summary = "Kong plugin: caps the number of concurrent in-flight requests per service/route.",
  license = "Apache 2.0",
}

dependencies = {
  "lua >= 5.1",
}

build = {
  type = "builtin",
  modules = {
    ["kong.plugins.concurrency-limit.handler"] = "kong/plugins/concurrency-limit/handler.lua",
    ["kong.plugins.concurrency-limit.schema"] = "kong/plugins/concurrency-limit/schema.lua",
    ["kong.plugins.concurrency-limit.api"] = "kong/plugins/concurrency-limit/api.lua",
  },
}
