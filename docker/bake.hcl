# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

variable "MAVEN_ARGS" {
  default = ""
}

variable "SOURCE_REVISION" {
  default = "local"
}

variable "IMAGE_TAG" {
  default = "local"
}

variable "CACHE_CHANNEL" {
  default = "latest"
}

variable "EXPORT_CACHE" {
  default = false
}

target "_common" {
  context = "."
  args = {
    MAVEN_ARGS     = MAVEN_ARGS
    SOURCE_REVISION = SOURCE_REVISION
  }
  platforms = [
    "linux/amd64",
    "linux/arm64",
  ]
}

target "build-cache" {
  inherits   = ["_common"]
  dockerfile = "hugegraph-pd/Dockerfile"
  target     = "build"
  output     = ["type=cacheonly"]
  cache-from = [
    "type=registry,ref=hugegraph/hugegraph:shared-${CACHE_CHANNEL}",
    "type=registry,ref=hugegraph/pd:buildcache-${CACHE_CHANNEL}",
  ]
  cache-to = EXPORT_CACHE ? [
    "type=registry,ref=hugegraph/hugegraph:shared-${CACHE_CHANNEL},mode=max",
  ] : []
}

# Runtime targets intentionally use Docker's local exporter. The publishing
# workflow enables the containerd image store, verifies both loaded platforms,
# runs functional checks against these exact tags, and only then pushes them.
target "pd" {
  inherits   = ["_common"]
  dockerfile = "hugegraph-pd/Dockerfile"
  tags       = ["hugegraph/pd:${IMAGE_TAG}"]
  output     = ["type=docker"]
  cache-from = [
    "type=registry,ref=hugegraph/hugegraph:shared-${CACHE_CHANNEL}",
    "type=registry,ref=hugegraph/pd:buildcache-${CACHE_CHANNEL}",
  ]
  cache-to = EXPORT_CACHE ? [
    "type=registry,ref=hugegraph/pd:buildcache-${CACHE_CHANNEL},mode=min",
  ] : []
}

target "store" {
  inherits   = ["_common"]
  dockerfile = "hugegraph-store/Dockerfile"
  tags       = ["hugegraph/store:${IMAGE_TAG}"]
  output     = ["type=docker"]
  cache-from = [
    "type=registry,ref=hugegraph/hugegraph:shared-${CACHE_CHANNEL}",
    "type=registry,ref=hugegraph/store:buildcache-${CACHE_CHANNEL}",
  ]
  cache-to = EXPORT_CACHE ? [
    "type=registry,ref=hugegraph/store:buildcache-${CACHE_CHANNEL},mode=min",
  ] : []
}

target "server-hstore" {
  inherits   = ["_common"]
  dockerfile = "hugegraph-server/Dockerfile-hstore"
  tags       = ["hugegraph/server:${IMAGE_TAG}"]
  output     = ["type=docker"]
  cache-from = [
    "type=registry,ref=hugegraph/hugegraph:shared-${CACHE_CHANNEL}",
    "type=registry,ref=hugegraph/server:buildcache-${CACHE_CHANNEL}",
  ]
  cache-to = EXPORT_CACHE ? [
    "type=registry,ref=hugegraph/server:buildcache-${CACHE_CHANNEL},mode=min",
  ] : []
}

target "server-standalone" {
  inherits   = ["_common"]
  dockerfile = "hugegraph-server/Dockerfile"
  tags       = ["hugegraph/hugegraph:${IMAGE_TAG}"]
  output     = ["type=docker"]
  cache-from = [
    "type=registry,ref=hugegraph/hugegraph:shared-${CACHE_CHANNEL}",
    "type=registry,ref=hugegraph/hugegraph:buildcache-${CACHE_CHANNEL}",
  ]
  cache-to = EXPORT_CACHE ? [
    "type=registry,ref=hugegraph/hugegraph:buildcache-${CACHE_CHANNEL},mode=min",
  ] : []
}

group "default" {
  targets = [
    "build-cache",
    "pd",
    "store",
    "server-hstore",
    "server-standalone",
  ]
}
