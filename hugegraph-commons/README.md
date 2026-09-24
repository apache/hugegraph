# hugegraph-commons

[![License](https://img.shields.io/badge/license-Apache%202-0E78BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![codecov](https://codecov.io/gh/hugegraph/hugegraph-common/branch/master/graph/badge.svg)](https://codecov.io/gh/hugegraph/hugegraph-common)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.apache.hugegraph/hugegraph-common/badge.svg)](https://mvnrepository.com/artifact/org.apache.hugegraph/hugegraph-common)
[![CodeQL](https://github.com/apache/hugegraph-commons/actions/workflows/codeql-analysis.yml/badge.svg)](https://github.com/apache/hugegraph-commons/actions/workflows/codeql-analysis.yml)
[![hugegraph-commons ci](https://github.com/apache/hugegraph-commons/actions/workflows/ci.yml/badge.svg)](https://github.com/apache/hugegraph-commons/actions/workflows/ci.yml)


hugegraph-commons is a common module for [HugeGraph](https://github.com/apache/hugegraph) and its peripheral components.
hugegraph-commons encapsulates locks, configurations, events, iterators, rest and some 
numeric or collection util classes to simplify the development of HugeGraph and its components.

## Components

- Lock: atomic lock, key lock, lock group and lock manager
- Config: register and load config option with security check
- Event: listening and notification, do something asynchronously
- Iterator: some iterators with extra functions, map, filter, extend, etc.
- Rest: RESTful client implemented on OkHttp, POST, PUT, GET and DELETE
- Util: performance analyzer, version checker, numeric and Collection utils, log and exception utils, etc.
- Rpc: rpc component for inner module communication, currently it's based on [Sofa-RPC](https://github.com/sofastack/sofa-rpc)

You could use import the dependencies in `maven` like this:

```xml
  <dependency>
       <groupId>org.apache.hugegraph</groupId>
       <artifactId>hugegraph-common</artifactId>
       <version>1.2.0</version>
  </dependency>
```

## Learn More

The [doc page](https://hugegraph.apache.org/docs/) contains more information about hugegraph modules.

And here are links of other repositories:
1. [hugegraph-server](https://github.com/apache/hugegraph) (graph's core component - OLTP server)
2. [hugegraph-toolchain](https://github.com/apache/hugegraph-toolchain) (include loader/dashboard/tool/client)
3. [hugegraph-computer](https://github.com/apache/hugegraph-computer) (graph processing system - OLAP)
4. [hugegraph-website/doc](https://github.com/apache/hugegraph-doc) (include doc & website code)



## Contributing

- Welcome to contribute to HugeGraph, please see [How to Contribute](https://hugegraph.apache.org/docs/contribution-guidelines/contribute/) for more information.  
- Note: It's recommended to use [GitHub Desktop](https://desktop.github.com/) to greatly simplify the PR and commit process.  
- Thank you to all the people who already contributed to HugeGraph!

[![contributors graph](https://contrib.rocks/image?repo=apache/hugegraph-commons)](https://github.com/apache/hugegraph-commons/graphs/contributors)

## Licence

Same as HugeGraph, hugegraph-commons are also licensed under [Apache 2.0](./LICENSE) License.

### Contact Us

---

 - [GitHub Issues](https://github.com/apache/hugegraph-commons/issues): Feedback on usage issues and functional requirements (quick response)
 - Feedback Email: [dev@hugegraph.apache.org](mailto:dev@hugegraph.apache.org) ([subscriber](https://hugegraph.apache.org/docs/contribution-guidelines/subscribe/) only)
 - WeChat public account: Apache HugeGraph, welcome to scan this QR code to follow us.

 <img src="https://raw.githubusercontent.com/apache/hugegraph-doc/master/assets/images/wechat.png" alt="QR png" width="350"/>
