# [New Podling Proposal of HugeGraph](https://cwiki.apache.org/confluence/display/INCUBATOR/New+Podling+Proposal)

## Abstract

HugeGraph is a high-performance scalable graph database.

## Proposal

HugeGraph is a large-scale and easy-to-use graph database. In the cases of 100+ billion data (vertices and edges), HugeGraph has complete HTAP capabilities built into its internal system. The design goal of HugeGraph is to achieve a balance between availability, performance, and cost.

We believe that the HugeGraph project will benefit the open-source community if HugeGraph is introduced into the Apache Software Foundation.

### Background

HugeGraph graph database was initially developed at Baidu, the leading search engine company in China. HugeGraph was designed to solve the large-scale graph analysis requirements for the task of anti-fraud. Relational databases are usually at a disadvantage in dealing with relational analysis, due to slow join performance, especially in the case of a large-scale graph with multiple dimensions and deep association relationships. Graph databases are in general considered a good alternative in dealing with large-scale relational analysis.

### Rationale

Generally, most graph databases can only process up to 1-billion-scale graphs, but it lacks a system to process 100-billion-scale graphs. Some graph databases may only support online query (OLTP), and some graph platforms only support graph computing (OLAP). HugeGraph is designed to support both online query and graph computing in the scenarios of 100+ billion data.

### Initial Goals

Although most of the main development of HugeGraph has been completed, there are several areas that still need future development. Some areas we want to focus on in the Apache incubation stage include:

- Higher graph computing loading performance: the current architecture (especially the separation of computing and storage) brings greater flexibility and cost savings, but with some performance overhead, which needs to be improved.
- RocksDB-based sharding storage development, based on the affinity architecture of query and storage. Since RocksDB is known for its good performance, but currently HugeGraph only supports the raft-based replication mode, we plan to support scale-out and distributed atomic transactions for RocksDB-based storage.
- Parallel OLTP query: at present, HugeGraph can perform parallel OLAP graph-computing, but the OLTP query only supports parallel execution on a single machine. In certain scenarios, the OLTP query needs to be implemented in parallel on multiple machines.
- Higher-performance queries: such as supporting faster query optimization, parallel primitive collections, and fine-grained caching strategies.
- Better usability: more OLTP/OLAP algorithms, APIs and toolchains, rich UI, etc...
- More language clients: such as supporting Python and Go languages.
- Some other optimizations: such as supporting pre-sorting graph loading.

### Current Status

#### Meritocracy

HugeGraph was incubated at Baidu in 2016 and open-sourced on [GitHub](https://github.com/apache/hugegraph) in 2018. The project (including sub-projects) now has 30+ contributors from many companies. Some of these contributors become committers, and the project has hundreds of known users around the world. We will follow Apache's Meritocracy way to re-organize the community roles. We have set up the PPMC Team and Committer Team. Of course, contributions are welcomed and highly valued. New contributors are guided and reviewed by existing PMC members. When an active contributor has submitted enough good patches, PMC will start a vote to promote him/her to become a member of the Committer Team or PMC Team.

#### Community

Baidu has been building a community around HugeGraph users and developers for the last 3 years, and now we make use of GitHub as code hosting and community communication. The most of core developers are from Baidu. Besides, there are 10+ contributors from other companies such as NetEase, 360, Iflytek, and Huya. We hope to grow the base of contributors by inviting all those who offer contributions through The Apache Way. 

#### Core Developers

The core developers are all experienced open-source developers. They have operated the HugeGraph Community for 3 years, and they are contributors to Linux kernel, OpenStack, Ceph, RocksDB, Apache TinkerPop, Apache Hadoop, and Apache Groovy.

#### Alignment

HugeGraph implements the API of [Apache TinkerPop](https://tinkerpop.apache.org/), which defines the [Gremlin](https://tinkerpop.apache.org/gremlin.html). Gremlin is a powerful graph traversal language, and various complicated graph queries can be accomplished through Gremlin. The TinkerPop ecosystem based on Java is relatively mature in terms of graph databases. We use Java language, which is developed efficient and stable, to develop upper-level components like graph engine, graph computing, graph API, and graph tools; and we manage storage through JNI which is able to freely manage memory and execute efficiently.

### Known Risks

#### Project Name

We have checked and believe the name is [suitable](https://github.com/apache/hugegraph/issues/1646) and the project has legal permission to continue using its current name. There are no other projects found using this name through Google Search.

#### Relationship with Titan/Janus Graph

In the early stage of the project, we referred to the storage structure of Titan/Janus Graph, some folks thought that HugeGraph was forked from Titan/Janus. In fact, HugeGraph is not based on these projects. HugeGraph is developed completely from scratch and in the process it addressed many new challenges. Certainly, the project was inspired by Titan/Janus and we are really grateful for these inspirations.

### Orphaned products

Due to a small and limited number of committers, the project has a relatively small risk of becoming an orphan project. However, the committers have been operating the HugeGraph Community for 3 years in the spirit of open-source, and continue to develop new contributors to participate.

#### Inexperience with Open-Source

HugeGraph has been open-sourced on GitHub for 3 years, during which committers submitted code and documents in full compliance with open-source specifications and requirements.

#### Length of Incubation

Expect to enter incubation in 4 months and graduate in about 2 years.

#### Homogenous Developers

The developers on the current list come from several different companies plus many independent volunteers, but the most of committers are from Baidu. The developers are geographically concentrated in China now. They are experienced with working in a distributed environment in other open-source projects, e.g. OpenStack.

#### Reliance on Salaried Developers

Most of the developers are paid by their employer to contribute to this project. Given some volunteer developers and the committers' sense of ownership for the code, the project could continue even if no salaried developers contributed to the project.

#### Relationships with Other Apache Products

HugeGraph follows the Apache TinkerPop specification, uses Apache Commons, Apache HttpClient, and Apache HttpCore to implement the basic functions. Users can choose Apache Cassandra or Apache HBase as one of the storage backends of HugeGraph.

#### An Excessive Fascination with the Apache Brand

Although we expect that the Apache brand may help attract more contributors, our interest in starting this project is based on the factors mentioned in the fundamentals section. We are interested in joining ASF to increase our connections in the open-source world. Based on extensive collaboration, it is possible to build a community of developers and committers that live longer than the founder.

### Documentation

HugeGraph documentation is provided on https://hugegraph.github.io/hugegraph-doc/ in Simplified Chinese, the complete English version of the documentation is being prepared.

### Initial Source

This project consists of 2 core sub-projects and 2 other sub-projects, all of which are hosted by [GitHub hugegraph organization](https://github.com/orgs/hugegraph/repositories) since 2018. The codes are already under Apache License Version 2.0. The git address of sub-project repositories are as follows:

1. The graph database repository `hugegraph`, core sub-project, including graph server, graph engine, and graph storage: https://github.com/apache/hugegraph
2. The graph computing repository `hugegraph-computer`, core sub-project, including graph computing and graph algorithms: https://github.com/apache/hugegraph-computer
3. The common functions repository `hugegraph-commons`: https://github.com/apache/hugegraph/tree/master/hugegraph-commons
4. The ecosystem repository `hugegraph-toolchain`, including `hugegraph-client`, `hugegraph-loader`, `hugegraph-tools`, `hugegraph-hubble`, `hugegraph-test`, `hugegraph-doc`: https://github.com/apache/hugegraph-toolchain

### Source and Intellectual Property Submission Plan

The code is currently under Apache License Version 2.0, and it was verified there were no intellectual property or license issues when it was open-sourced by Baidu in 2018. Baidu will provide SGA and all committers will sign ICLA after HugeGraph is accepted into the Incubator.

#### External Dependencies

As all dependencies are managed by Apache Maven, none of the external libraries need to be packaged in a source distribution. All dependencies have Apache compatible licenses except for 4 dependencies: MySQL Connector (GPL), word (GPL), JBoss Logging 3 (LGPL), and jnr-posix (LGPL+GPL), we will remove these dependencies in the future.

HugeGraph has the following external [dependencies](https://github.com/apache/hugegraph/issues/1632):

- Apache License

  - HttpClient
  - HttpCore

- Apache License 2.0

  - Annotations for Metrics
  - ansj_seg
  - Apache Cassandra
  - Apache Commons
  - Apache Groovy
  - Apache HttpClient
  - Apache HttpCore
  - Apache Ivy
  - Apache Log4j
  - Apache Thrift
  - Apache TinkerPop
  - Apache Yetus
  - Bean Validation API
  - Caffeine cache
  - CLI
  - Sofa
  - ConcurrentLinkedHashMap
  - Data Mapper for Jackson
  - DataStax Java Driver for Apache Cassandra
  - Disruptor Framework
  - error-prone annotations
  - exp4j
  - fastutil
  - Findbugs
  - Google Android Annotations Library
  - Gson
  - Guava
  - Hibernate Validator Engine
  - HPPC Collections
  - htrace-core4
  - IKAnalyzer
  - io.grpc
  - J2ObjC Annotations
  - Jackson
  - Java Agent for Memory Measurements
  - Java Concurrency Tools Core Library
  - JavaPoet
  - javatuples
  - jcseg-core
  - jffi
  - JJWT
  - jnr
  - Joda
  - jraft
  - JSON.simple
  - JVM Integration for Metrics
  - Lucene
  - LZ4 and xxHash
  - Metrics
  - mmseg4j-core
  - Netty
  - Ning-compress-LZF
  - nlp-lang
  - Objenesis
  - OHC core
  - OpenTracing
  - perfmark
  - picocli
  - proto-google-common-protos
  - sigar
  - snappy-java
  - stream-lib
  - swagger
  - Thrift Server implementation backed by LMAX Disruptor
  - jieba for java
  - Java Native Access
  - HanLP
  - SnakeYAML

- Apache License 2.0 + GPLv2 License

  - RocksDB JNI

- BSD License

  - ANTLR 3
  - ASM
  - Chinese to Pinyin
  - Hamcrest
  - JLine
  - jcabi
  - PostgreSQL JDBC Driver - JDBC 4.2
  - Protocol Buffers
  - StringTemplate

- CDDL + GPL License

  - Grizzly
  - Jersey

- CDDL + GPLv2 License

  - glassfish
  - Java Servlet API
  - javax.ws.rs-api 
  - jersey-inject-hk2
  - JSR 353 (JSON Processing) Default Provider

- Eclipse Distribution License

  - Eclipse Collections
  - JUnit

- ISC/BSD License

  - jBCrypt

- LGPL License

  - JBoss Logging 3

- LGPL + GPL License

  - jnr-posix

- LGPL 2.1 + MPL 1.1 + Apache License 2.0

  - Javassist

- MIT License

  - Animal Sniffer Annotations
  - Mockito
  - SLF4J
  - Jedis
  - high-scale-lib
  - jnr-x86asm

- GPL + MIT License

  - Checker Qual

- GPL License

  - MySQL Connector/J
  - word

#### Cryptography

None

### Required Resources

#### Mailing lists

- hugegraph-dev: [dev@hugegraph.incubator.apache.org](mailto:dev@hugegraph.incubator.apache.org) for development and users discussions.
- hugegraph-private: [private@hugegraph.incubator.apache.org](mailto:private@hugegraph.incubator.apache.org) for PPMC discussions.
- hugegraph-commits: [commits@hugegraph.incubator.apache.org](mailto:commits@hugegraph.incubator.apache.org) for commits / [pull requests](https://github.com/apache/hugegraph/pulls) and other notifications like code review comments.

#### Subversion Directory

None

#### Git Repositories

1. `hugegraph`: https://github.com/apache/hugegraph.git
2. `hugegraph-computer`: https://github.com/apache/hugegraph-computer.git
3. `hugegraph-commons`: https://github.com/apache/hugegraph.git
4. `hugegraph-toolchain`: https://github.com/apache/hugegraph-toolchain.git

#### Issue Tracking

The community would like to continue using [GitHub Issues](https://github.com/apache/hugegraph/issues) (but will move to github.com/apache/).

#### Other Resources

- The community has already chosen [GitHub actions](https://github.com/apache/hugegraph/actions) as continuous integration tools.
- The community has already used [codecov](https://github.com/marketplace/codecov) to check code coverage.
- The community has already used [mvn repository](https://mvnrepository.com/search?q=hugegraph) as binary package release platform.

### Initial Committers

- Jermy Li (javaloveme at gmail dot com)
- Linary (liningrui at vip dot qq dot com)
- Zhoney (zhangyi89817 at 126 dot com)
- Imbajin (0x00 at imbajin.com)
- Vaughn (1318247699 at qq dot com)
- LiuNanke (liunanke at baidu dot com)
- Coderzc (zc1217zc at 126 dot com)
- ShouJing (1075185785 at qq dot com)
- Xiaobiao Li (13703287619 at 163 dot com)
- Haiping Huang (954872405 at qq dot com)
- Kidleaf Jiang (kidleaf at 163 dot com)

### Sponsors

#### Champion

- Willem Ning Jiang (ningjiang at apache dot org)

#### Nominated Mentors

- Trista Pan (panjuan at apache dot org)
- Lidong Dai (lidongdai at apache dot org)
- Xiangdong Huang (hxd at apache dot org)
- Yu Li (liyu at apache dot org)

#### Sponsoring Entity

We are expecting the Apache Incubator could sponsor this project.
