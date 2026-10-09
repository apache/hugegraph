Building HugeGraph
--------------

Required:

* Java 17 (currently the only supported Java release)
* Maven 3.6.3+

The launch scripts enforce Java 17 as the minimum runtime. This check does not
qualify later Java releases; build HugeGraph with Java 17 unless another
release is explicitly listed as supported.

The published `hugegraph-common` and `hugegraph-rpc` libraries use Java 11
bytecode for downstream compatibility. Their build and tests still run on
Java 17; this library bytecode target does not lower HugeGraph's runtime
requirement.

See the [TinkerPop 3.8.1 migration guide](upgrade-tinkerpop-3.8.md) for the
upgraded runtime, client configuration and compatibility checks.

To build without executing tests: `mvn clean package -Dmaven.test.skip=true`

## Optional word analyzer

The standard distribution does not include `org.apdplat:word:1.3`. It is a
provided dependency used to compile and test the optional `word` analyzer.
The default analyzer remains `ikanalyzer`; the shipped graph configurations
use `jieba`, and neither requires the word library.

Obtain the GPLv3-licensed external component from
[Maven Central](https://repo.maven.apache.org/maven2/org/apdplat/word/1.3/word-1.3.jar).

To use `word`, supply `word-1.3.jar` separately in the Server's `lib/` directory
and restart the Server. Select it in the graph configuration with a supported
mode, for example:

```properties
search.text_analyzer=word
search.text_analyzer_mode=PureEnglish
```

Selecting `word` without its external library fails with a configuration error
identifying the missing optional dependency. Other analyzers remain available.
Applications using Struct directly must add the same dependency to their
runtime classpath when selecting `word`.

## Building in IDEA

To build without executing tests:

1. Click on "File" -> "Open", choose your project location.
2. Open maven view by click "View" -> "Tool Windows" -> "Maven Projects".
3. Choose root module "hugegraph: Distributed Graph Database", unfold the menu of "Lifecycle".
4. Click the "Toggle 'Skip Tests' Mode" button which is located on the top navibar of "Maven Projects" window to skip tests.
5. Double click "package" or "install" to build a project.

Could also refer [Dev-In-IDEA](https://hugegraph.apache.org/docs/contribution-guidelines/hugegraph-server-idea-setup/) for more details.

## Building in Eclipse

> Note: this has only been tested on Eclipse Neon.2 Release (4.6.2) with m2e (1.7.0.20160603-1933) and m2e-wtp (1.3.1.20160831-1005) plugin.

To build without executing tests:

1. Right-click on your project -> "Run As..." -> "Run Configurations..."
2. On "Goals", populate with `install`
3. Select the options `Update Snapshots` and `Skip Tests`
4. Before clicking "Run", make sure that Eclipse knows where `JAVA_HOME` is. In the same window, go to "Environment" tab and click "New".
5. Under "Name:", add `JAVA_HOME`
6. Under "Value:", add the path where `java` is located
7. Click "OK"
8. Then click "Run"

To find the Java binary in your environment, run the appropriate command for your operating system:
* Linux/macOS: `which java`
* Windows: `for %i in (java.exe) do @echo. %~$PATH:i`
