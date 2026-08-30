Building HugeGraph
--------------

Required:

* Java 11
* Maven 3.5+

To build without executing tests: `mvn clean package -Dmaven.test.skip=true`

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

