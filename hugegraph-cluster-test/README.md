# Cluster integration tests

The simple and multi cluster suites start isolated PD, Store and Server processes
using the locally packaged distributions. Build the distributions first, then run
`mvn test -pl hugegraph-cluster-test/hugegraph-clustertest-test -am -P simple-cluster-test`
or the `multi-cluster-test` profile.

Each node has a five-minute startup deadline. An exited process fails immediately,
even if an old log contains a ready marker. Failure messages identify the node and
its startup log. Each launch archives an existing startup log beside the new
log under a unique `.previous-*.log` name so readiness only uses the current attempt. A startup failure stops the task's node processes and preserves
their directories for diagnosis; normal teardown removes test directories.
An interrupted startup wait preserves the thread interrupt status. Shutdown waits
up to 20 seconds for a graceful exit and then up to 10 seconds after force-kill.
Interruptions do not reset these deadlines or abandon the wait; the interrupt
status is restored afterward. A process surviving force-kill fails teardown, and
its directory is retained. Cleanup still attempts the remaining nodes. These checks bound
startup diagnostics; the suites must still pass their functional assertions.

The generated Server metadata `pd.peers` uses the same randomized PD addresses
as graph storage. Store readiness waits for the completed application startup
marker, rather than Spring's initial starting message.
