# TXN-001 independent state-transition oracle

Oracle source: `PrimaryKeyStrategyCoreTest#testFoldsSingleCardinalityProperties`.

The test performs a transactional add followed by rollback and verifies the
rolled-back state does not survive, then commits the valid state and verifies
visibility. Java 11 execution completed with BUILD SUCCESS.

Evidence: `txn-001-primarykey-java11-20260913.log`.
