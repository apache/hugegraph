# ToplingDB Troubleshooting

## Startup Rejects the Easy Migrate Configuration

Confirm that the startup log prints the expected component path:

```text
[preload-topling] TOPLINGDB_EASY_MIGRATE_CONF=/path/to/config.yaml
```

Then check:

- the file exists and is readable by the service user;
- the YAML uses valid indentation and supported Topling option names;
- `DBOptions.default` exists as the fallback;
- the dedicated `DBOptions.log` mapping is still present.

Do not diagnose configuration loading by calling `SidePluginRepo` from Java.

## Wrong or Missing Native Runtime

Verify the prepared library and the process mapping:

```bash
test -r /path/to/component/library/librocksdbjni-linux64.so
grep librocksdbjni /proc/$PID/maps
```

Only the selected component library should be mapped. A Topling class marker in
the JAR is useful for ABI diagnosis, but it does not prove that Easy Migrate
options were applied or that DB/CF operations work.

## HTTP Port Conflict

Check the component-specific `http.listening_ports` value and whether another
process already owns it:

```bash
lsof -i :<PORT>
```

The provided configurations use 2011 for Server, 2012 for PD, and 2013 for
Store. Confirm the actual value in the selected YAML. Components on one host
must not share a port. Keep the endpoint on a trusted interface because it has
no built-in authentication.

## Database Lock Failure

A lock error normally means another process owns the database or the service
user cannot access the data directory.

1. Confirm there is only one owner for the database path.
2. Confirm directory permissions.
3. Confirm the component uses its own Easy Migrate configuration.
4. Inspect the component and RocksDB logs.

Do not bypass the normal RocksDB open path and do not attempt a reflected
`openDB`.

## Shutdown Timeout

The expected lifecycle is:

```text
SIGTERM -> graceful shutdown -> CF/DB close
        -> native MaybeForgetCF/MaybeForgetDB -> exit
```

If Store exceeds its existing timeout:

1. capture thread dumps before forcing termination;
2. inspect Store shutdown and background-thread logs;
3. confirm CF handles and RocksDB reached their normal close paths;
4. treat the result as a general Store lifecycle issue.

Do not call `SidePluginRepo.closeAllDB()`. This integration does not provide a
second Java-side owner and does not expand the scope of general thread-exit
repairs.

## DB/CF Test Gives a False Positive

A ToplingDB test that unsets `TOPLINGDB_EASY_MIGRATE_CONF` before opening a
database proves only RocksDB Java ABI compatibility. It does not test ToplingDB
configuration or native lifecycle hooks.

For a functional test, keep a readable component configuration in the
environment and exercise create, write, close, reopen, drop, and recreate
operations. Reserve no-configuration checks for ABI diagnostics that do not
open a database.
