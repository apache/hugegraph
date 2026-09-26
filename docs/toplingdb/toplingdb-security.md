# ToplingDB Security Hardening Guide

This document provides best practices for securing a ToplingDB deployment. It
covers file permissions, network access control, firewall rules, and additional
hardening measures that reduce the production attack surface.

---

## 1. File Permissions

Keep installation files owned by a deployment account. Give the service
account write access only to data, log, and runtime-state directories:

```bash
chown root:hugegraph "$HUGEGRAPH_HOME"/bin/*.sh
chmod 750 "$HUGEGRAPH_HOME"/bin/*.sh

for config in toplingdb.yaml rocksdb_pd.yaml rocksdb_store.yaml; do
  test -e "$HUGEGRAPH_HOME/conf/$config" || continue
  chown root:hugegraph "$HUGEGRAPH_HOME/conf/$config"
  chmod 640 "$HUGEGRAPH_HOME/conf/$config"
done

install -d -o hugegraph -g hugegraph -m 750 \
  "$HUGEGRAPH_HOME"/logs \
  /srv/hugegraph/topling/data \
  /run/hugegraph
```

- Replace `root` with the actual deployment owner when packages are installed
  by a non-root account.
- The service account can read scripts and configuration but cannot modify the
  startup chain, JARs, native libraries, or Easy Migrate configuration.
- Adjust the data path for Server, PD, or Store. Do not recursively change
  ownership of the whole installation directory.

---

## 2. Network Access Control

Restrict network exposure by binding services to localhost or specific
interfaces:

```yaml
# Localhost-only access
http:
  listening_ports: '127.0.0.1:2011'
```

- Avoid binding to `0.0.0.0` unless absolutely necessary.
- Use reverse proxies (e.g., Nginx) or VPN tunnels if remote access is required.

---

## 3. Firewall Rules

The Topling HTTP monitor is disabled by default. If it is explicitly enabled,
insert a dedicated chain before broader `ACCEPT` rules. Preserve established
connections and define equivalent IPv4 and IPv6 policies:

```bash
iptables -N HG_TOPLING 2>/dev/null || true
iptables -F HG_TOPLING
iptables -A HG_TOPLING -s 192.168.1.0/24 -j ACCEPT
iptables -A HG_TOPLING -j DROP
iptables -C INPUT -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT ||
  iptables -I INPUT 1 -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT
iptables -C INPUT -p tcp -m multiport --dports 2011,2012,2013 \
  -j HG_TOPLING ||
  iptables -I INPUT 2 -p tcp -m multiport --dports 2011,2012,2013 \
  -j HG_TOPLING

ip6tables -N HG_TOPLING6 2>/dev/null || true
ip6tables -F HG_TOPLING6
ip6tables -A HG_TOPLING6 -s 2001:db8:1234::/48 -j ACCEPT
ip6tables -A HG_TOPLING6 -j DROP
ip6tables -C INPUT -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT ||
  ip6tables -I INPUT 1 -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT
ip6tables -C INPUT -p tcp -m multiport --dports 2011,2012,2013 \
  -j HG_TOPLING6 ||
  ip6tables -I INPUT 2 -p tcp -m multiport --dports 2011,2012,2013 \
  -j HG_TOPLING6
```

- Replace both documentation prefixes with trusted production networks.
- Existing established connections remain allowed. Revoke them separately
  through the host connection-tracking policy when immediate eviction is
  required.
- Inspect the resulting rule order before applying it to a remote host.
- Consider using `firewalld` or `ufw` for simplified management.
