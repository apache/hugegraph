#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Partition { pub start: u64, pub end: u64, pub version: u64 }

pub fn validate(parts: &[Partition], max: u64) -> Result<(), &'static str> {
    if parts.is_empty() || parts[0].start != 0 { return Err("gap-at-start"); }
    for (i, p) in parts.iter().enumerate() {
        if p.start >= p.end || p.end > max { return Err("invalid-range"); }
        if i > 0 {
            let prev = &parts[i - 1];
            if prev.end != p.start { return Err("gap-or-overlap"); }
            if p.version < prev.version { return Err("version-regression"); }
        }
    }
    if parts.last().unwrap().end != max { return Err("gap-at-end"); }
    Ok(())
}

pub fn apply_heartbeat(current: &mut Partition, incoming: Partition) -> Result<(), &'static str> {
    if incoming.start != current.start || incoming.end != current.end { return Err("range-mismatch"); }
    if incoming.version < current.version { return Err("stale-heartbeat"); }
    *current = incoming;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn base() -> Vec<Partition> { vec![Partition{start:0,end:10,version:1}, Partition{start:10,end:20,version:1}] }
    #[test] fn baseline_is_valid() { assert!(validate(&base(), 20).is_ok()); }
    #[test] fn catches_dropped_right_partition() { let mut x=base(); x.pop(); assert!(validate(&x,20).is_err()); }
    #[test] fn catches_overlap() { let mut x=base(); x[1].start=9; assert_eq!(validate(&x,20),Err("gap-or-overlap")); }
    #[test] fn catches_version_regression() { let mut x=base(); x[1].version=0; assert_eq!(validate(&x,20),Err("version-regression")); }
    #[test] fn rejects_stale_heartbeat() { let mut c=Partition{start:0,end:10,version:2}; assert_eq!(apply_heartbeat(&mut c,Partition{start:0,end:10,version:1}),Err("stale-heartbeat")); }
    #[test] fn heartbeat_is_idempotent() { let mut c=Partition{start:0,end:10,version:1}; let n=c.clone(); apply_heartbeat(&mut c,n.clone()).unwrap(); apply_heartbeat(&mut c,n).unwrap(); assert_eq!(c.version,1); }
}
