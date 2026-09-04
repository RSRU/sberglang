use parking_lot::RwLock;
use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// Why a request was interrupted. Encoded as u8 so `abort_all` can store it atomically.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum AbortReason {
    ClientDisconnect = 1,
    Timeout = 2,
    Explicit = 3,
    Shutdown = 4,
}

impl AbortReason {
    pub fn as_str(self) -> &'static str {
        match self {
            AbortReason::ClientDisconnect => "client_disconnect",
            AbortReason::Timeout => "timeout",
            AbortReason::Explicit => "explicit",
            AbortReason::Shutdown => "shutdown",
        }
    }

    fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::ClientDisconnect),
            2 => Some(Self::Timeout),
            3 => Some(Self::Explicit),
            4 => Some(Self::Shutdown),
            _ => None,
        }
    }
}

impl FromStr for AbortReason {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "client_disconnect" | "disconnect" => Ok(Self::ClientDisconnect),
            "timeout" => Ok(Self::Timeout),
            "explicit" | "abort" => Ok(Self::Explicit),
            "shutdown" => Ok(Self::Shutdown),
            other => Err(format!("unknown abort reason: {other:?}")),
        }
    }
}

/// Snapshot handed back to callers when a request matches an abort.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AbortInfo {
    /// The registered key that matched (a full rid, or the parent rid for `n>1` children).
    /// Empty string means "matched by abort_all".
    pub key: String,
    pub reason: AbortReason,
    pub epoch: u64,
}

struct Entry {
    reason: AbortReason,
    epoch: u64,
    created: Instant,
    hits: AtomicU32,
}

#[derive(Default)]
struct Inner {
    entries: HashMap<String, Entry>,
    /// key length -> number of keys of that length. `check()` probes only the
    /// prefix lengths that actually exist, so the cost is O(distinct lengths)
    /// instead of O(len(rid)) hash lookups.
    len_hist: BTreeMap<usize, usize>,
}

impl Inner {
    fn remove_len(&mut self, len: usize) {
        if let Some(c) = self.len_hist.get_mut(&len) {
            *c -= 1;
            if *c == 0 {
                self.len_hist.remove(&len);
            }
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Stats {
    pub live_entries: u64,
    pub total_aborts: u64,
    pub total_hits: u64,
    pub total_acked: u64,
    pub total_expired: u64,
    pub epoch: u64,
    pub abort_all_epoch: u64,
}

const GC_EVERY_OPS: u32 = 256;

/// Persistent, prefix-aware abort registry.
///
/// Semantics:
/// * `abort(rid)` registers `rid` as a **prefix** — this mirrors the scheduler's
///   `req.rid.startswith(recv_req.rid)` rule used for parallel-sampling children
///   (`{rid}_0`, `{rid}_1`, ...).
/// * `abort_all()` records an epoch; every request whose admission epoch is
///   older than it is interrupted, requests admitted afterwards are untouched.
/// * Entries survive until `ack()` or TTL expiry, so an abort that arrives
///   before/around its request is not lost.
pub struct InterruptController {
    inner: RwLock<Inner>,
    live: AtomicUsize,
    epoch: AtomicU64,
    abort_all_epoch: AtomicU64,
    abort_all_reason: AtomicU8,
    ttl: Duration,
    total_aborts: AtomicU64,
    total_hits: AtomicU64,
    total_acked: AtomicU64,
    total_expired: AtomicU64,
    ops_since_gc: AtomicU32,
}

impl InterruptController {
    pub fn new(ttl: Duration) -> Self {
        Self {
            inner: RwLock::new(Inner::default()),
            live: AtomicUsize::new(0),
            epoch: AtomicU64::new(0),
            abort_all_epoch: AtomicU64::new(0),
            abort_all_reason: AtomicU8::new(0),
            ttl,
            total_aborts: AtomicU64::new(0),
            total_hits: AtomicU64::new(0),
            total_acked: AtomicU64::new(0),
            total_expired: AtomicU64::new(0),
            ops_since_gc: AtomicU32::new(0),
        }
    }

    #[inline]
    fn next_epoch(&self) -> u64 {
        self.epoch.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// Call when the scheduler admits a request; store the result on the `Req`.
    pub fn admit(&self) -> u64 {
        self.next_epoch()
    }

    /// Register an abort for `rid` (and all rids that start with it). Idempotent.
    pub fn abort(&self, rid: &str, reason: AbortReason) -> u64 {
        if rid.is_empty() {
            // An empty prefix matches everything — identical to abort_all.
            return self.abort_all(reason);
        }
        self.maybe_gc();
        let mut g = self.inner.write();
        if let Some(e) = g.entries.get(rid) {
            return e.epoch;
        }
        let epoch = self.next_epoch();
        g.entries.insert(
            rid.to_owned(),
            Entry { reason, epoch, created: Instant::now(), hits: AtomicU32::new(0) },
        );
        *g.len_hist.entry(rid.len()).or_insert(0) += 1;
        self.live.fetch_add(1, Ordering::Release);
        self.total_aborts.fetch_add(1, Ordering::Relaxed);
        epoch
    }

    /// Interrupt every request admitted before this call.
    pub fn abort_all(&self, reason: AbortReason) -> u64 {
        let epoch = self.next_epoch();
        self.abort_all_reason.store(reason as u8, Ordering::Release);
        self.abort_all_epoch.fetch_max(epoch, Ordering::AcqRel);
        self.total_aborts.fetch_add(1, Ordering::Relaxed);
        epoch
    }

    /// Is `rid` (admitted at `admitted_epoch`) interrupted? Returns the match.
    pub fn check(&self, rid: &str, admitted_epoch: u64) -> Option<AbortInfo> {
        let all = self.abort_all_epoch.load(Ordering::Acquire);
        if all != 0 && admitted_epoch < all {
            self.total_hits.fetch_add(1, Ordering::Relaxed);
            let reason = AbortReason::from_u8(self.abort_all_reason.load(Ordering::Acquire))
                .unwrap_or(AbortReason::Shutdown);
            return Some(AbortInfo { key: String::new(), reason, epoch: all });
        }
        // Fast path: the registry is empty almost all of the time.
        if self.live.load(Ordering::Acquire) == 0 {
            return None;
        }
        let g = self.inner.read();
        for (&len, _) in g.len_hist.range(..=rid.len()) {
            if !rid.is_char_boundary(len) {
                continue;
            }
            if let Some(e) = g.entries.get(&rid[..len]) {
                e.hits.fetch_add(1, Ordering::Relaxed);
                self.total_hits.fetch_add(1, Ordering::Relaxed);
                return Some(AbortInfo { key: rid[..len].to_owned(), reason: e.reason, epoch: e.epoch });
            }
        }
        None
    }

    #[inline]
    pub fn should_interrupt(&self, rid: &str, admitted_epoch: u64) -> bool {
        self.check(rid, admitted_epoch).is_some()
    }

    /// Batch sweep: returns the rids that must be interrupted. Takes the read
    /// lock once for the whole batch.
    pub fn filter_aborted<'a, I>(&self, reqs: I) -> Vec<String>
    where
        I: IntoIterator<Item = (&'a str, u64)>,
    {
        let all = self.abort_all_epoch.load(Ordering::Acquire);
        let live = self.live.load(Ordering::Acquire) != 0;
        if all == 0 && !live {
            return Vec::new();
        }
        let g = self.inner.read();
        let mut out = Vec::new();
        'outer: for (rid, admitted) in reqs {
            if all != 0 && admitted < all {
                out.push(rid.to_owned());
                continue;
            }
            if !live {
                continue;
            }
            for (&len, _) in g.len_hist.range(..=rid.len()) {
                if rid.is_char_boundary(len) {
                    if let Some(e) = g.entries.get(&rid[..len]) {
                        e.hits.fetch_add(1, Ordering::Relaxed);
                        out.push(rid.to_owned());
                        continue 'outer;
                    }
                }
            }
        }
        self.total_hits.fetch_add(out.len() as u64, Ordering::Relaxed);
        out
    }

    /// The scheduler finished cleaning up `rid`; drop its entry (exact key only —
    /// a parent prefix for `n>1` children is left to TTL, since siblings may still run).
    pub fn ack(&self, rid: &str) -> bool {
        let mut g = self.inner.write();
        if g.entries.remove(rid).is_some() {
            g.remove_len(rid.len());
            self.live.fetch_sub(1, Ordering::Release);
            self.total_acked.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    /// Drop entries older than the TTL (aborts for requests that never showed up).
    pub fn gc(&self) -> usize {
        if self.live.load(Ordering::Acquire) == 0 {
            return 0;
        }
        let now = Instant::now();
        let ttl = self.ttl;
        let mut g = self.inner.write();
        let dead: Vec<String> = g
            .entries
            .iter()
            .filter(|(_, e)| now.duration_since(e.created) > ttl)
            .map(|(k, _)| k.clone())
            .collect();
        for k in &dead {
            g.entries.remove(k);
            g.remove_len(k.len());
        }
        let n = dead.len();
        if n > 0 {
            self.live.fetch_sub(n, Ordering::Release);
            self.total_expired.fetch_add(n as u64, Ordering::Relaxed);
        }
        n
    }

    fn maybe_gc(&self) {
        if self.ops_since_gc.fetch_add(1, Ordering::Relaxed) + 1 >= GC_EVERY_OPS {
            self.ops_since_gc.store(0, Ordering::Relaxed);
            self.gc();
        }
    }

    pub fn len(&self) -> usize {
        self.live.load(Ordering::Acquire)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn stats(&self) -> Stats {
        Stats {
            live_entries: self.len() as u64,
            total_aborts: self.total_aborts.load(Ordering::Relaxed),
            total_hits: self.total_hits.load(Ordering::Relaxed),
            total_acked: self.total_acked.load(Ordering::Relaxed),
            total_expired: self.total_expired.load(Ordering::Relaxed),
            epoch: self.epoch.load(Ordering::Acquire),
            abort_all_epoch: self.abort_all_epoch.load(Ordering::Acquire),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctl() -> InterruptController {
        InterruptController::new(Duration::from_secs(60))
    }

    #[test]
    fn exact_and_prefix_match() {
        let c = ctl();
        let e = c.admit();
        assert!(!c.should_interrupt("req-1", e));
        c.abort("req-1", AbortReason::ClientDisconnect);
        assert!(c.should_interrupt("req-1", e));
        assert!(c.should_interrupt("req-1_0", e)); // n>1 child
        assert!(c.should_interrupt("req-1_7", e));
        assert!(!c.should_interrupt("req-10", e) == false); // "req-10" starts with "req-1" — same rule as scheduler
        assert!(!c.should_interrupt("req-2", e));
        assert!(!c.should_interrupt("req", e)); // shorter than key
    }

    #[test]
    fn abort_is_idempotent() {
        let c = ctl();
        let a = c.abort("x", AbortReason::Explicit);
        let b = c.abort("x", AbortReason::Timeout);
        assert_eq!(a, b);
        assert_eq!(c.len(), 1);
        assert_eq!(c.stats().total_aborts, 1);
    }

    #[test]
    fn abort_all_respects_admission_epoch() {
        let c = ctl();
        let old = c.admit();
        c.abort_all(AbortReason::Shutdown);
        let new = c.admit();
        assert!(c.should_interrupt("any", old));
        assert!(!c.should_interrupt("any", new));
        let info = c.check("any", old).unwrap();
        assert_eq!(info.reason, AbortReason::Shutdown);
        assert!(info.key.is_empty());
    }

    #[test]
    fn filter_batch() {
        let c = ctl();
        let e = c.admit();
        c.abort("a", AbortReason::ClientDisconnect);
        let got = c.filter_aborted([("a_0", e), ("b", e), ("a_1", e), ("c", e)]);
        assert_eq!(got, vec!["a_0".to_string(), "a_1".to_string()]);
        assert_eq!(c.stats().total_hits, 2);
    }

    #[test]
    fn ack_removes_exact_only() {
        let c = ctl();
        c.abort("p", AbortReason::Explicit);
        assert!(!c.ack("p_0")); // child key not registered
        assert!(c.ack("p"));
        assert!(!c.ack("p"));
        assert!(c.is_empty());
        assert!(!c.should_interrupt("p_0", 0));
    }

    #[test]
    fn gc_expires_entries() {
        let c = InterruptController::new(Duration::ZERO);
        c.abort("stale", AbortReason::Explicit);
        std::thread::sleep(Duration::from_millis(2));
        assert_eq!(c.gc(), 1);
        assert!(c.is_empty());
        assert_eq!(c.stats().total_expired, 1);
    }

    #[test]
    fn unicode_boundaries_are_safe() {
        let c = ctl();
        c.abort("ré", AbortReason::Explicit); // 3 bytes
        assert!(c.should_interrupt("ré-child", 0));
        assert!(!c.should_interrupt("r€", 0)); // shares 1st byte only
    }

    #[test]
    fn empty_prefix_is_abort_all() {
        let c = ctl();
        let e = c.admit();
        c.abort("", AbortReason::Shutdown);
        assert!(c.should_interrupt("whatever", e));
        assert!(!c.should_interrupt("later", c.admit()));
    }
}