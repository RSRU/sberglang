import time

class InterruptController:
    def __init__(self, ttl_secs: float = 600.0):
        self._e, self._ttl, self._epoch, self._all = {}, ttl_secs, 0, 0
    def admit(self):
        self._epoch += 1; return self._epoch
    def abort(self, rid, reason="explicit"):
        if rid == "": return self.abort_all(reason)
        if rid in self._e: return self._e[rid][1]
        self._epoch += 1; self._e[rid] = (time.monotonic(), self._epoch, reason); return self._epoch
    def abort_all(self, reason="shutdown"):
        self._epoch += 1; self._all = self._epoch; return self._epoch
    def should_interrupt(self, rid, admitted_epoch=0):
        if self._all and admitted_epoch < self._all: return True
        return any(rid.startswith(k) for k in self._e)
    def filter_aborted(self, reqs):
        return [r for r, e in reqs if self.should_interrupt(r, e)]
    def ack(self, rid): return self._e.pop(rid, None) is not None
    def gc(self):
        now = time.monotonic()
        dead = [k for k, (t, _, _) in self._e.items() if now - t > self._ttl]
        for k in dead: del self._e[k]
        return len(dead)
    def __len__(self): return len(self._e)