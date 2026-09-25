from sgl_interrupt import InterruptController
c = InterruptController(ttl_secs=60)
e = c.admit()
assert not c.should_interrupt("abc", e)
c.abort("abc", "client_disconnect")
assert c.should_interrupt("abc_1", e)                         # n>1 child
assert c.filter_aborted([("abc_0", e), ("zzz", e)]) == ["abc_0"]
assert c.check("abc_0", e) == ("abc", "client_disconnect", 2)
c.abort_all()
assert c.should_interrupt("zzz", e) and not c.should_interrupt("zzz", c.admit())
print(c.stats())