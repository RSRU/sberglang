use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;
use std::time::Duration;

use crate::controller::{AbortReason, InterruptController};

fn parse_reason(s: &str) -> PyResult<AbortReason> {
    s.parse::<AbortReason>().map_err(PyValueError::new_err)
}

/// Python-facing handle. `frozen` => all methods take `&self`; interior
/// mutability lives in the Rust core, so the GIL can be released safely.
#[pyclass(name = "InterruptController", module = "sgl_interrupt", frozen)]
pub struct PyInterruptController {
    inner: Arc<InterruptController>,
}

#[pymethods]
impl PyInterruptController {
    #[new]
    #[pyo3(signature = (ttl_secs = 600.0))]
    fn new(ttl_secs: f64) -> PyResult<Self> {
        if !(ttl_secs >= 0.0) || !ttl_secs.is_finite() {
            return Err(PyValueError::new_err("ttl_secs must be a finite number >= 0"));
        }
        Ok(Self {
            inner: Arc::new(InterruptController::new(Duration::from_secs_f64(ttl_secs))),
        })
    }

    /// Admission epoch for a new request; store it as `req.interrupt_epoch`.
    fn admit(&self) -> u64 {
        self.inner.admit()
    }

    /// Register an abort for `rid` and all `rid*` children. Returns the abort epoch.
    #[pyo3(signature = (rid, reason = "explicit"))]
    fn abort(&self, rid: &str, reason: &str) -> PyResult<u64> {
        Ok(self.inner.abort(rid, parse_reason(reason)?))
    }

    /// Interrupt everything admitted so far.
    #[pyo3(signature = (reason = "shutdown"))]
    fn abort_all(&self, reason: &str) -> PyResult<u64> {
        Ok(self.inner.abort_all(parse_reason(reason)?))
    }

    #[pyo3(signature = (rid, admitted_epoch = 0))]
    fn should_interrupt(&self, rid: &str, admitted_epoch: u64) -> bool {
        self.inner.should_interrupt(rid, admitted_epoch)
    }

    /// Returns `(matched_key, reason, epoch)` or `None`.
    #[pyo3(signature = (rid, admitted_epoch = 0))]
    fn check(&self, rid: &str, admitted_epoch: u64) -> Option<(String, &'static str, u64)> {
        self.inner
            .check(rid, admitted_epoch)
            .map(|i| (i.key, i.reason.as_str(), i.epoch))
    }

    /// Batch sweep over `[(rid, admitted_epoch), ...]`; runs without the GIL.
    fn filter_aborted(&self, py: Python<'_>, reqs: Vec<(String, u64)>) -> Vec<String> {
        let inner = Arc::clone(&self.inner);
        py.allow_threads(move || {
            inner.filter_aborted(reqs.iter().map(|(r, e)| (r.as_str(), *e)))
        })
    }

    fn ack(&self, rid: &str) -> bool {
        self.inner.ack(rid)
    }

    fn gc(&self) -> usize {
        self.inner.gc()
    }

    fn stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let s = self.inner.stats();
        let d = PyDict::new_bound(py);
        d.set_item("live_entries", s.live_entries)?;
        d.set_item("total_aborts", s.total_aborts)?;
        d.set_item("total_hits", s.total_hits)?;
        d.set_item("total_acked", s.total_acked)?;
        d.set_item("total_expired", s.total_expired)?;
        d.set_item("epoch", s.epoch)?;
        d.set_item("abort_all_epoch", s.abort_all_epoch)?;
        Ok(d)
    }

    fn __len__(&self) -> usize {
        self.inner.len()
    }

    fn __repr__(&self) -> String {
        let s = self.inner.stats();
        format!(
            "InterruptController(live={}, aborts={}, hits={}, epoch={})",
            s.live_entries, s.total_aborts, s.total_hits, s.epoch
        )
    }
}

#[pymodule]
fn sgl_interrupt(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyInterruptController>()?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}