//! `TableHandle` / `ReducerHandle` / `ViewHandle` — fluent chaining over a
//! shared `PipelineInner`. Mirrors the TS handles in
//! `bindings/typescript/settle/src/pipeline.ts:114`.

use std::cell::RefCell;
use std::rc::Rc;

use super::PipelineInner;
use super::ddl::{ReducerOptions, ViewOptions};

/// Handle returned by [`crate::pipeline::Pipeline::table`].
#[derive(Clone)]
pub struct TableHandle {
    inner: Rc<RefCell<PipelineInner>>,
    name: String,
}

impl TableHandle {
    pub(crate) fn new(inner: Rc<RefCell<PipelineInner>>, name: String) -> Self {
        Self { inner, name }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Mark the underlying table as VIRTUAL after creation. Equivalent to
    /// passing `{ virtual: true }` at `table()` time.
    pub fn set_virtual(self, value: bool) -> Self {
        self.inner.borrow_mut().mark_virtual(&self.name, value);
        self
    }

    pub fn create_reducer(
        &self,
        name: impl Into<String>,
        opts: ReducerOptions,
    ) -> ReducerHandle {
        let reducer_name = name.into();
        self.inner
            .borrow_mut()
            .add_reducer(reducer_name.clone(), self.name.clone(), opts);
        ReducerHandle::new(self.inner.clone(), reducer_name)
    }

    pub fn create_view(&self, name: impl Into<String>, opts: ViewOptions) -> ViewHandle {
        let view_name = name.into();
        self.inner
            .borrow_mut()
            .add_view(view_name.clone(), self.name.clone(), opts);
        ViewHandle::new(view_name)
    }
}

/// Handle returned by [`TableHandle::create_reducer`] /
/// [`ReducerHandle::create_reducer`].
#[derive(Clone)]
pub struct ReducerHandle {
    inner: Rc<RefCell<PipelineInner>>,
    name: String,
}

impl ReducerHandle {
    pub(crate) fn new(inner: Rc<RefCell<PipelineInner>>, name: String) -> Self {
        Self { inner, name }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn create_reducer(
        &self,
        name: impl Into<String>,
        opts: ReducerOptions,
    ) -> ReducerHandle {
        let reducer_name = name.into();
        self.inner
            .borrow_mut()
            .add_reducer(reducer_name.clone(), self.name.clone(), opts);
        ReducerHandle::new(self.inner.clone(), reducer_name)
    }

    pub fn create_view(&self, name: impl Into<String>, opts: ViewOptions) -> ViewHandle {
        let view_name = name.into();
        self.inner
            .borrow_mut()
            .add_view(view_name.clone(), self.name.clone(), opts);
        ViewHandle::new(view_name)
    }
}

/// Terminal handle returned by [`TableHandle::create_view`] /
/// [`ReducerHandle::create_view`]. Holds only the view name.
#[derive(Clone)]
pub struct ViewHandle {
    name: String,
}

impl ViewHandle {
    pub(crate) fn new(name: String) -> Self {
        Self { name }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}
