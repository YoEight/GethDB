use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use bytes::{BufMut, Bytes, BytesMut};
use proptest::collection::vec;
use proptest::prelude::{any, Strategy};
use proptest::prop_compose;

use crate::entry::Entry;
use crate::{CommandDispatch, RaftCommand, RaftSender, Request, UserCommand};

mod sm;
mod storage;

prop_compose! {
    pub fn arb_entry(index_range: impl Strategy<Value = u64>)(
        index in index_range,
        term in 1u64..=100,
        payload in vec(any::<u8>(), 0 ..= 10),
    ) -> Entry {
        Entry {
            index,
            term,
            payload: Bytes::from(payload),
        }
    }
}

prop_compose! {
    pub fn arb_entries(index_range: impl Strategy<Value = u64>)(
        mut entries in vec(arb_entry(index_range), 0 ..= 100),
    ) -> Vec<Entry> {
        entries.sort_by(|a: &Entry, b| (a.index, a.term).cmp(&(b.index, b.term)));
        entries.dedup_by(|a, b| a.index == b.index);
        entries
    }
}

#[derive(PartialEq, Eq, Copy, Clone, Debug)]
pub enum TestCommandKind {
    Read,
    Write,
}

#[derive(Clone)]
pub struct TestCommand {
    pub reject: Arc<AtomicBool>,
    pub kind: TestCommandKind,
}

impl TestCommand {
    pub fn write_command() -> Self {
        Self {
            reject: Arc::new(Default::default()),
            kind: TestCommandKind::Write,
        }
    }

    pub fn is_rejected(&self) -> bool {
        self.reject.load(Ordering::SeqCst)
    }
}

impl RaftCommand for TestCommand {
    fn write(&self, buffer: &mut BytesMut) {
        let value = match self.kind {
            TestCommandKind::Read => "read",
            TestCommandKind::Write => "write",
        };

        let bytes = value.as_bytes();
        buffer.put_u64_le(bytes.len() as u64);
        buffer.put_slice(bytes);
    }
}

impl UserCommand for TestCommand {
    fn is_read(&self) -> bool {
        self.kind == TestCommandKind::Read
    }

    fn reject(self) {
        let _ = self
            .reject
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst);
    }
}

pub struct TestRequest<A> {
    pub target: A,
    pub request: Request<A>,
}

pub struct TestSender<A> {
    inner: Arc<Mutex<Vec<TestRequest<A>>>>,
}

impl<A> TestSender<A> {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn take(&self) -> Vec<TestRequest<A>> {
        let mut inner = self.inner.lock().unwrap();
        let result = std::mem::take(inner.as_mut());

        result
    }
}

impl<NodeId> RaftSender for TestSender<NodeId>
where
    NodeId: Ord,
{
    type Id = NodeId;

    fn send(&self, target: Self::Id, request: Request<Self::Id>) {
        let mut inner = self.inner.lock().unwrap();
        inner.push(TestRequest { target, request });
    }
}

pub struct TestDispatch<A> {
    inner: Arc<Mutex<Vec<A>>>,
}

impl<A> TestDispatch<A> {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn take(&self) -> Vec<A> {
        let mut inner = self.inner.lock().unwrap();
        let result = std::mem::take(inner.as_mut());

        result
    }
}

impl<A> CommandDispatch for TestDispatch<A>
where
    A: UserCommand,
{
    type Command = A;

    fn dispatch(&self, cmd: Self::Command) {
        let mut inner = self.inner.lock().unwrap();
        inner.push(cmd);
    }
}
