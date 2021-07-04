use anyhow::Result;
use async_channel::{Receiver, Sender};
use automerge::{Backend, Frontend, InvalidChangeRequest, MutableDocument};
use automerge_protocol::{ActorId, Change, Op};
use blake_streams::{BlakeStreams, Head, StreamWriter};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use fnv::FnvHashMap;
use futures::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::io::{BufWriter, Read, Write};
use std::pin::Pin;
use unicycle::StreamsUnordered;
use zerocopy::AsBytes;

pub use automerge::{LocalChange, Path, Primitive, Value};
pub use automerge_protocol::Patch;
pub use blake_streams::{ipfs_embed, Ipfs, StreamId};

/// A blake stream contains stream ops.
#[derive(Debug, Deserialize, Serialize)]
enum StreamOp {
    /// Links a stream to another stream.
    Link(StreamId),
    /// A causal dependency identified by the
    /// local stream index and offset.
    Depend(u64, u64),
    /// The operations to perform on a document.
    Change(Vec<Op>),
}

impl StreamOp {
    fn write_to<W: Write>(&self, w: &mut W) -> Result<()> {
        let bytes = serde_cbor::to_vec(&self)?;
        w.write_u64::<BigEndian>(bytes.len() as u64)?;
        w.write_all(&bytes)?;
        Ok(())
    }

    fn read_one<R: Read>(r: &mut R, buf: &mut Vec<u8>) -> Result<Self> {
        let len = r.read_u64::<BigEndian>()?;
        buf.clear();
        buf.reserve(len as usize);
        buf.resize(len as usize, 0);
        r.read_exact(buf)?;
        let op = serde_cbor::from_slice(&buf)?;
        Ok(op)
    }
}

/// Queued stream event.
#[derive(Debug)]
enum QueueOp {
    Link(u64),
    Depend(u64, StreamId, u64),
    Change(u64, Vec<Op>),
}

/// State of a stream.
#[derive(Debug)]
struct StreamState {
    /// Document id.
    doc: u64,
    /// The list of streams this stream has linked to.
    streams: Vec<StreamId>,
    /// The next sequence number.
    seq: u64,
    /// The offset that was synced.
    sync_offset: u64,
    /// The offset that was processed.
    offset: u64,
    /// Queue of unprocessed events.
    queue: VecDeque<QueueOp>,
    /// Queue of streams blocked by this stream.
    dependents: VecDeque<StreamId>,
}

impl StreamState {
    fn new(doc: u64) -> Self {
        Self {
            doc,
            streams: Default::default(),
            seq: 1,
            sync_offset: 0,
            offset: 0,
            queue: Default::default(),
            dependents: Default::default(),
        }
    }
}

/// Frontend initiated operations that are performed
/// on a document.
#[derive(Debug)]
enum DocOp {
    /// Make a change to the document.
    Change(u64, Change),
    /// Link to another stream.
    Link(u64, StreamId),
}

/// State of a document.
struct DocumentState {
    /// Send patches to the frontend.
    tx: Sender<Patch>,
    /// Stream writer to write stream ops to.
    write: BufWriter<StreamWriter>,
    /// Document backend.
    backend: Backend,
    /// Streams that are linked to this document.
    streams: FnvHashMap<StreamId, u64>,
    /// The next op number.
    ops: u64,
}

impl DocumentState {
    pub fn new(tx: Sender<Patch>, write: StreamWriter) -> Self {
        Self {
            tx,
            write: BufWriter::new(write),
            backend: Backend::new(),
            streams: Default::default(),
            ops: 1,
        }
    }

    pub async fn commit(&mut self) -> Result<()> {
        self.write.flush()?;
        self.write.get_mut().commit().await?;
        Ok(())
    }
}

/// Document handle.
#[derive(Debug)]
pub struct Document {
    /// Local stream id.
    id: StreamId,
    /// State of the document.
    frontend: Frontend,
    /// Send document operations to the backend.
    tx: Sender<DocOp>,
    /// Receive patches from the backend.
    rx: Receiver<Patch>,
}

impl Document {
    fn new(id: StreamId, tx: Sender<DocOp>, rx: Receiver<Patch>) -> Self {
        Self {
            id,
            frontend: Frontend::new_with_timestamper_and_actor_id(Box::new(|| None), id.as_bytes()),
            tx,
            rx,
        }
    }

    pub fn stream_id(&self) -> &StreamId {
        &self.id
    }

    pub fn link(&mut self, id: &StreamId) -> Result<()> {
        self.tx.try_send(DocOp::Link(self.id.stream(), *id))?;
        Ok(())
    }

    pub fn change<F, O>(&mut self, cb: F) -> Result<O>
    where
        F: FnOnce(&mut dyn MutableDocument) -> Result<O, InvalidChangeRequest>,
    {
        let (output, change) = self.frontend.change(None, cb)?;
        if let Some(change) = change {
            self.tx.try_send(DocOp::Change(self.id.stream(), change))?;
        }
        Ok(output)
    }

    pub fn state(&mut self) -> &Value {
        self.frontend.state()
    }

    pub fn patches(&self) -> Receiver<Patch> {
        self.rx.clone()
    }

    pub fn apply_patch(&mut self, patch: Patch) -> Result<()> {
        tracing::info!("applying patch");
        self.frontend.apply_patch(patch)?;
        Ok(())
    }

    pub async fn next(&mut self) -> Result<()> {
        if let Some(patch) = self.rx.next().await {
            self.apply_patch(patch)?;
        }
        Ok(())
    }
}

pub struct BlakeDb {
    inner: BlakeStreams,
    streams: FnvHashMap<StreamId, StreamState>,
    docs: FnvHashMap<u64, DocumentState>,
    heads: StreamsUnordered<Pin<Box<dyn Stream<Item = Head> + Send + 'static>>>,
    buf: Vec<u8>,
    tx: Sender<DocOp>,
    /// Receive commands from the frontends.
    rx: Receiver<DocOp>,
}

impl BlakeDb {
    pub fn new(inner: Ipfs) -> Self {
        let (tx, rx) = async_channel::unbounded();
        Self {
            inner: BlakeStreams::new(inner),
            streams: Default::default(),
            docs: Default::default(),
            heads: StreamsUnordered::new(),
            buf: vec![],
            tx,
            rx,
        }
    }

    pub fn ipfs(&self) -> &Ipfs {
        self.inner.ipfs()
    }

    pub async fn document(&mut self, id: u64) -> Result<Document> {
        let mut write = self.inner.append(id).await?;
        let head = *write.head();
        if head.len() == 0 {
            write.commit().await?;
        }
        let (patch_tx, patch_rx) = async_channel::unbounded();
        let doc = DocumentState::new(patch_tx, write);
        self.docs.insert(head.id().stream(), doc);
        if head.len() != 0 {
            self.streams
                .insert(*head.id(), StreamState::new(head.id().stream()));
            self.apply_events(id, *head.id(), 0, head.len())?;
            let links = self.streams.get(head.id()).unwrap().streams.clone();
            for link in links {
                if let Some(head) = self.inner.head(&link)? {
                    self.apply_events(id, *head.id(), 0, head.len())?;
                }
            }
            let stream = self.streams.remove(head.id()).unwrap();
            assert!(stream.queue.is_empty());
        }
        let mut doc = Document::new(*head.id(), self.tx.clone(), patch_rx);
        while doc.next().now_or_never().is_some() {}
        Ok(doc)
    }

    async fn link(&mut self, id: u64, stream: &StreamId) -> Result<()> {
        if stream.peer() == self.inner.ipfs().local_public_key() {
            return Ok(());
        }
        if self.streams.contains_key(stream) {
            return Ok(());
        }
        tracing::info!("doc {}: linking {}", id, stream);
        let doc = self.docs.get_mut(&id).unwrap();
        let index = doc.streams.len() as u64;
        doc.streams.insert(*stream, index);
        self.streams.insert(*stream, StreamState::new(id));
        StreamOp::Link(*stream).write_to(&mut doc.write)?;
        doc.commit().await?;
        let subscription = self.inner.subscribe(stream).await?;
        self.heads.push(Box::pin(subscription));
        Ok(())
    }

    async fn change(&mut self, id: u64, change: Change) -> Result<()> {
        tracing::debug!("doc: {} local_change: {:?}", id, change);
        let ops = change.operations.clone();
        let doc = self.docs.get_mut(&id).unwrap();
        let (patch, _change) = doc.backend.apply_local_change(change)?;
        doc.tx.try_send(patch)?;
        let mut deps = vec![];
        for (id, index) in &doc.streams {
            if let Some(stream) = self.streams.get(id) {
                // TODO only write changed offsets.
                deps.push(StreamOp::Depend(*index, stream.offset));
            } else {
                tracing::error!("missing stream {}", id);
            }
        }
        for dep in &deps {
            dep.write_to(&mut doc.write)?;
        }
        doc.ops += ops.len() as u64;
        StreamOp::Change(ops).write_to(&mut doc.write)?;
        doc.commit().await?;
        Ok(())
    }

    pub async fn next(&mut self) -> Result<()> {
        futures::select! {
            head = self.heads.next().fuse() => {
                if let Some(head) = head {
                    self.new_head(head).await?;
                }
            }
            cmd = self.rx.next().fuse() => {
                match cmd {
                    Some(DocOp::Link(id, stream)) => self.link(id, &stream).await?,
                    Some(DocOp::Change(id, change)) => self.change(id, change).await?,
                    None => {}
                }
            }
        }
        Ok(())
    }

    async fn new_head(&mut self, head: Head) -> Result<()> {
        let id = head.id;
        let stream = self.streams.get(&id).unwrap();
        let doc = stream.doc;
        let start = stream.sync_offset;
        let len = head.len - start;
        tracing::info!("start {} len {} offset {}", start, len, head.len);
        self.apply_events(doc, id, start, len)?;
        self.streams.get_mut(&id).unwrap().sync_offset = head.len;
        Ok(())
    }

    fn apply_events(&mut self, doc: u64, id: StreamId, start: u64, len: u64) -> Result<()> {
        let mut reader = self.inner.slice(&id, start, len)?;
        let mut pos = 0;
        while pos < len {
            let op = StreamOp::read_one(&mut reader, &mut self.buf)?;
            pos += self.buf.len() as u64 + 8;
            self.apply_op(doc, id, op, start + pos)?;
        }
        Ok(())
    }

    fn apply_op(&mut self, doc: u64, id: StreamId, op: StreamOp, offset: u64) -> Result<()> {
        self.queue_op(&id, op, offset);
        let streams = self.docs.get(&doc).unwrap().streams.clone();
        let mut progress = true;
        while progress {
            progress = false;
            for (id, _) in &streams {
                while self.dequeue_op(id)? {
                    progress = true;
                }
            }
            if !progress {
                break;
            }
        }
        Ok(())
    }

    fn queue_op(&mut self, id: &StreamId, op: StreamOp, offset: u64) {
        let entry = self.streams.get_mut(&id).unwrap();
        match op {
            StreamOp::Link(dep) => {
                entry.streams.push(dep);
                tracing::info!(
                    "linking {} to {} with index {}",
                    id,
                    dep,
                    entry.streams.len()
                );
                entry.queue.push_back(QueueOp::Link(offset));
            }
            StreamOp::Depend(dep_idx, dep_offset) => {
                let dep_id = *entry.streams.get(dep_idx as usize).unwrap();
                entry
                    .queue
                    .push_back(QueueOp::Depend(offset, dep_id, dep_offset));
            }
            StreamOp::Change(ops) => {
                entry.queue.push_back(QueueOp::Change(offset, ops));
            }
        }
    }

    fn dequeue_op(&mut self, id: &StreamId) -> Result<bool> {
        let entry = self.streams.get_mut(id).unwrap();
        match entry.queue.pop_front() {
            Some(QueueOp::Link(offset)) => {
                entry.offset = offset;
                Ok(true)
            }
            Some(QueueOp::Depend(offset, dep_id, dep_offset)) => {
                // `None` if dep_id is our write stream.
                if let Some(dep) = self.streams.get_mut(&dep_id) {
                    if dep_offset > dep.offset {
                        tracing::info!("{} depends on {} {}", id, dep_id, dep_offset);
                        let stream = self.streams.get_mut(id).unwrap();
                        stream
                            .queue
                            .push_front(QueueOp::Depend(offset, dep_id, dep_offset));
                        return Ok(false);
                    }
                }
                self.streams.get_mut(id).unwrap().offset = offset;
                Ok(true)
            }
            Some(QueueOp::Change(offset, ops)) => {
                let doc = self.docs.get_mut(&entry.doc).unwrap();
                let change = Change {
                    actor_id: ActorId::from_bytes(id.as_bytes()),
                    seq: entry.seq,
                    start_op: doc.ops,
                    time: 0,
                    message: None,
                    hash: None,
                    deps: vec![],
                    operations: ops,
                    extra_bytes: vec![],
                };
                entry.seq += 1;
                doc.ops += change.operations.len() as u64;
                entry.offset = offset;
                tracing::info!("applying change {} {}", id, offset);
                let patch = doc.backend.apply_changes(vec![change.into()])?;
                doc.tx.try_send(patch)?;
                Ok(true)
            }
            None => Ok(false),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::channel::oneshot;
    use ipfs_embed::{generate_keypair, Config, Keypair};
    use serde_json::json;
    use std::path::PathBuf;
    use tempdir::TempDir;

    fn tracing_try_init() {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .try_init()
            .ok();
    }

    async fn create_swarm(path: PathBuf, keypair: Keypair) -> Result<BlakeDb> {
        let mut config = Config::new(&path, keypair);
        config.network.broadcast = None;
        let ipfs = Ipfs::new(config).await?;
        ipfs.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?.next().await;
        let db = BlakeDb::new(ipfs);
        Ok(db)
    }

    #[async_std::test]
    async fn test_cards() -> Result<()> {
        tracing_try_init();
        let tmp = TempDir::new("test_cards")?;

        let bootstrap = create_swarm(tmp.path().join("bootstrap"), generate_keypair()).await?;
        let addr = bootstrap.ipfs().listeners()[0].clone();
        let peer_id = bootstrap.ipfs().local_peer_id();
        let nodes = [(peer_id, addr)];

        let key = generate_keypair();
        let key2 = Keypair::from_bytes(&key.to_bytes()).unwrap();
        let mut db1 = create_swarm(tmp.path().join("a"), key2).await?;
        db1.ipfs().bootstrap(&nodes).await?;
        let mut db2 = create_swarm(tmp.path().join("b"), generate_keypair()).await?;
        db2.ipfs().bootstrap(&nodes).await?;

        let mut doc1 = db1.document(0).await?;
        let mut doc2 = db2.document(0).await?;

        let (exit_tx, mut exit_rx) = oneshot::channel();
        async_std::task::spawn(async move {
            let exit = &mut exit_rx;
            loop {
                futures::select! {
                    _ = exit.fuse() => {
                        break;
                    }
                    next = db1.next().fuse() => {
                        next.unwrap();
                    }
                }
            }
        });

        async_std::task::spawn(async move {
            loop {
                db2.next().await.unwrap();
            }
        });

        doc1.link(&doc2.stream_id())?;
        doc2.link(&doc1.stream_id())?;

        doc1.change(|doc| {
            doc.add_change(LocalChange::set(
                Path::root(),
                Value::from_json(&json!({ "cards": [] })),
            ))
        })?;

        doc1.next().await?;
        doc2.next().await?;

        doc1.change(|doc| {
            doc.add_change(LocalChange::insert(
                Path::root().key("cards").index(0),
                Value::from_json(
                    &json!({ "title": "Rewrite everything in Clojure", "done": false }),
                ),
            ))
        })?;
        doc1.next().await?;
        doc2.next().await?;

        doc1.change(|doc| {
            doc.add_change(LocalChange::insert(
                Path::root().key("cards").index(0),
                Value::from_json(
                    &json!({ "title": "Rewrite everything in Haskell", "done": false }),
                ),
            ))
        })?;
        doc1.next().await?;
        doc2.next().await?;

        doc1.change(|doc| {
            doc.add_change(LocalChange::set(
                Path::root().key("cards").index(0).key("done"),
                Value::Primitive(Primitive::Boolean(true)),
            ))
        })?;
        doc1.next().await?;
        doc2.next().await?;

        doc2.change(|doc| doc.add_change(LocalChange::delete(Path::root().key("cards").index(1))))?;
        doc1.next().await?;
        doc2.next().await?;

        assert_eq!(doc1.state(), doc2.state());
        println!(
            "{}",
            serde_json::to_string(&doc1.state().to_json()).unwrap()
        );

        exit_tx.send(()).unwrap();

        let mut db1 = create_swarm(tmp.path().join("a"), key).await?;
        db1.ipfs().bootstrap(&nodes).await?;
        let mut doc1 = db1.document(0).await?;

        async_std::task::spawn(async move {
            loop {
                db1.next().await.unwrap();
            }
        });

        assert_eq!(doc1.state(), doc2.state());

        Ok(())
    }
}
