use anyhow::Result;
use automerge::{Backend, Frontend, InvalidChangeRequest, MutableDocument};
use automerge_protocol::{ActorId, Change, Op};
use blake_streams::{BlakeStreams, Head, StreamWriter};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use fnv::FnvHashMap;
use async_channel::{Receiver, Sender};
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
struct StreamEvent {
    /// The parsed stream op.
    op: StreamOp,
    /// The offset of the op.
    offset: u64,
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
    /// The next op number.
    ops: u64,
    /// The offset that was synced.
    sync_offset: u64,
    /// The offset that was processed.
    offset: u64,
    /// Queue of unprocessed events.
    queue: VecDeque<StreamEvent>,
    /// Queue of streams blocked by this stream.
    dependents: VecDeque<StreamId>,
}

impl StreamState {
    fn new(doc: u64) -> Self {
        Self {
            doc,
            streams: Default::default(),
            seq: 1,
            ops: 1,
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
}

impl DocumentState {
    pub fn new(tx: Sender<Patch>, write: StreamWriter) -> Self {
        Self {
            tx,
            write: BufWriter::new(write),
            backend: Backend::new(),
            streams: Default::default(),
        }
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
        self.frontend.apply_patch(patch)?;
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
        if write.head().len() == 0 {
            write.commit().await?;
        }
        let id = *write.id();
        let (patch_tx, patch_rx) = async_channel::unbounded();
        let doc = DocumentState::new(patch_tx, write);
        self.docs.insert(id.stream(), doc);
        Ok(Document::new(id, self.tx.clone(), patch_rx))
    }

    async fn link(&mut self, id: u64, stream: &StreamId) -> Result<()> {
        if stream.peer() == self.inner.ipfs().local_public_key() {
            return Ok(());
        }
        let doc = self.docs.get_mut(&id).unwrap();
        if doc.streams.contains_key(stream) {
            return Ok(());
        }
        tracing::info!("doc {}: linking {}", id, stream);
        let index = doc.streams.len() as u64;
        doc.streams.insert(*stream, index);
        StreamOp::Link(*stream).write_to(&mut doc.write)?;
        doc.write.flush()?;
        doc.write.get_mut().commit().await?;
        self.streams.insert(*stream, StreamState::new(id));
        let subscription = self.inner.subscribe(stream).await?;
        self.heads.push(Box::pin(subscription));
        Ok(())
    }

    async fn change(&mut self, id: u64, change: Change) -> Result<()> {
        let ops = change.operations.clone();
        let doc = self.docs.get_mut(&id).unwrap();
        let (patch, _change) = doc.backend.apply_local_change(change)?;
        doc.tx.try_send(patch)?;
        let mut deps = vec![];
        for (id, index) in &doc.streams {
            let stream = self.streams.get(id).unwrap();
            // TODO only write changed offsets.
            deps.push(StreamOp::Depend(*index, stream.offset));
        }
        for dep in &deps {
            dep.write_to(&mut doc.write)?;
        }
        StreamOp::Change(ops).write_to(&mut doc.write)?;
        doc.write.flush()?;
        doc.write.get_mut().commit().await?;
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
        let mut reader = self.inner.slice(&id, start, len)?;
        let mut pos = 0;
        while pos < len {
            let op = StreamOp::read_one(&mut reader, &mut self.buf)?;
            if let StreamOp::Link(id) = &op {
                self.link(doc, id).await?;
            }
            pos += self.buf.len() as u64 + 8;
            let event = StreamEvent {
                op,
                offset: start + pos,
            };
            self.apply_event(&id, event, false)?;
        }
        self.streams.get_mut(&id).unwrap().sync_offset = head.len;
        Ok(())
    }

    fn apply_event(&mut self, id: &StreamId, ev: StreamEvent, skip_queue: bool) -> Result<()> {
        tracing::info!("{} {:?}", id, ev);
        let entry = self.streams.get_mut(id).unwrap();
        if !entry.queue.is_empty() && !skip_queue {
            entry.queue.push_back(ev);
            return Ok(());
        }
        let mut entry = self.streams.remove(&id).unwrap();
        entry.offset = ev.offset;
        match ev.op {
            StreamOp::Link(id) => {
                entry.streams.push(id);
            }
            StreamOp::Depend(idx, offset) => {
                if let Some(dep_id) = entry.streams.get_mut(idx as usize) {
                    // Is None if the dep_id == self
                    if let Some(dep) = self.streams.get_mut(dep_id) {
                        if offset > dep.offset {
                            tracing::info!("{} depends on {} {}", id, dep_id, offset);
                            debug_assert!(entry.queue.is_empty());
                            entry.queue.push_back(ev);
                            dep.dependents.push_back(*id);
                        }
                    }
                }
            }
            StreamOp::Change(ops) => {
                let change = Change {
                    actor_id: ActorId::from_bytes(id.as_bytes()),
                    seq: entry.seq,
                    start_op: entry.ops,
                    time: 0,
                    message: None,
                    hash: None,
                    deps: vec![],
                    operations: ops,
                    extra_bytes: vec![],
                };
                entry.seq += 1;
                entry.ops += change.operations.len() as u64;
                let doc = self.docs.get_mut(&entry.doc).unwrap();
                let patch = doc.backend.apply_changes(vec![change.into()])?;
                doc.tx.try_send(patch)?;
            }
        };
        while let Some(id) = entry.dependents.pop_front() {
            let rdep = self.streams.get_mut(&id).unwrap();
            let event = rdep.queue.pop_front().unwrap();
            if event.offset > entry.offset {
                entry.dependents.push_front(id);
                rdep.queue.push_front(event);
                break;
            }
            self.apply_event(&id, event, true)?;
        }
        self.streams.insert(*id, entry);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::path::PathBuf;
    use tempdir::TempDir;

    fn tracing_try_init() {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .try_init()
            .ok();
    }

    async fn create_swarm(path: PathBuf) -> Result<BlakeDb> {
        std::fs::create_dir_all(&path)?;
        let mut config = Config::new(&path, generate_keypair());
        config.network.broadcast = None;
        let ipfs = Ipfs::new(config).await?;
        ipfs.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?.next().await;
        let streams = BlakeStreams::new(ipfs);
        let db = BlakeDb::new(streams);
        Ok(db)
    }

    #[async_std::test]
    async fn test_cards() -> Result<()> {
        tracing_try_init();
        let tmp = TempDir::new("test_cards")?;

        let bootstrap = create_swarm(tmp.path().join("bootstrap")).await?;
        let addr = bootstrap.ipfs().listeners()[0].clone();
        let peer_id = bootstrap.ipfs().local_peer_id();
        let nodes = [(peer_id, addr)];

        let mut db1 = create_swarm(tmp.path().join("a")).await?;
        db1.ipfs().bootstrap(&nodes).await?;
        let mut db2 = create_swarm(tmp.path().join("b")).await?;
        db2.ipfs().bootstrap(&nodes).await?;

        let mut doc1 = db1.document(0).await?;
        let mut doc2 = db2.document(0).await?;

        async_std::task::spawn(async move {
            loop {
                db1.next().await.unwrap();
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
        println!("{}", serde_json::to_string(&doc1.state().to_json()).unwrap());

        Ok(())
    }
}
