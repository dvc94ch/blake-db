use anyhow::Result;
use automerge::{Backend, Frontend, InvalidChangeRequest, MutableDocument, Value};
use automerge_protocol::{ActorId, Change, Op};
use blake_streams::{BlakeStreams, Head, Ipfs, StreamId, StreamWriter};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use fnv::FnvHashMap;
use futures::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::io::{BufWriter, Read, Write};
use std::pin::Pin;
use unicycle::StreamsUnordered;
use zerocopy::AsBytes;

#[derive(Debug, Deserialize, Serialize)]
pub enum StreamOp {
    Link(StreamId),
    Depend(u64, u64),
    Change(Vec<Op>),
}

#[derive(Debug)]
pub struct StreamEvent {
    op: StreamOp,
    offset: u64,
}

struct StreamState {
    index: u64,
    streams: Vec<StreamId>,
    seq: u64,
    ops: u64,
    sync_offset: u64,
    offset: u64,
    queue: VecDeque<StreamEvent>,
    dependents: VecDeque<StreamId>,
}

impl StreamState {
    pub fn new(index: u64) -> Self {
        Self {
            index,
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

pub struct State {
    id: StreamId,
    streams: FnvHashMap<StreamId, StreamState>,
    backend: Backend,
    frontend: Frontend,
}

impl State {
    pub fn new(id: StreamId) -> Self {
        Self {
            id,
            streams: Default::default(),
            backend: Backend::new(),
            frontend: Frontend::new_with_timestamper_and_actor_id(Box::new(|| None), id.as_bytes()),
        }
    }

    pub fn apply_event(&mut self, id: &StreamId, ev: StreamEvent, skip_queue: bool) -> Result<()> {
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
                    tracing::info!("dep {}", dep_id);
                    // Is None if the dep_id == self
                    if let Some(dep) = self.streams.get_mut(dep_id) {
                        if offset > dep.offset {
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
                let patch = self.backend.apply_changes(vec![change.into()])?;
                self.frontend.apply_patch(patch)?;
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

pub struct BlakeDb {
    state: State,
    streams: BlakeStreams,
    write: BufWriter<StreamWriter>,
    read: StreamsUnordered<Pin<Box<dyn Stream<Item = Head> + Send + 'static>>>,
}

impl BlakeDb {
    pub async fn new(streams: BlakeStreams) -> Result<Self> {
        let key = streams.ipfs().local_public_key();
        let id = StreamId::new(key.to_bytes(), 0);
        let mut write = streams.append(0).await?;
        write.commit().await?;
        Ok(Self {
            state: State::new(id),
            streams,
            write: BufWriter::new(write),
            read: StreamsUnordered::new(),
        })
    }

    pub fn stream_id(&self) -> &StreamId {
        &self.state.id
    }

    pub fn ipfs(&self) -> &Ipfs {
        self.streams.ipfs()
    }

    fn write_op(&mut self, op: &StreamOp) -> Result<()> {
        let bytes = serde_cbor::to_vec(&op)?;
        self.write.write_u64::<BigEndian>(bytes.len() as u64)?;
        self.write.write_all(&bytes)?;
        tracing::info!("writing {} bytes", bytes.len() + 8);
        Ok(())
    }

    async fn commit(&mut self) -> Result<()> {
        tracing::info!("commit");
        self.write.flush()?;
        self.write.get_mut().commit().await
    }

    pub async fn link(&mut self, id: &StreamId) -> Result<()> {
        if self.state.streams.contains_key(id) || id == self.stream_id() {
            return Ok(());
        }
        tracing::info!("linking {}", id);
        let idx = self.state.streams.len() as u64;
        self.write_op(&StreamOp::Link(*id))?;
        self.commit().await?;
        self.state.streams.insert(*id, StreamState::new(idx));
        let subscription = self.streams.subscribe(id).await?;
        self.read.push(Box::pin(subscription));
        Ok(())
    }

    pub async fn change<F, O>(&mut self, cb: F) -> Result<O>
    where
        F: FnOnce(&mut dyn MutableDocument) -> Result<O, InvalidChangeRequest>,
    {
        let (output, change) = self.state.frontend.change(None, cb)?;
        if let Some(change) = change {
            let ops = change.operations.clone();
            let (patch, _change) = self.state.backend.apply_local_change(change)?;
            self.state.frontend.apply_patch(patch)?;
            let mut deps = vec![];
            for (_, stream) in &self.state.streams {
                // TODO only write changed offsets
                deps.push(StreamOp::Depend(stream.index, stream.offset));
            }
            for dep in &deps {
                self.write_op(dep)?;
            }
            self.write_op(&StreamOp::Change(ops))?;
            self.commit().await?;
        }
        Ok(output)
    }

    pub async fn next(&mut self) -> Result<()> {
        tracing::info!("next");
        let mut buf = vec![];
        if let Some(head) = self.read.next().await {
            tracing::info!("got head");
            let id = head.id;
            let start = self
                .state
                .streams
                .get(&id)
                .map(|stream| stream.sync_offset)
                .unwrap_or_default();
            let len = head.len - start;
            let mut reader = self.streams.slice(&id, start, len)?;
            let mut pos = 0;
            tracing::info!("start {} len {} offset {}", start, len, head.len);
            while pos < len {
                let len = reader.read_u64::<BigEndian>()?;
                buf.clear();
                tracing::info!("len {}", len);
                buf.reserve(len as usize);
                buf.resize(len as usize, 0);
                reader.read_exact(&mut buf)?;
                let op: StreamOp = serde_cbor::from_slice(&buf)?;
                if let StreamOp::Link(id) = &op {
                    self.link(id).await?;
                }
                pos += 8 + len;
                let event = StreamEvent {
                    op,
                    offset: start + pos,
                };
                self.state.apply_event(&id, event, false)?;
            }
            self.state.streams.get_mut(&id).unwrap().sync_offset = head.len;
        }
        Ok(())
    }

    pub fn state(&mut self) -> &Value {
        self.state.frontend.state()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use automerge::{LocalChange, Path, Primitive, Value};
    use ipfs_embed::{generate_keypair, Config};
    use serde_json::json;
    use std::path::PathBuf;
    use tempdir::TempDir;

    fn tracing_try_init() {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .try_init()
            .ok();
    }

    async fn create_swarm(path: PathBuf) -> Result<BlakeStreams> {
        std::fs::create_dir_all(&path)?;
        let mut config = Config::new(&path, generate_keypair());
        config.network.broadcast = None;
        let ipfs = Ipfs::new(config).await?;
        ipfs.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?.next().await;
        Ok(BlakeStreams::new(ipfs))
    }

    #[async_std::test]
    async fn test_cards() -> Result<()> {
        tracing_try_init();
        let tmp = TempDir::new("test_cards")?;

        let bootstrap = create_swarm(tmp.path().join("bootstrap")).await?;
        let addr = bootstrap.ipfs().listeners()[0].clone();
        let peer_id = bootstrap.ipfs().local_peer_id();
        let nodes = [(peer_id, addr)];

        let doc1 = create_swarm(tmp.path().join("a")).await?;
        doc1.ipfs().bootstrap(&nodes).await?;
        let doc2 = create_swarm(tmp.path().join("b")).await?;
        doc2.ipfs().bootstrap(&nodes).await?;

        let mut doc1 = BlakeDb::new(doc1).await?;
        let mut doc2 = BlakeDb::new(doc2).await?;

        doc1.link(&doc2.stream_id()).await?;
        doc2.link(&doc1.stream_id()).await?;

        doc1.next().await?;
        doc2.next().await?;

        tracing::info!("Initial state");
        doc1.change(|doc| {
            doc.add_change(LocalChange::set(
                Path::root(),
                Value::from_json(&json!({ "cards": [] })),
            ))
        })
        .await?;
        tracing::info!(
            "{}",
            serde_json::to_string(&doc1.state().to_json()).unwrap()
        );
        doc2.next().await?;
        tracing::info!(
            "{}",
            serde_json::to_string(&doc2.state().to_json()).unwrap()
        );

        tracing::info!("Add card");
        doc1.change(|doc| {
            doc.add_change(LocalChange::insert(
                Path::root().key("cards").index(0),
                Value::from_json(
                    &json!({ "title": "Rewrite everything in Clojure", "done": false }),
                ),
            ))
        })
        .await?;
        tracing::info!(
            "{}",
            serde_json::to_string(&doc1.state().to_json()).unwrap()
        );
        doc2.next().await?;
        tracing::info!(
            "{}",
            serde_json::to_string(&doc2.state().to_json()).unwrap()
        );

        tracing::info!("Add another card");
        doc1.change(|doc| {
            doc.add_change(LocalChange::insert(
                Path::root().key("cards").index(0),
                Value::from_json(
                    &json!({ "title": "Rewrite everything in Haskell", "done": false }),
                ),
            ))
        })
        .await?;
        doc2.next().await?;

        tracing::info!("Mark card as done");
        doc1.change(|doc| {
            doc.add_change(LocalChange::set(
                Path::root().key("cards").index(0).key("done"),
                Value::Primitive(Primitive::Boolean(true)),
            ))
        })
        .await?;

        tracing::info!("Delete card");
        doc2.change(|doc| doc.add_change(LocalChange::delete(Path::root().key("cards").index(1))))
            .await?;

        doc1.next().await?;
        doc2.next().await?;

        // TODO wait for convergence
        tracing::info!(
            "{}",
            serde_json::to_string(&doc1.state().to_json()).unwrap()
        );
        tracing::info!(
            "{}",
            serde_json::to_string(&doc2.state().to_json()).unwrap()
        );

        Ok(())
    }
}
