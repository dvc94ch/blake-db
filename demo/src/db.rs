use anyhow::Result;
use blake_db::ipfs_embed::{Config, Keypair, Multiaddr, PeerId};
use blake_db::{BlakeDb, Document, Ipfs, LocalChange, Patch, Path, Primitive, StreamId, Value};
use futures::prelude::*;
use futures::stream::BoxStream;
use iced::Subscription;
use iced_futures::subscription::Recipe;
use serde_json::json;
use std::borrow::Cow;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;

#[derive(Clone, Debug, Default)]
pub struct Todo {
    pub title: String,
    pub done: bool,
}

impl Todo {
    pub fn new(title: String) -> Self {
        Self { title, done: false }
    }
}

#[derive(Debug)]
pub struct Db {
    doc: Document,
}

impl Db {
    pub async fn new(
        path: PathBuf,
        keypair: Keypair,
        bootstrap: &[(PeerId, Multiaddr)],
    ) -> Result<Self> {
        let config = Config::new(&path, keypair);
        let ipfs = Ipfs::new(config).await?;
        ipfs.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?.next().await;
        ipfs.bootstrap(bootstrap).await?;
        let mut db = BlakeDb::new(ipfs);
        let mut doc = db.document(0).await?;
        if doc.state().get_value(Path::root().key("todos")).is_none() {
            doc.change(|doc| {
                doc.add_change(LocalChange::set(
                    Path::root(),
                    Value::from_json(&json!({ "todos": [] })),
                ))
            })?;
        }
        async_std::task::spawn(async move {
            loop {
                if let Err(err) = db.next().await {
                    tracing::error!("{}", err);
                }
            }
        });
        Ok(Self { doc })
    }

    pub fn stream_id(&self) -> &StreamId {
        self.doc.stream_id()
    }

    pub fn todos(&mut self) -> Vec<Todo> {
        let mut todos = vec![];
        let path = Path::root().key("todos");
        let seq = self.doc.state().get_value(path);
        let seq = if let Some(Cow::Borrowed(Value::Sequence(seq))) = seq {
            seq
        } else {
            return todos;
        };
        todos.reserve(seq.len());
        for val in seq {
            let mut todo = Todo::default();
            if let Value::Map(map) = val {
                if let Some(Value::Primitive(Primitive::Str(title))) = map.get("title") {
                    todo.title = title.to_string();
                }
                if let Some(Value::Primitive(Primitive::Boolean(done))) = map.get("done") {
                    todo.done = *done;
                }
            }
            todos.push(todo);
        }
        todos
    }

    pub fn apply_patch(&mut self, patch: Patch) -> Result<()> {
        self.doc.apply_patch(patch)
    }

    pub fn link(&mut self, id: &StreamId) -> Result<()> {
        self.doc.link(id)
    }

    pub fn add_todo(&mut self, title: &str) -> Result<()> {
        self.doc.change(|doc| {
            doc.add_change(LocalChange::insert(
                Path::root().key("todos").index(0),
                Value::from_json(&json!({ "title": title, "done": false })),
            ))
        })
    }

    pub fn set_title(&mut self, idx: u32, title: &str) -> Result<()> {
        self.doc.change(|doc| {
            doc.add_change(LocalChange::set(
                Path::root().key("todos").index(idx).key("title"),
                Value::Primitive(Primitive::Str(title.into())),
            ))
        })
    }

    pub fn set_done(&mut self, idx: u32, done: bool) -> Result<()> {
        self.doc.change(|doc| {
            doc.add_change(LocalChange::set(
                Path::root().key("todos").index(idx).key("done"),
                Value::Primitive(Primitive::Boolean(done)),
            ))
        })
    }

    pub fn delete_todo(&mut self, idx: u32) -> Result<()> {
        self.doc
            .change(|doc| doc.add_change(LocalChange::delete(Path::root().key("todos").index(idx))))
    }

    pub fn subscription(&self) -> Subscription<Patch> {
        let recipe = DbRecipe(self.doc.patches());
        Subscription::from_recipe(recipe)
    }
}

pub struct DbRecipe(async_channel::Receiver<Patch>);

impl<H: Hasher, E> Recipe<H, E> for DbRecipe {
    type Output = Patch;

    fn hash(&self, state: &mut H) {
        std::any::TypeId::of::<DbRecipe>().hash(state);
    }

    fn stream(self: Box<Self>, _input: BoxStream<'static, E>) -> BoxStream<'static, Self::Output> {
        self.0.boxed()
    }
}
