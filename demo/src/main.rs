use anyhow::Result;
use blake_db::{
    ipfs_embed::{Keypair, PublicKey, SecretKey},
    Patch,
};
use blake_db_demo::{
    db::Db,
    style,
    task::{Task, TaskMessage},
};
use iced::{
    button, scrollable, text_input, Align, Application, Button, Clipboard, Column, Command,
    Container, Element, HorizontalAlignment, Length, Row, Scrollable, Settings, Subscription, Text,
    TextInput,
};
use std::path::Path;

#[async_std::main]
pub async fn main() -> Result<()> {
    let i: u8 = std::env::args().nth(1).unwrap().parse()?;
    let path = Path::new("/tmp").join(i.to_string());
    let secret = SecretKey::from_bytes(&[i; 32])?;
    let public = PublicKey::from(&secret);
    let keypair = Keypair { secret, public };
    let db = Db::new(path, keypair, &[]).await?;
    Todos::run(Settings::with_flags(db))?;
    Ok(())
}

#[derive(Debug)]
struct Todos {
    scroll: scrollable::State,
    input: text_input::State,
    input_value: String,
    filter: Filter,
    tasks: Vec<Task>,
    controls: Controls,
    db: Db,
}

impl Todos {
    fn new(mut db: Db) -> Self {
        Self {
            scroll: Default::default(),
            input: Default::default(),
            input_value: Default::default(),
            filter: Default::default(),
            controls: Default::default(),
            tasks: db.todos().into_iter().map(From::from).collect(),
            db,
        }
    }
}

#[derive(Debug, Clone)]
enum Message {
    InputChanged(String),
    CreateTask,
    FilterChanged(Filter),
    TaskMessage(usize, TaskMessage),
    ApplyPatch(Patch),
}

impl Application for Todos {
    type Executor = iced::executor::Default;
    type Message = Message;
    type Flags = Db;

    fn new(flags: Db) -> (Todos, Command<Message>) {
        (Self::new(flags), Command::none())
    }

    fn title(&self) -> String {
        format!("blake-db todo demo")
    }

    fn update(&mut self, message: Message, _clipboard: &mut Clipboard) -> Command<Message> {
        match message {
            Message::InputChanged(value) => {
                self.input_value = value;
            }
            Message::CreateTask => {
                if !self.input_value.is_empty() {
                    let title = self.input_value.clone();
                    if let Err(err) = self.db.add_todo(&title) {
                        tracing::error!("err: {}", err);
                    } else {
                        self.input_value.clear();
                        self.tasks.push(Task::new(title));
                    }
                }
            }
            Message::FilterChanged(filter) => {
                self.filter = filter;
            }
            Message::TaskMessage(i, TaskMessage::SetDone(done)) => {
                if let Err(err) = self.db.set_done(i as u32, done) {
                    tracing::error!("err: {}", err);
                } else {
                    if let Some(task) = self.tasks.get_mut(i) {
                        task.update(TaskMessage::SetDone(done));
                    }
                }
            }
            Message::TaskMessage(i, TaskMessage::SetTitle(title)) => {
                if let Err(err) = self.db.set_title(i as u32, &title) {
                    tracing::error!("err: {}", err);
                } else {
                    if let Some(task) = self.tasks.get_mut(i) {
                        task.update(TaskMessage::SetTitle(title));
                    }
                }
            }
            Message::TaskMessage(i, TaskMessage::Delete) => {
                if let Err(err) = self.db.delete_todo(i as u32) {
                    tracing::error!("err: {}", err);
                } else {
                    self.tasks.remove(i);
                }
            }
            Message::TaskMessage(i, task_message) => {
                if let Some(task) = self.tasks.get_mut(i) {
                    task.update(task_message);
                }
            }
            Message::ApplyPatch(patch) => {
                if let Err(err) = self.db.apply_patch(patch) {
                    tracing::error!("err: {}", err);
                } else {
                    let todos = self.db.todos();
                    self.tasks.truncate(todos.len());
                    for (i, todo) in todos.into_iter().enumerate() {
                        if let Some(task) = self.tasks.get_mut(i) {
                            task.set_state(todo);
                        } else {
                            self.tasks.push(todo.into());
                        }
                    }
                }
            }
        }
        Command::none()
    }

    fn subscription(&self) -> Subscription<Message> {
        self.db.subscription().map(Message::ApplyPatch)
    }

    fn view(&mut self) -> Element<Message> {
        let title = Text::new("todos")
            .width(Length::Fill)
            .size(100)
            .color([0.5, 0.5, 0.5])
            .horizontal_alignment(HorizontalAlignment::Center);

        let input = TextInput::new(
            &mut self.input,
            "What needs to be done?",
            &self.input_value,
            Message::InputChanged,
        )
        .padding(15)
        .size(30)
        .on_submit(Message::CreateTask);

        let filter = self.filter;
        let controls = self.controls.view(&self.tasks, filter);
        let filtered_tasks = self.tasks.iter().filter(|task| filter.matches(task));

        let tasks: Element<_> = if filtered_tasks.count() > 0 {
            self.tasks
                .iter_mut()
                .enumerate()
                .filter(|(_, task)| filter.matches(task))
                .fold(Column::new().spacing(20), |column, (i, task)| {
                    column.push(
                        task.view()
                            .map(move |message| Message::TaskMessage(i, message)),
                    )
                })
                .into()
        } else {
            empty_message(match filter {
                Filter::All => "You have not created a task yet...",
                Filter::Active => "All your tasks are done! :D",
                Filter::Completed => "You have not completed a task yet...",
            })
        };

        let content = Column::new()
            .max_width(800)
            .spacing(20)
            .push(title)
            .push(input)
            .push(controls)
            .push(tasks);

        Scrollable::new(&mut self.scroll)
            .padding(40)
            .push(Container::new(content).width(Length::Fill).center_x())
            .into()
    }
}

fn empty_message<'a>(message: &str) -> Element<'a, Message> {
    Container::new(
        Text::new(message)
            .width(Length::Fill)
            .size(25)
            .horizontal_alignment(HorizontalAlignment::Center)
            .color([0.7, 0.7, 0.7]),
    )
    .width(Length::Fill)
    .height(Length::Units(200))
    .center_y()
    .into()
}

#[derive(Debug, Default, Clone)]
struct Controls {
    all_button: button::State,
    active_button: button::State,
    completed_button: button::State,
}

impl Controls {
    fn view(&mut self, tasks: &[Task], current_filter: Filter) -> Row<Message> {
        let Controls {
            all_button,
            active_button,
            completed_button,
        } = self;

        let tasks_left = tasks.iter().filter(|task| !task.done()).count();

        let filter_button = |state, label, filter, current_filter| {
            let label = Text::new(label).size(16);
            let button = Button::new(state, label).style(style::Button::Filter {
                selected: filter == current_filter,
            });

            button.on_press(Message::FilterChanged(filter)).padding(8)
        };

        Row::new()
            .spacing(20)
            .align_items(Align::Center)
            .push(
                Text::new(&format!(
                    "{} {} left",
                    tasks_left,
                    if tasks_left == 1 { "task" } else { "tasks" }
                ))
                .width(Length::Fill)
                .size(16),
            )
            .push(
                Row::new()
                    .width(Length::Shrink)
                    .spacing(10)
                    .push(filter_button(
                        all_button,
                        "All",
                        Filter::All,
                        current_filter,
                    ))
                    .push(filter_button(
                        active_button,
                        "Active",
                        Filter::Active,
                        current_filter,
                    ))
                    .push(filter_button(
                        completed_button,
                        "Completed",
                        Filter::Completed,
                        current_filter,
                    )),
            )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Filter {
    All,
    Active,
    Completed,
}

impl Default for Filter {
    fn default() -> Self {
        Filter::All
    }
}

impl Filter {
    fn matches(&self, task: &Task) -> bool {
        match self {
            Filter::All => true,
            Filter::Active => !task.done(),
            Filter::Completed => task.done(),
        }
    }
}
