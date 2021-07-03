use crate::{db, icons, style};
use iced::{button, text_input, Align, Button, Checkbox, Element, Length, Row, Text, TextInput};

#[derive(Debug, Clone)]
pub struct Task {
    db: db::Todo,
    ui: TaskState,
}

impl From<db::Todo> for Task {
    fn from(db: db::Todo) -> Self {
        Self {
            db,
            ui: TaskState::Idle {
                edit_button: button::State::new(),
            },
        }
    }
}

#[derive(Debug, Clone)]
pub enum TaskState {
    Idle {
        edit_button: button::State,
    },
    Editing {
        text_input: text_input::State,
        delete_button: button::State,
    },
}

impl Default for TaskState {
    fn default() -> Self {
        TaskState::Idle {
            edit_button: button::State::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum TaskMessage {
    Edit,
    EditDone,
    SetDone(bool),
    SetTitle(String),
    Delete,
}

impl Task {
    pub fn new(title: String) -> Self {
        Self::from(db::Todo::new(title))
    }

    pub fn done(&self) -> bool {
        self.db.done
    }

    pub fn set_state(&mut self, todo: db::Todo) {
        self.db = todo;
    }

    pub fn update(&mut self, message: TaskMessage) {
        match message {
            TaskMessage::Edit => {
                self.ui = TaskState::Editing {
                    text_input: text_input::State::focused(),
                    delete_button: button::State::new(),
                };
            }
            TaskMessage::EditDone => {
                if !self.db.title.is_empty() {
                    self.ui = TaskState::Idle {
                        edit_button: button::State::new(),
                    }
                }
            }
            TaskMessage::SetDone(done) => {
                self.db.done = done;
            }

            TaskMessage::SetTitle(title) => {
                self.db.title = title;
            }
            TaskMessage::Delete => {}
        }
    }

    pub fn view(&mut self) -> Element<TaskMessage> {
        match &mut self.ui {
            TaskState::Idle { edit_button } => {
                let checkbox = Checkbox::new(self.db.done, &self.db.title, TaskMessage::SetDone)
                    .width(Length::Fill);
                let edit = Button::new(edit_button, icons::edit_icon())
                    .on_press(TaskMessage::Edit)
                    .padding(10)
                    .style(style::Button::Icon);
                Row::new()
                    .spacing(20)
                    .align_items(Align::Center)
                    .push(checkbox)
                    .push(edit)
                    .into()
            }
            TaskState::Editing {
                text_input,
                delete_button,
            } => {
                let text_input = TextInput::new(
                    text_input,
                    "Describe your task...",
                    &self.db.title,
                    TaskMessage::SetTitle,
                )
                .on_submit(TaskMessage::EditDone)
                .padding(10);
                let delete = Button::new(
                    delete_button,
                    Row::new()
                        .spacing(10)
                        .push(icons::delete_icon())
                        .push(Text::new("Delete")),
                )
                .on_press(TaskMessage::Delete)
                .padding(10)
                .style(style::Button::Destructive);
                Row::new()
                    .spacing(20)
                    .align_items(Align::Center)
                    .push(text_input)
                    .push(delete)
                    .into()
            }
        }
    }
}
