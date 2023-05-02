extern crate rand;
use actix::prelude::*;
use actix::{Actor, StreamHandler};
use actix_web_actors::ws;
use chrono::prelude::*;
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use sled::IVec;
use std::sync::{Arc, Mutex};
// use std::time::{SystemTime, UNIX_EPOCH};
// use super::conn_mng::{AppendCmd, RemoveCmd, MsgCmd, ClearCmd, ConnectionActor};
use super::partition::PartitionDispacher;

#[derive(Debug, PartialEq)]
struct TransactionError;

//fn got_timestamp() -> u128 {
//    let now = SystemTime::now();
//    let timestamp = now
//        .duration_since(UNIX_EPOCH)
//        .expect("Time went backwards")
//        .as_millis();
//    timestamp
//}
//
//fn diff_timestamp(last: u128) -> u128 {
//    got_timestamp() - last
//}

pub fn today_ts() -> i64 {
    let today = NaiveDate::from_ymd_opt(
        chrono::Local::now().year(),
        chrono::Local::now().month(),
        chrono::Local::now().day(),
    )
    .unwrap();
    let dt = chrono::NaiveDateTime::new(today, chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap());
    let timestamp = dt.timestamp();
    timestamp
}

pub fn i64to_vec(n: i64) -> Vec<u8> {
    Vec::from(n.to_be_bytes())
}

pub fn vectu64(vc: Vec<u8>) -> u64 {
    u64::from_be_bytes(vc.try_into().unwrap())
}

pub fn make_key(topic: &str, nonce: u64) -> IVec {
    let mut nonce_vec = Vec::from(nonce.to_be_bytes());
    let mut topic_vec = Vec::from(topic.as_bytes());
    topic_vec.append(&mut nonce_vec);
    IVec::from(topic_vec)
}

#[derive(Debug, Clone)]
pub struct IdGenerator {
    max_id: Arc<Mutex<u64>>,
}

impl IdGenerator {
    pub fn new(init_id: u64) -> IdGenerator {
        IdGenerator {
            max_id: Arc::new(Mutex::new(init_id)),
        }
    }

    pub fn init_with(&self, init_id: u64) {
        let mut max_id = self.max_id.lock().unwrap();
        *max_id = init_id;
    }

    pub fn gen_id(&self) -> u64 {
        let mut max_id = self.max_id.lock().unwrap();
        *max_id += 1;
        let new_id = *max_id;
        new_id
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Message {
    pub uid: String,
    pub topic: Option<String>,
    pub payload: Option<String>,
    pub key: Option<String>,
    pub cmd: Option<String>,
    pub params: Option<Vec<String>>,
    pub offset: Option<u64>,
    pub nonce: Option<u64>,
}

impl Message {
    pub fn got_topic(&mut self) -> Option<String> {
        if let Some(topic) = &self.topic {
            Some(topic.clone())
        } else {
            None
        }
    }
    pub fn got_cmd(&mut self) -> Option<String> {
        if let Some(cmd) = &self.cmd {
            Some(cmd.clone())
        } else {
            None
        }
    }
    pub fn got_offset(&mut self) -> Option<u64> {
        if let Some(offset) = &self.offset {
            Some(*offset)
        } else {
            None
        }
    }
    pub fn got_params(&mut self) -> Option<Vec<String>> {
        if let Some(params) = &self.params {
            Some(params.clone())
        } else {
            None
        }
    }
    pub fn set_nonce(&mut self, nonce: u64) {
        self.nonce = Some(nonce);
    }
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct InnerMessage(pub String);


#[derive(Serialize, Deserialize)]
pub struct Command {
    cmd: String,
    params: Vec<String>,
}

#[derive(Serialize, Deserialize)]
pub struct ErrResp {
    pub rs: bool,
    pub detail: String,
}

pub struct WsSession {
    pub client_id: String,
    pub dispacher: PartitionDispacher
}

impl Actor for WsSession {
    type Context = ws::WebsocketContext<Self>;
    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.set_mailbox_capacity(65536);
        ctx.text("{\"rs\":true,\"detail\":\"connected\"}");
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        self.dispacher.unsubscribe(self.client_id.as_str());
        println!("client:{} disconnected", self.client_id);
    }
}

impl Handler<InnerMessage> for WsSession {
    type Result = ();

    fn handle(&mut self, msg: InnerMessage, ctx: &mut Self::Context) {
        ctx.text(msg.0);
    }
}

/// Handler for ws::Message message
impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for WsSession {
    fn handle(&mut self, msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        match msg {
            Ok(ws::Message::Ping(msg)) => ctx.pong(&msg),
            Ok(ws::Message::Text(text)) => {
                match serde_json::from_str::<Message>(&text) {
                    Ok(msg) => {
                        //println!("Got Event to:{:?}", msg.topic);
                        let mut msg = msg;
                        self.process_message(&mut msg, ctx);
                    }
                    Err(err) => {
                        println!("Invalid Message:{} error:{}", text, err);
                        ctx.text(
                            serde_json::to_string(&ErrResp {
                                rs: false,
                                detail: format!("Invalid json:{err}"),
                            })
                            .unwrap(),
                        )
                    }
                };
            }
            Ok(ws::Message::Binary(bin)) => ctx.binary(bin),
            Ok(ws::Message::Close(_)) => {
                ctx.close(Some(ws::CloseReason {
                    code: ws::CloseCode::Normal,
                    description: Some("reset by peer".to_string()),
                }));
            }
            _ => (),
        }
    }
}

impl WsSession {
    fn process_message(&mut self, message: &mut Message, ctx: &mut <WsSession as Actor>::Context) {
        if let Some(cmd) = message.got_cmd() {
            // this is a command
            let command = cmd.clone();
            let command_str = command.as_str();
            let params = if let Some(p) = message.got_params() {
                p
            } else {
                vec![]
            };
            let offset = if let Some(off) = message.got_offset() {
                off
            } else {
                0 as u64
            };
            //self.process_command(command_str, params, offset);
            if command_str == "subscribe" {
                let topics = params;
                self.dispacher.subscribe(ctx.address(), self.client_id.as_str(), topics, offset);
                ctx.text("{\"rs\":true,\"detail\":\"Subscribe Success\"}");
            }
        }
        if let Some(_) = message.got_topic() {
            // if got topic, it's a message, run dispatch!
            self.dispatch_message(message);
        }
    }

    fn dispatch_message(&mut self, message: &mut Message) {
        self.dispacher.dispach_message(message);
    }
}
