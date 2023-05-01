extern crate rand;
use rand::distributions::{Distribution, Uniform};
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
use super::storage::{StorageCmd, StorageActor};
use super::consumer::{ConsumerActor, RegisterCmd, ClearConnCmd};

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
    uid: String,
    topic: Option<String>,
    payload: Option<String>,
    pub key: Option<String>,
    cmd: Option<String>,
    params: Option<Vec<String>>,
    offset: Option<u64>,
    nonce: Option<u64>,
}

impl Message {
    fn got_topic(&mut self) -> Option<String> {
        if let Some(topic) = &self.topic {
            Some(topic.clone())
        } else {
            None
        }
    }
    fn got_cmd(&mut self) -> Option<String> {
        if let Some(cmd) = &self.cmd {
            Some(cmd.clone())
        } else {
            None
        }
    }
    fn got_offset(&mut self) -> Option<u64> {
        if let Some(offset) = &self.offset {
            Some(*offset)
        } else {
            None
        }
    }
    fn got_params(&mut self) -> Option<Vec<String>> {
        if let Some(params) = &self.params {
            Some(params.clone())
        } else {
            None
        }
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
    pub topics: Option<Vec<String>>,
    pub db: sled::Db,
    pub offset: u64,
    pub range_idx: sled::Tree,
    pub day_idx: sled::Tree,
    pub main_idx: sled::Tree,
    pub nonce_idx: sled::Tree,
    pub id_generator: IdGenerator,
    //pub conn: Addr<ConnectionActor>,
    pub storage: Vec<Addr<StorageActor>>,
    pub consumers: Vec<Addr<ConsumerActor>>
}

impl Actor for WsSession {
    type Context = ws::WebsocketContext<Self>;
    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.set_mailbox_capacity(65536);
        ctx.text("{\"rs\":true,\"detail\":\"connected\"}");
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
//        if let Some(topics) = &self.topics{
//            for topic in topics.iter(){
//                self.conn.do_send(RemoveCmd{topic: topic.to_string(), client_id: self.client_id.clone()});
//            }
//        }
        for consumer in self.consumers.iter(){
            match consumer.try_send(ClearConnCmd{client_id: self.client_id.clone()}){
                Ok(())=>{},
                Err(err)=>{
                    eprintln!("Unregist connection {} with err {}", self.client_id, err);
                }
            }
        }
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
                        self.process_message(&mut msg, &text, ctx);
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
    fn process_message(&mut self, message: &mut Message, raw_msg:&str, ctx: &mut <WsSession as Actor>::Context) {
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
                println!("process subscribe on {} consumers", self.consumers.len());
                let topics = Some(params);
                self.topics = topics.clone();
                if offset > 0 {
                    self.offset = offset;
                }
                // --- 找一个连接数最小的consumer来注册连接
                let mut rng = rand::thread_rng();
                let die = Uniform::from(0..self.consumers.len());
                let st_idx = die.sample(&mut rng);
                let consumer_addr_opt = self.consumers.get(st_idx);
                if let Some(consumer_addr) = consumer_addr_opt {
                    consumer_addr.try_send(RegisterCmd{
                        client_id: self.client_id.clone(),
                        offset,
                        addr: ctx.address(),
                        topics: topics.unwrap()
                    }).expect("Regist conn faild");
                }else{
                    println!("get consumer faild");
                }
                ctx.text("{\"rs\":true,\"detail\":\"Subscribe Success\"}");
            }
        }
        if let Some(_) = message.got_topic() {
            // if got topic, it's a message, run dispatch!
            self.dispatch_message(message, raw_msg);
        }
    }

    fn dispatch_message(&mut self, message: &mut Message, raw_msg: &str) {
        let nonce = self.id_generator.gen_id();
        message.nonce = Some(nonce);
        let message_topic = message.got_topic().unwrap();
        let topic = message_topic.clone();
        let key = format!("{}-{}", message_topic, message.uid);
        //self.conn.do_send(MsgCmd{topic: message_topic, msg: raw_msg.to_string()});
        let cmd = StorageCmd {
            st_key: key,
            message_topic: topic,
            nonce: nonce,
            data: raw_msg.to_string()
        };
        let mut rng = rand::thread_rng();
        let die = Uniform::from(0..self.storage.len());
        let st_idx = die.sample(&mut rng);
        let storage_addr_opt = self.storage.get(st_idx);
        if let Some(storage_addr) = storage_addr_opt{
            // println!("will insert msg:{} with none :{} in consumer:{}", key2, nonce, st_idx);
            storage_addr.try_send(cmd).expect("Insert Faild");
        }

    }
}
