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
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use super::conn_mng::{AppendCmd, RemoveCmd, MsgCmd, ClearCmd, ConnectionActor};
use super::storage::{StorageCmd, StorageActor};

#[derive(Debug, PartialEq)]
struct TransactionError;

fn got_timestamp() -> u128 {
    let now = SystemTime::now();
    let timestamp = now
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis();
    timestamp
}

fn diff_timestamp(last: u128) -> u128 {
    got_timestamp() - last
}

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
    key: Option<String>,
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
    pub conn: Addr<ConnectionActor>,
    pub storage: Vec<Addr<StorageActor>>
}

impl Actor for WsSession {
    type Context = ws::WebsocketContext<Self>;
    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.text("{\"rs\":true,\"detail\":\"connected\"}");
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        if let Some(topics) = &self.topics{
            for topic in topics.iter(){
                self.conn.do_send(RemoveCmd{topic: topic.to_string(), client_id: self.client_id.clone()});
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
    fn try_fetch_topics(&mut self, ctx: &mut <WsSession as Actor>::Context) {
        // execute every 200ms
        if let Some(topics) = &self.topics {
            // if subscribe some topics
            let start_ts = got_timestamp();
            let clear_cmd = ClearCmd {
                topics: topics.clone(),
                client_id: self.client_id.clone()
            };
            self.conn.do_send(clear_cmd);
            let rest_count = 0;

            let init_offset = self.offset;
            for topic in topics {
                let fetch_flag_min = make_key(topic.as_str(), init_offset);
                let fetch_flag_max = make_key(topic.as_str(), u64::MAX);
                println!("fetch topic:{} from {}", topic, init_offset);
                let mut data_key2: IVec = IVec::from("");
                for item in self.main_idx.range(fetch_flag_min..fetch_flag_max){
                    let rest_count = rest_count + 1;
                    if let Ok((_k, data_key)) = item {
                        data_key2 = data_key.clone();
                        let k4 = data_key.clone();
                        match self.db.get(data_key){
                            Ok(Some(data))=>{
                                if let Ok(json_text) = String::from_utf8(data.to_vec()) {
                                    ctx.text(json_text);
                                    //println!("retain message diliverd:{:?}", data_key2.clone());
                                } else {
                                    println!("invalid json");
                                }
                            },
                            Ok(None)=>{
                                println!("get data None with key:{}", String::from_utf8(k4.to_vec()).unwrap());
                            },
                            Err(err)=>{
                                println!("get data with err:{}", err);
                            }
                        }
                    } else {
                        println!("get iter faild");
                    }
                }
                if rest_count > 0 {
                    let k3 = data_key2.clone();
                    match self.nonce_idx.get(data_key2){
                        Ok(Some(nonce_ivec))=>{
                            let offset = vectu64(nonce_ivec.to_vec());
                            if offset > self.offset {
                                self.offset = offset;
                                println!("processed {} record in topic:{} set offset t0:{}", rest_count, topic, offset);
                            }
                        },
                        Ok(None)=>{
                            eprintln!("can not get nonce with key:{:?}", k3);
                        }
                        Err(err)=>{
                            eprintln!("get nonce error:{}", err);
                        }
                    }
                }
            }

            let diff_ts = diff_timestamp(start_ts);
            if diff_ts >= 199 {
                eprintln!("slow {diff_ts} ms")
            }
            if rest_count < 1{
                println!("no more retain message regist conn");
                for topic in topics{
                    self.conn.do_send(AppendCmd{topic: topic.to_string(), client_id: self.client_id.clone(), addr: ctx.address()});
                }
            }else{
                println!("retry {:?} with rest_count {}", topics, rest_count);
                ctx.run_later(Duration::from_millis(1), |act, ctx|{
                    act.try_fetch_topics(ctx);
                });
            }
        }
    }

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
                self.topics = Some(params);
                if offset > 0 {
                    self.offset = offset;
                }
                self.try_fetch_topics(ctx);
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
        self.conn.do_send(MsgCmd{topic: message_topic, msg: raw_msg.to_string()});
        let cmd = StorageCmd {
            st_key: key,
            message_topic: topic,
            nonce: nonce,
            data: raw_msg.to_string()
        };
        let mut rng = rand::thread_rng();
        let die = Uniform::from(0..256);
        let st_idx = die.sample(&mut rng);
        let storage_addr_opt = self.storage.get(st_idx);
        if let Some(storage_addr) = storage_addr_opt{
            storage_addr.try_send(cmd).expect("Insert Faild");
        }
    }
}
