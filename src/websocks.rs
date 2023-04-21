use actix::{Actor, StreamHandler};
use actix::prelude::*;
use actix_web_actors::ws;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};
use chrono::NaiveDate;
use chrono::prelude::*;
use sled::IVec;

#[derive(Debug, PartialEq)]
struct TransactionError;

fn got_timestamp() -> u128 {
    let now = SystemTime::now();
    let timestamp = now.duration_since(UNIX_EPOCH).expect("Time went backwards").as_millis();
    timestamp
}

fn diff_timestamp(last: u128) -> u128 {
    got_timestamp() - last
}

pub fn today_ts() -> i64{
    let today = NaiveDate::from_ymd_opt(
        chrono::Local::now().year(),
        chrono::Local::now().month(),
        chrono::Local::now().day()
    ).unwrap();
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
    max_id: Arc<Mutex<u64>>
}

impl IdGenerator {
    pub fn new(init_id: u64) -> IdGenerator {
        IdGenerator { 
            max_id: Arc::new(Mutex::new(init_id))
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
    nonce: Option<u64>
}

impl Message {
    fn got_topic(&mut self) -> Option<String>{
        if let Some(topic) = &self.topic{
            Some(topic.clone())
        }else{
            None
        }
    }
    fn got_cmd(&mut self) -> Option<String>{
        if let Some(cmd) = &self.cmd{
            Some(cmd.clone())
        }else{
            None
        }
    }
    fn got_offset(&mut self) -> Option<u64>{
        if let Some(offset) = &self.offset{
            Some(*offset)
        }else{
            None
        }
    }
    fn got_params(&mut self) -> Option<Vec<String>>{
        if let Some(params) = &self.params{
            Some(params.clone())
        }else{
            None
        }
    }
}


#[derive(Serialize, Deserialize)]
pub struct Command {
    cmd: String,
    params: Vec<String>,
}


#[derive(Serialize, Deserialize)]
pub struct ErrResp {
    pub rs: bool,
    pub detail: String
}

#[derive(Message)]
#[rtype(result = "()")]
struct InnerMessage(String);

pub struct WsSession{
    pub client_id: String,
    pub topics: Option<Vec<String>>,
    pub db: sled::Db,
    pub offset: u64,
    pub range_idx: sled::Tree,
    pub day_idx: sled::Tree,
    pub main_idx: sled::Tree,
    pub nonce_idx: sled::Tree,
    pub id_generator: IdGenerator
}

impl Actor for WsSession {
    type Context = ws::WebsocketContext<Self>;
    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.text("{\"rs\":true,\"detail\":\"connected\"}");
        ctx.run_interval(Duration::from_millis(200), |act, ctx|{
            act.every_200_ms(ctx);
        });
       
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        println!("client:{} disconnected", self.client_id);
    }

}


/// Handler for ws::Message message
impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for WsSession {
    fn handle(&mut self, msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        match msg {
            Ok(ws::Message::Ping(msg)) => ctx.pong(&msg),
            Ok(ws::Message::Text(text)) => {
                match serde_json::from_str::<Message>(&text){
                    Ok(msg)=>{
                        // println!("Got Event to:{:?}", msg);
                        let mut msg = msg;
                        self.process_message(&mut msg);
                    }
                    Err(err)=>{
                        println!("Invalid Message:{} error:{}", text, err);
                        ctx.text(serde_json::to_string(&ErrResp{rs: false, detail: format!("Invalid json:{err}")}).unwrap())
                    }
                };
            },
            Ok(ws::Message::Binary(bin)) => ctx.binary(bin),
            Ok(ws::Message::Close(_))=>{ctx.close(Some(ws::CloseReason{code:ws::CloseCode::Normal, description: Some("reset by peer".to_string())}));}
            _ => (),
        }
    }
}

impl WsSession {
    fn every_200_ms(&mut self, ctx: &mut <WsSession as Actor>::Context){
        // execute every 200ms
        if let Some(topics) = &self.topics{
            // if subscribe some topics
            let start_ts = got_timestamp();
            for topic in topics{
                let fetch_flag_min = make_key(topic.as_str(), self.offset);
                let fetch_flag_max = make_key(topic.as_str(), u64::MAX);
                for item in self.main_idx.range(fetch_flag_min..fetch_flag_max) {
                    if let Ok((_k, data_key)) = item{
                        let data_key2 = data_key.clone();
                        if let Ok(Some(data)) = self.db.get(data_key){
                            if let Ok(json_text) = String::from_utf8(data.to_vec()){
                                if let Ok(Some(nonce_ivec)) = self.nonce_idx.get(data_key2){
                                    let offset = vectu64(nonce_ivec.to_vec());
                                    self.offset = offset + 1;
                                }
                                ctx.text(json_text);
                            }
                        }
                    }
                }
            }
            let diff_ts = diff_timestamp(start_ts);
            if diff_ts >= 199 {
                println!("slow {diff_ts} ms")
            }
        }
    }
    
    fn process_message(&mut self, message: &mut Message) {
        if let Some(cmd) = message.got_cmd() {
            // this is a command
            let command = cmd.clone();
            let command_str = command.as_str();
            let params = if let Some(p) = message.got_params() { p } else { vec![] };
            let offset = if let Some(off) = message.got_offset() { off } else { 0 as u64};
            //self.process_command(command_str, params, offset);
            if command_str == "subscribe" {
                self.topics = Some(params);
                if offset > 0{
                    self.offset = offset;
                }
            }
        }
        if let Some(_) = message.got_topic() {
            // if got topic, it's a message, run dispatch!
            self.dispatch_message(message);
        }
    }

    fn dispatch_message(&mut self, message: &mut Message) {
        let nonce = self.id_generator.gen_id();
        message.nonce = Some(nonce);
        let message_topic = message.got_topic().unwrap();
        let key = format!("{}-{}", message_topic, message.uid);
        let val = serde_json::to_string(message).unwrap();
        let idx_key = Vec::from(nonce.to_be_bytes());
        let today_timestamp_vec = i64to_vec(today_ts());
        // update today's last nonce index
        let new_idx_key = idx_key.clone();
        if let Ok(_k) = self.day_idx.insert(today_timestamp_vec, idx_key){
            //println!("update today's last nonce success!");
            if let Ok(_) = self.range_idx.insert(new_idx_key, key.as_bytes()){
                //println!("update range index success!");
                let main_key = make_key(message_topic.as_str(), nonce);
                if let Ok(_) = self.main_idx.insert(main_key, key.as_bytes()) {
                    //println!("update main index success!");
                    let key2 = key.clone();
                    if let Ok(_) = self.db.insert(key, val.into_bytes()){
                        //println!("insert data success!");
                        if let Ok(_) = self.nonce_idx.insert(key2, IVec::from(nonce.to_be_bytes().to_vec())){
                            //println!("insert nonce idx success!");
                        }else{
                            println!("insert nonce idx faild!");
                        }
                        
                    }else{
                        eprintln!("insert data faild!");
                    }
                }else{
                    eprintln!("update main index faild!");
                }
            }else{
                eprintln!("update range index faild!");
            }
        }else{
            eprintln!("update today's last nonce faild!");
        };
    }
    
}