use actix::{Actor, Addr, Context, Handler};
use std::collections::HashMap;
use sled::IVec;
use super::websocks::{WsSession, InnerMessage, make_key, vectu64, Message};
use actix::prelude::*;
use std::time::Duration;
use serde_json::from_str;

    
#[derive(Message)]
#[rtype(result = "()")]
pub struct RegisterCmd {
    pub topics: Vec<String>,
    pub client_id: String,
    pub offset: u64,
    pub addr: Addr<WsSession>
}


#[derive(Message)]
#[rtype(result = "()")]
pub struct ClearConnCmd {
    pub client_id: String
}


pub struct ConsumerActor {
    pub connection_offset: HashMap<String, u64>,
    pub connection_addr: HashMap<String, Addr<WsSession>>,
    pub connection_count: u16,
    pub connection_topics: HashMap<String, Vec<String>>,
    pub db: sled::Db,
    pub range_idx: sled::Tree,
    pub day_idx: sled::Tree,
    pub main_idx: sled::Tree,
    pub nonce_idx: sled::Tree,
}

impl Handler<RegisterCmd> for ConsumerActor {
    type Result = ();

    fn handle(&mut self, msg: RegisterCmd, _ctx: &mut Self::Context) -> Self::Result {
        let client_id = msg.client_id.as_str();
        println!("consumer:{client_id} subscribe topics:{:?}", msg.topics);
        
        if self.connection_offset.contains_key(client_id){
            *self.connection_offset.get_mut(client_id).unwrap() = msg.offset;
        }else{
            self.connection_offset.insert(client_id.to_string(), msg.offset);
        }
        if self.connection_addr.contains_key(client_id){
            *self.connection_addr.get_mut(client_id).unwrap() = msg.addr;
        }else{
            self.connection_addr.insert(client_id.to_string(), msg.addr);
        }
        if self.connection_topics.contains_key(client_id){
            let mut tps =  self.connection_topics.get_mut(client_id).unwrap();
            for topic in msg.topics {
               tps.insert(0, topic);
            }
        }else{
            self.connection_topics.insert(client_id.to_string(), msg.topics);
        }
        self.connection_count +=1;
    }
}

impl Handler<ClearConnCmd> for ConsumerActor {
    type Result = ();
    fn handle(&mut self, msg: ClearConnCmd, _ctx: &mut Self::Context) {
        let mut ct = 0;
        if self.connection_addr.contains_key(msg.client_id.as_str()){
            self.connection_addr.remove(msg.client_id.as_str());
            ct+=1;
        }
        if self.connection_offset.contains_key(msg.client_id.as_str()){
            self.connection_offset.remove(msg.client_id.as_str());
            ct+=1;
        }
        if self.connection_topics.contains_key(msg.client_id.as_str()){
            self.connection_topics.remove(msg.client_id.as_str());
            ct+=1;
        }
        //self.connection_count = self.connection_count - 1;
        if ct > 2{
            println!("Client:{} Unsubscribe Successful", msg.client_id);
        }
    }
}


impl Actor for ConsumerActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        // println!("consumer started");
        ctx.set_mailbox_capacity(65536);
        ctx.run_later(Duration::from_millis(10), |act, ctx|{
            act.process_message(ctx);
        });
    }
    fn stopping(&mut self, _ctx: &mut Self::Context) -> actix::Running {

        actix::Running::Stop
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {}

    fn start(self) -> Addr<Self>
    where
        Self: Actor<Context = actix::Context<Self>>,
    {
        actix::Context::new().run(self)
    }

    fn create<F>(f: F) -> Addr<Self>
    where
        Self: Actor<Context = actix::Context<Self>>,
        F: FnOnce(&mut actix::Context<Self>) -> Self,
    {
        let mut ctx = actix::Context::new();
        let act = f(&mut ctx);
        ctx.run(act)
    }
}


impl ConsumerActor {
    fn process_message(&mut self, ctx: &mut <ConsumerActor as Actor>::Context) {
        let mut all_count = 0;
        if self.connection_topics.len() < 1 {
            ctx.run_later(Duration::from_millis(500), |act, ctx|{
                act.process_message(ctx); 
            });
            return;
        }
        
        for (cid, ofs) in self.connection_offset.iter_mut() {
            let mut offset = *ofs;
            let mut msg_count = 0;
            if let Some(topics) = self.connection_topics.get(cid){
                for topic in topics {
                    let fetch_flag_min = make_key(topic.as_str(), offset);
                    let fetch_flag_max = make_key(topic.as_str(), u64::MAX);
                    // println!("fetch topic:{} from {}", topic, offset);
                    let mut last_key = IVec::from("");
                    let mut rest_count = 0;
                    for item in self.main_idx.range(fetch_flag_min..fetch_flag_max){
                        if let Ok((_k, data_key)) = item {
                            let k4 = data_key.clone();
                            match self.db.get(data_key){
                                Ok(Some(data))=>{
                                    if let Ok(json_text) = String::from_utf8(data.to_vec()) {
                                        //let the_msg: Message = from_str(json_text.as_str()).unwrap();
                                        if let Some(addr) = self.connection_addr.get(cid){
                                            match addr.try_send(InnerMessage(json_text)) {
                                                Ok(())=>{
                                                    last_key = k4.clone();
                                                    rest_count = rest_count + 1;
                                                    all_count = all_count + 1;
                                                    msg_count = msg_count + 1;
                                                    // println!("{topic}=>{:?}", the_msg.key);
                                                },
                                                Err(err)=>{
                                                    println!("dispatch message to {} with err:{}", cid, err);
                                                }
                                            }
                                        }else{
                                            println!("can not get addr:{cid} msg:{json_text}");
                                        }
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
                        }
                    }//---循环获取消息
                    if rest_count > 0 {
                        let k3 = last_key.clone();
                        //println!("got {rest_count} msg, last key:{:?}", String::from_utf8(k3.clone().to_vec()));
                        match self.nonce_idx.get(last_key){
                            Ok(Some(nonce_ivec))=>{
                                let new_offset = vectu64(nonce_ivec.to_vec());
                                if new_offset > offset{
                                    offset = new_offset;
                                    //println!("processed {} record in topic:{} set offset t0:{}", rest_count, topic, offset);
                                }
                            },
                            Ok(None)=>{
                                eprintln!("can not get nonce with key:{:?}", k3);
                            }
                            Err(err)=>{
                                eprintln!("get nonce error:{}", err);
                            }
                        }
                    }else{
                        //println!("no more message on {topic} offset:{offset}");
                    }
                }
            }
            if msg_count > 0{
                *ofs = offset+1;
            }else{
                //println!("{cid} have no more message");
            }
        }
        
        if all_count > 0{
            ctx.run_later(Duration::from_millis(100), |act, ctx|{
                act.process_message(ctx); 
            });
        }else{
            ctx.run_later(Duration::from_millis(200), |act, ctx|{
                act.process_message(ctx); 
            });
        }
    }
    
}