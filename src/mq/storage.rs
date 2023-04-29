use actix::{ Actor, Context, Handler};
use actix::prelude::*;
use sled::IVec;
use super::websocks::{i64to_vec, today_ts, make_key};

#[derive(Message)]
#[rtype(result = "()")]
pub struct StorageCmd {
    pub st_key: String,
    pub message_topic: String,
    pub nonce: u64,
    pub data: String
}

pub struct StorageActor{
    pub db: sled::Db,
    pub range_idx: sled::Tree,
    pub day_idx: sled::Tree,
    pub main_idx: sled::Tree,
    pub nonce_idx: sled::Tree,
}

impl Actor for StorageActor {
    type Context = Context<Self>;
    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.set_mailbox_capacity(65536);
    }
    
    fn stopped(&mut self, _ctx: &mut Self::Context) {
        self.db.flush().unwrap();
        self.range_idx.flush().unwrap();
        self.day_idx.flush().unwrap();
        self.main_idx.flush().unwrap();
        self.nonce_idx.flush().unwrap();
    }
}

impl Handler<StorageCmd> for StorageActor {
    type Result = ();
    fn handle(&mut self, msg: StorageCmd, _ctx: &mut Self::Context) {
        // let val = serde_json::to_string(message).unwrap();
        let key = msg.st_key;
        let key3 = key.clone();
        let idx_key = Vec::from(msg.nonce.to_be_bytes());
        let today_timestamp_vec = i64to_vec(today_ts());
        // update today's last nonce index
        let new_idx_key = idx_key.clone();
        if let Ok(_k) = self.day_idx.insert(today_timestamp_vec, idx_key) {
            //println!("update today's last nonce success!");
            if let Ok(_) = self.range_idx.insert(new_idx_key, key.as_bytes()) {
                //println!("update range index success!");
                let main_key = make_key(msg.message_topic.as_str(), msg.nonce);
                if let Ok(_) = self.main_idx.insert(main_key, key.as_bytes()) {
                    //println!("update main index success!");
                    let key2 = key.clone();
                    //let val2 = val.clone();
                    if let Ok(_) = self.db.insert(key, msg.data.as_str()) {
                        //println!("insert data success!");
                        if let Ok(_) = self
                            .nonce_idx
                            .insert(key2, IVec::from(msg.nonce.to_be_bytes().to_vec()))
                        {

                        } else {
                            eprintln!("insert nonce idx faild!");
                        }
                    } else {
                        eprintln!("insert data faild!{:?}", key3);
                    }
                } else {
                    eprintln!("update main index faild!");
                }
            } else {
                eprintln!("update range index faild!");
            }
        } else {
            eprintln!("update today's last nonce faild!");
        };
        
    }
}