use actix::{ Actor, Context, Handler};
use actix::dev::{MessageResponse};
use actix::prelude::*;
use sled::IVec;
use std::time::Duration;
use super::websocks::{i64to_vec, today_ts, make_key, vectu64};

#[derive(Message)]
#[rtype(result = "()")]
pub struct StorageCmd {
    pub st_key: String,
    pub message_topic: String,
    pub nonce: u64,
    pub data: String
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct TrimCmd {
    pub days: u16
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct StatusCmd {
    pub days: u16
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


impl Handler<TrimCmd> for StorageActor {
    type Result = ();
    fn handle(&mut self, msg: TrimCmd, _ctx: &mut Self::Context) -> Self::Result {
        let days = msg.days;
        let target_timestamp = today_ts() - (86400 as i64 * (days + 1) as i64) as i64;
        println!("tar ts: {target_timestamp:?}");
        if let Ok(Some(v)) = self.day_idx.get(i64to_vec(target_timestamp)){
            println!("Got nonce:{v:?}");
            for item in self.range_idx.range(..v){
                if let Ok((rkey, data_key)) = item {
                    let data_key2 = data_key.clone();
                    let data_key_for_split = data_key.clone();
                    println!("Matched key:「{data_key:?}」");
                    if let Ok(_) = self.db.remove(data_key){
                        println!("removed from storage");
                    }
                    let str_key = String::from_utf8(data_key_for_split.to_vec().try_into().unwrap()).unwrap();
                    let topic = str_key.split('-').next().unwrap();
                    let nonce = vectu64(rkey.to_vec());
                    let main_key = make_key(topic, nonce);
                    if let Ok(_) = self.main_idx.remove(main_key){
                        println!("removed from main index");
                    }
                    if let Ok(_) = self.range_idx.remove(rkey){
                        println!("removed from range index");
                    }
                    if let Ok(_) = self.nonce_idx.remove(data_key2){
                        println!("removed from nonce index");
                    }
                }
            }
        }
    }

}

impl Handler<StorageCmd> for StorageActor {
    ///
    ///   day_idx -   today_ts last -> nonce
    ///   range_idx - nonce as key -> data_key
    ///   data  -     data_key -> raw_msg
    ///   nonce -     data_key -> nonce
    ///   main_idx -  main_key -> data_key
    ///
    type Result = ();
    fn handle(&mut self, msg: StorageCmd, ctx: &mut Self::Context) {
        // let val = serde_json::to_string(message).unwrap();
        let data_key = msg.st_key;
        // let log_data_key = data_key.clone();
        let data_key_in_range_idx = data_key.clone();
        let data_key_as_nonce_idx_key = data_key.clone();
        let data_key_as_main_idx_val = data_key.clone();
        let nonce_as_key = Vec::from(msg.nonce.to_be_bytes());
        let nonce_in_day_idx = nonce_as_key.clone();
        let today_timestamp_vec = i64to_vec(today_ts());
        // update today's last nonce index
        let main_key = make_key(msg.message_topic.as_str(), msg.nonce);

        // println!("insert {:?} with nonce {}", log_data_key, msg.nonce);
        if let Ok(_k) = self.day_idx.insert(today_timestamp_vec, nonce_in_day_idx) {
            //println!("update today's last nonce success!");
            if let Ok(_) = self.range_idx.insert(nonce_as_key, data_key_in_range_idx.as_bytes()) {
                if let Ok(_) = self.db.insert(data_key, msg.data.as_str()) {
                    //println!("insert data success!");
                    if let Ok(_) = self
                        .nonce_idx
                        .insert(data_key_as_nonce_idx_key, IVec::from(msg.nonce.to_be_bytes().to_vec()))
                    {
                        if let Ok(_) = self.main_idx.insert(main_key, data_key_as_main_idx_val.as_bytes()) {
                            ctx.wait(actix::clock::sleep(Duration::from_millis(5)).into_actor(self));
                        }else{
                            eprintln!("insert main idx faild!");
                        }

                    } else {
                        eprintln!("insert nonce idx faild!");
                    }
                } else {
                    eprintln!("insert data faild!");
                }
            } else {
                eprintln!("update range index faild!");
            }
        } else {
            eprintln!("update today's last nonce faild!");
        };
        
    }
}