use actix::Addr;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use super::consumer::{ConsumerActor, RegisterCmd, ClearConnCmd};
use super::storage::{StorageActor, StorageCmd};
use super::websocks::Message;
use super::websocks::{WsSession, IdGenerator};
use actix::prelude::*;
use std::collections::hash_map::DefaultHasher;
use serde_json::to_string_pretty;

#[derive(Clone)]
pub struct PartitionDispacher {
    pub partitions: HashMap<u16, Partition>
}

impl PartitionDispacher {
    pub fn from_number(num: u16) -> Self {
        let mut partitions: HashMap<u16, Partition> = HashMap::new();
        for idx in 0..num {
            partitions.insert(idx, Partition::from_idx(idx));
        }
        PartitionDispacher{
            partitions
        }
    }
    fn topic_for_partition(&mut self, topic: &str) -> u16 {
        let mut state = DefaultHasher::new();
        topic.hash(&mut state);
        (state.finish() % self.partitions.len() as u64) as u16
    }
    
    pub fn dispach_message(&mut self, message: &mut Message) {
        if let Some(topic) = message.got_topic(){
            let pidx = self.topic_for_partition(topic.as_str());
            if let Some(p) = self.partitions.get_mut(&pidx){
                p.dispach_message(message);
            }
        }
    }
    
    pub fn subscribe(&mut self, sock_addr:Addr<WsSession>, client_id: &str, topics: Vec<String>, offset: u64){
        for topic in topics {
            let pidx = self.topic_for_partition(topic.as_str());
            println!("topic {topic} subscribe to {pidx}");
            if let Some(p) = self.partitions.get_mut(&pidx){
                p.subscribe(sock_addr.clone(), client_id, vec![topic], offset)
            }
        }
    }
    
    pub fn unsubscribe(&mut self, client_id: &str) {
        for (_, p) in self.partitions.iter_mut() {
            p.unsubscribe(client_id);
        }
    }
    
}


#[derive(Clone)]
pub struct Partition {
    pub idx: u16,
    pub db: sled::Db,
    pub producer_addr: Addr<StorageActor>,
    pub consumer_addr: Addr<ConsumerActor>,
    pub id_gen: IdGenerator
}

impl Partition {
    fn from_idx(idx: u16) -> Self{
        let db_file = format!("data/db_{idx}.sled");
        let db = sled::open(db_file.as_str()).unwrap();
        let r_idx = db.open_tree("range_idx").unwrap();
        let d_idx = db.open_tree("day_idx").unwrap();
        let m_idx = db.open_tree("main_idx").unwrap();
        let nonce_idx = db.open_tree("uid_to_nonce_idx").unwrap();
        let id_gen = IdGenerator::new(0);
        if let Ok(Some((k, _v))) = r_idx.last(){
            let last_id = u64::from_be_bytes(k.to_vec().try_into().unwrap());
            println!("last id:{}", last_id);
            id_gen.init_with(last_id);
        }
        
        Partition {
            idx,
            db: db.clone(),
            id_gen,
            producer_addr: StorageActor{
                db: db.clone(),
                range_idx: r_idx.clone(),
                day_idx: d_idx.clone(),
                main_idx: m_idx.clone(),
                nonce_idx: nonce_idx.clone()
            }.start(),
            consumer_addr: ConsumerActor {
                connection_offset: HashMap::new(),
                connection_addr: HashMap::new(),
                connection_count: 0,
                connection_topics: HashMap::new(),
                db: db.clone(),
                range_idx: r_idx.clone(),
                day_idx: d_idx.clone(),
                main_idx: m_idx.clone(),
                nonce_idx: nonce_idx.clone(),
            }.start()
        }
    }
    pub fn dispach_message(&mut self, message: &mut Message) {
        if let Some(topic) = message.got_topic() {
            let key = format!("{}-{}", topic, message.uid);
            let nonce = self.id_gen.gen_id();
            message.set_nonce(nonce);
            let cmd = StorageCmd{
                st_key: key,
                message_topic: topic.to_string(),
                nonce: nonce,
                data: to_string_pretty(message).unwrap()
            };
            match self.producer_addr.try_send(cmd) {
                Ok(())=>{},
                Err(err)=>{
                    eprintln!("Dispatch message with error:{}", err);
                }
            }

        }
    }
    
    pub fn subscribe(&mut self, sock_addr:Addr<WsSession>, client_id: &str, topics: Vec<String>, offset: u64){
        let cmd = RegisterCmd{
            topics: topics.clone(),
            client_id: client_id.to_string(),
            offset,
            addr: sock_addr
        };
        match self.consumer_addr.try_send(cmd) {
            Ok(())=>{
                println!("Client:{client_id} subscribe to {:?}", topics.clone());
            },
            Err(err)=>{
                eprintln!("Subscribe topic with error:{}", err);
            }
        }
        
    }
    
    pub fn unsubscribe(&mut self, client_id: &str) {
        let cmd = ClearConnCmd{
            client_id: client_id.to_string()
        };
        match self.consumer_addr.try_send(cmd) {
            Ok(())=>{
            },
            Err(err)=>{
                eprintln!("Unsubcribe with error:{}", err);
            }
        }
    }

}
