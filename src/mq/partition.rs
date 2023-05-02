use actix::Addr;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use serde::Serialize;
use super::consumer::{ConsumerActor, RegisterCmd, ClearConnCmd};
use super::storage::{StorageActor, StorageCmd, TrimCmd};
use super::websocks::{WsSession, IdGenerator, Message};
use actix::prelude::*;
use std::collections::hash_map::DefaultHasher;
use serde_json::to_string_pretty;


#[derive(Serialize)]
pub struct Status {
    pub retain_messages: usize,
    pub disk_size: u64,
    pub last_nonce: u64
}

#[derive(Clone)]
pub struct PartitionDispacher {
    pub partitions: HashMap<u16, Partition>,
    pub id_generator: IdGenerator
}

impl PartitionDispacher {
    pub fn from_number(num: u16) -> Self {
        let id_generator = IdGenerator::new(0);
        let mut partitions: HashMap<u16, Partition> = HashMap::new();
        let mut nonce_vec: Vec<u64> = vec![];
        for idx in 0..num {
            let mut partition = Partition::from_idx(idx, id_generator.clone());
            nonce_vec.insert(idx as usize, partition.last_nonce());
            partitions.insert(idx, partition);
        }
        let max_nonce = nonce_vec.iter().max().unwrap();
        id_generator.init_with(*max_nonce);
        PartitionDispacher{
            partitions,
            id_generator
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
    
    pub fn trim_data(&mut self, days: u16) {
        for (_, p) in self.partitions.iter_mut() {
            p.trim_data(days);
        }
    }

    pub fn sum_status(&mut self) -> Status {
        let mut disk_size = 0;
        let mut retain_messages = 0;
        let last_nonce = self.id_generator.get_max_id();
        for (_, p) in self.partitions.iter_mut() {
            let st = p.sum_status();
            disk_size+=st.disk_size;
            retain_messages+=st.retain_messages;
        }
        Status{
            disk_size,
            retain_messages,
            last_nonce
        }
    }

}


#[derive(Clone)]
pub struct Partition {
    pub idx: u16,
    pub db: sled::Db,
    pub r_idx: sled::Tree,
    pub d_idx: sled::Tree,
    pub m_idx: sled::Tree,
    pub nonce_idx: sled::Tree,
    pub producer_addr: Addr<StorageActor>,
    pub consumer_addr: Addr<ConsumerActor>,
    pub id_gen: IdGenerator
}

impl Partition {
    fn from_idx(idx: u16, id_generator: IdGenerator) -> Self{
        let db_file = format!("data/db_{idx}.sled");
        let db = sled::open(db_file.as_str()).unwrap();
        let r_idx = db.open_tree("range_idx").unwrap();
        let d_idx = db.open_tree("day_idx").unwrap();
        let m_idx = db.open_tree("main_idx").unwrap();
        let nonce_idx = db.open_tree("uid_to_nonce_idx").unwrap();
        
        Partition {
            idx,
            db: db.clone(),
            r_idx: r_idx.clone(),
            d_idx: d_idx.clone(),
            m_idx: m_idx.clone(),
            nonce_idx: nonce_idx.clone(),
            id_gen: id_generator,
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

    pub fn last_nonce(&mut self) -> u64 {
        if let Ok(Some((k, _v))) = self.r_idx.last() {
            let last_id = u64::from_be_bytes(k.to_vec().try_into().unwrap());
            println!("last id:{}", last_id);
            last_id
        }else{
            0
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

    pub fn trim_data(&mut self, days: u16) {
        let cmd = TrimCmd{days: days};
        match self.producer_addr.try_send(cmd) {
            Ok(())=>{
                println!("trim data in segment {} success", self.idx);
            },
            Err(err)=>{
                eprintln!("trim data {days} days with error:{err}");
            }
        }
    }

    pub fn sum_status(&mut self) -> Status {
        let disk_size = match self.db.size_on_disk(){
            Ok(sz)=>sz,
            Err(_)=>0
        };

        let retain_messages = self.r_idx.len();
        Status{
            retain_messages,
            disk_size,
            last_nonce:0
        }
    }

}
