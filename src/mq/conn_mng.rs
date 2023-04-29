use actix::{Actor, Addr, Context, Handler};
use std::collections::HashMap;
use super::websocks::{WsSession, InnerMessage};
use actix::prelude::*;


#[derive(Message)]
#[rtype(result = "()")]
pub struct AppendCmd {
    pub topic: String,
    pub client_id: String,
    pub addr: Addr<WsSession>
}


#[derive(Message)]
#[rtype(result = "()")]
pub struct RemoveCmd {
    pub topic: String,
    pub client_id: String,
}


#[derive(Message)]
#[rtype(result = "()")]
pub struct ClearCmd {
    pub client_id: String,
    pub topics: Vec<String>
}


#[derive(Message)]
#[rtype(result = "()")]
pub struct MsgCmd {
    pub topic: String,
    pub msg: String
}


pub struct ConnectionActor{
    pub connections: HashMap<String, HashMap<String, Addr<WsSession>>>
}

impl Actor for ConnectionActor {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        self.connections = HashMap::new();
        
    }

    fn stopping(&mut self, _ctx: &mut Self::Context) -> actix::Running {
        self.connections.clear();
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

impl Handler<ClearCmd> for ConnectionActor {
    type Result = ();
    fn handle(&mut self, msg: ClearCmd, _ctx: &mut Self::Context) {
        for topic in msg.topics {
            if self.connections.contains_key(&topic){
                if let Some(topic_conns) = self.connections.get_mut(&topic){
                    topic_conns.remove(&msg.client_id).unwrap();
                }
            }
        }
    }
}

impl Handler<AppendCmd> for ConnectionActor {
    type Result = ();
    fn handle(&mut self, msg: AppendCmd, _ctx: &mut Self::Context) {
        if self.connections.contains_key(&msg.topic){
            if let Some(topic_conns) = self.connections.get_mut(&msg.topic){
                if topic_conns.contains_key(&msg.client_id) {
                    println!("Connection Exists");
                }else{
                    topic_conns.insert(msg.client_id, msg.addr);    
                }
            }
        }else{
            let mut topic_conns = HashMap::new();
            topic_conns.insert(msg.client_id, msg.addr);
            self.connections.insert(msg.topic, topic_conns);
        }
    }
}

impl Handler<RemoveCmd> for ConnectionActor {
    type Result = ();
    fn handle(&mut self, msg: RemoveCmd, _ctx: &mut Self::Context) {
        if self.connections.contains_key(&msg.topic){
            if let Some(topic_conns) = self.connections.get_mut(&msg.topic){
                if topic_conns.contains_key(&msg.client_id) {
                    topic_conns.remove(&msg.client_id);
                }
            }
        }
    }
}


impl Handler<MsgCmd> for ConnectionActor {
    type Result = ();
    fn handle(&mut self, msg: MsgCmd, _ctx: &mut Self::Context) {
        if self.connections.contains_key(&msg.topic){
            if let Some(topic_conns) = self.connections.get_mut(&msg.topic){
                for (_key, val) in topic_conns.iter_mut() {
                    let inner_msg = InnerMessage(msg.msg.clone());
                    val.do_send(inner_msg);
                }
            }
        }
    }
}
