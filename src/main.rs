use std::env;
use actix_web::{web, App, Error, HttpRequest, HttpResponse, HttpServer, Responder};
use actix_web_actors::ws;
use serde::Serialize;
mod websocks;

#[macro_use] 
extern crate lazy_static;

lazy_static! {
    static ref ID_GENERATOR:websocks::IdGenerator = websocks::IdGenerator::new(0);
}
struct AppState {
    db: sled::Db,
    range_idx: sled::Tree,
    day_idx: sled::Tree,
    main_idx: sled::Tree,
    nonce_idx: sled::Tree
}

async fn index(req: HttpRequest, stream: web::Payload, data: web::Data<AppState>) -> Result<HttpResponse, Error> {
    let client_id: &str = req.match_info().get("cid").unwrap();
    let db = data.db.clone();
    let resp = ws::start(websocks::WsSession {
        topics:Some(vec![]),
        db,
        client_id: client_id.to_string(),
        offset: 0,
        range_idx: data.range_idx.clone(),
        day_idx: data.day_idx.clone(),
        main_idx: data.main_idx.clone(),
        nonce_idx: data.nonce_idx.clone(),
        id_generator: ID_GENERATOR.clone()
    }, &req, stream);
    println!("{:?}", resp);
    resp
}

#[derive(Serialize)] 
struct Status {
    retain_messages: usize,
    disk_size: u64,
    last_nonce: u64
}

async fn status_handler(_req: HttpRequest, data: web::Data<AppState>) -> impl Responder{
    let disk_size = match data.db.size_on_disk(){
        Ok(sz)=>sz,
        Err(_)=>0
    };
    
    let retain_messages = data.range_idx.len();

    let last_nonce = match data.range_idx.last() {
        Ok(Some((k, _v)))=>u64::from_be_bytes(k.to_vec().try_into().unwrap()),
        Err(_e)=>0,
        Ok(None)=>0
    };
    
    let status = Status{
        retain_messages,
        disk_size,
        last_nonce
    };
    
    let json = serde_json::to_string(&status).unwrap();
    HttpResponse::Ok().body(json)
}

async fn trim_handler(req: HttpRequest, data: web::Data<AppState>) -> impl Responder{
    let offset: &str = req.match_info().get("offset").unwrap();
    let off_i32: i32 = offset.parse::<i32>().unwrap();
    let target_timestamp = websocks::today_ts() - (3600 * 24 * (off_i32 + 1)) as i64;
    println!("tar ts: {target_timestamp:?}");
    if let Ok(Some(v)) = data.day_idx.get(websocks::i64to_vec(target_timestamp)){
        println!("Got nonce:{v:?}");
        for item in data.range_idx.range(..v){
            if let Ok((rkey, data_key)) = item {
                let data_key_for_split = data_key.clone();
                println!("Matched key:「{data_key:?}」");
                if let Ok(_) = data.db.remove(data_key){
                    println!("removed from storage");
                }
                let str_key = String::from_utf8(data_key_for_split.to_vec().try_into().unwrap()).unwrap();
                let topic = str_key.split('-').next().unwrap();
                let nonce = websocks::vectu64(rkey.to_vec());
                let main_key = websocks::make_key(topic, nonce);
                let main_key2 = main_key.clone();
                if let Ok(_) = data.main_idx.remove(main_key){
                    println!("removed from index");
                }
                if let Ok(_) = data.range_idx.remove(rkey){
                    println!("removed from index");
                }
                if let Ok(_) = data.nonce_idx.remove(main_key2){
                    println!("removed from index");
                }
            }
        }
    }
    let resp: websocks::ErrResp = websocks::ErrResp{rs:true, detail:"".to_string()};
    let json = serde_json::to_string(&resp).unwrap();
    HttpResponse::Ok().body(json)
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let args: Vec<String> = env::args().collect();
    println!("args:{args:?}");
    let port = if args.len() > 1usize {
        args[1].parse::<u16>().unwrap()
    }else{
        8080
    };
    let db = sled::open("db.sled").unwrap();
    let r_idx = db.open_tree("range").unwrap();
    let d_idx = db.open_tree("d_idx").unwrap();
    let m_idx = db.open_tree("main_idx").unwrap();
    let nonce_idx = db.open_tree("uid_to_nonce_idx").unwrap();

    if let Ok(Some((k, _v))) = r_idx.last(){
        let last_id = u64::from_be_bytes(k.to_vec().try_into().unwrap());
        println!("last id:{}", last_id);
        ID_GENERATOR.init_with(last_id);
    }
    let app_state = web::Data::new(AppState { db, range_idx: r_idx, day_idx: d_idx, main_idx: m_idx, nonce_idx});
    HttpServer::new(move || App::new()
                            .route("/ws/{cid}", web::get().to(index))
                            .route("/api/status", web::get().to(status_handler))
                            .route("/api/trim/{offset}/days", web::get().to(trim_handler))
                            .app_data(app_state.clone()))
        .bind(("0.0.0.0", port))?
        .run()
        .await
}
