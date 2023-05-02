use std::env;
use actix_web::{web, get, App, Error, HttpRequest, HttpResponse, HttpServer, Responder};
use actix_web_actors::ws;
use serde::Serialize;
mod mq;
use mq::websocks;
use mq::partition::PartitionDispacher;

struct AppState {
    dispacher: PartitionDispacher
}


#[derive(Serialize)] 
struct Status {
    retain_messages: usize,
    disk_size: u64,
    last_nonce: u64
}


const MAX_FRAME_SIZE: usize = 128_384; // 16KiB

#[get("/ws/{cid}")]
async fn websocket_service(req: HttpRequest, stream: web::Payload, data: web::Data<AppState>) -> Result<HttpResponse, Error> {
    // Create a Websocket session with a specific max frame size, codec, and protocols.
    let client_id: &str = req.match_info().get("cid").unwrap();
    let actor = websocks::WsSession {
        client_id: client_id.to_string(),
        dispacher: data.dispacher.clone()
    };
    ws::WsResponseBuilder::new(actor, &req, stream)
        .codec(actix_http::ws::Codec::new())
        .frame_size(MAX_FRAME_SIZE)
        .protocols(&["A", "B"])
        .start()
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

    let dispatcher = PartitionDispacher::from_number(10);

    let app_state = web::Data::new(AppState {
        dispacher:dispatcher
    });
    HttpServer::new(move || App::new()
                            .service(websocket_service)
                            //.route("/api/status", web::get().to(status_handler))
                            //.route("/api/trim/{offset}/days", web::get().to(trim_handler))
                            .app_data(app_state.clone()))
        .bind(("0.0.0.0", port))?
        .run()
        .await
}
