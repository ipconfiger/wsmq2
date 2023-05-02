use std::env;
use actix_web::{web, get, App, Error, HttpRequest, HttpResponse, HttpServer, Responder};
use actix_web_actors::ws;
mod mq;
use mq::websocks;
use mq::partition::PartitionDispacher;

struct AppState {
    dispacher: PartitionDispacher
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

async fn trim_handler(req: HttpRequest, data: web::Data<AppState>) -> impl Responder{
    let offset: &str = req.match_info().get("offset").unwrap();
    let days: u16 = offset.parse::<u16>().unwrap();
    data.dispacher.clone().trim_data(days);
    let resp: websocks::ErrResp = websocks::ErrResp{rs:true, detail:"".to_string()};
    let json = serde_json::to_string(&resp).unwrap();
    HttpResponse::Ok().body(json)
}

async fn status_handler(_req: HttpRequest, data: web::Data<AppState>) -> impl Responder{
    let status = data.dispacher.clone().sum_status();
    let json = serde_json::to_string(&status).unwrap();
    HttpResponse::Ok().body(json)
}

async fn publish_handler(_req: HttpRequest, data: web::Data<AppState>, msg: web::Json<websocks::Message>) -> impl Responder {
    let mut message = msg;
    data.dispacher.clone().dispach_message(&mut message);
    let resp: websocks::ErrResp = websocks::ErrResp{rs:true, detail:"".to_string()};
    let json = serde_json::to_string(&resp).unwrap();
    HttpResponse::Ok().body(json)
}


#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let args: Vec<String> = env::args().collect();
    println!("args:{args:?}");

    let matches = clap::App::new("Websocket Messsage Queue")
    .version("0.1")
    .author("Alexander.Li")
    .about("Light and Fast Message Queue Run Under Websocket")
    .arg(clap::Arg::with_name("port")
        .short('p')
        .long("port")
        .value_name("Listen Port")
        .help("The port mq listen on")
        .default_value("8080")
        .takes_value(true))
    .arg(clap::Arg::with_name("Segment")
        .short('s')
        .long("segment")
        .value_name("segment")
        .help("Segment count for storage")
        .default_value("10")
        .takes_value(true))
    .get_matches();
    let port_str = matches.value_of("port").unwrap();
    let segment_str = matches.value_of("Segment").unwrap();


    let port:u16 = port_str.parse().unwrap();
    let segment:u16 = segment_str.parse().unwrap();

    let dispatcher = PartitionDispacher::from_number(segment);

    let app_state = web::Data::new(AppState {
        dispacher:dispatcher
    });
    HttpServer::new(move || App::new()
                            .service(websocket_service)
                            .route("/api/publish", web::post().to(publish_handler))
                            .route("/api/status", web::get().to(status_handler))
                            .route("/api/trim/{offset}/days", web::get().to(trim_handler))
                            .app_data(app_state.clone()))
        .bind(("0.0.0.0", port))?
        .run()
        .await
}
