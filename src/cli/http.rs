use hyper::{
    header,
    service::{make_service_fn, service_fn},
    Body, Method, Request, Response, Server, StatusCode,
};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

type Error = Box<dyn std::error::Error + Send + Sync>;

#[derive(Serialize, Deserialize)]
struct BuildInfo<'a> {
    commit_sha: &'a str,
    time: &'a str,
    compiler: &'a str,
}

pub struct Http {
    // So the thread doesn't get disposed as soon as we didn't kept the handle.
    _handle: std::thread::JoinHandle<()>,
}

impl Http {
    #[allow(dead_code)]
    pub fn wait(self) {
        self._handle.join().unwrap();
    }
}

pub fn start_http_server() -> Http {
    let _handle = std::thread::spawn(|| {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .worker_threads(1)
            .build()
            .expect("to start a tokio runtime successfully");

        runtime.block_on(run_http_server()).unwrap();
    });

    Http { _handle }
}

async fn run_http_server() -> Result<(), Error> {
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    let build_time_bytes = serde_json::to_vec(&BuildInfo {
        commit_sha: crate::build_info::git_commit_hash_full(),
        time: crate::build_info::time(),
        compiler: crate::build_info::compiler(),
    })?;

    let build_time_bytes = hyper::body::Bytes::from(build_time_bytes);

    let new_service = make_service_fn(move |_| {
        let build_time_bytes = build_time_bytes.clone();

        async move {
            let build_time_bytes = build_time_bytes.clone();
            Ok::<_, Error>(service_fn(move |req: Request<Body>| {
                let build_time_bytes = build_time_bytes.clone();
                async move {
                    let resp: Response<Body> = match (req.method(), req.uri().path()) {
                        (&Method::GET, "/build_info") => Ok::<Response<Body>, Error>(
                            Response::builder()
                                .status(StatusCode::OK)
                                .header(header::CONTENT_TYPE, "application/json")
                                .body(Body::from(build_time_bytes.clone()))?,
                        ),

                        _ => Ok(Response::builder()
                            .status(StatusCode::OK)
                            .body("not found".into())?),
                    }?;

                    Ok::<Response<Body>, Error>(resp)
                }
            }))
        }
    });
    let server = Server::bind(&addr).serve(new_service);

    server.await?;

    Ok(())
}
