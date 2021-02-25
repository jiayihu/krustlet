use crate::config::ServerConfig;
use crate::exec::{Command, CommandOptions};
use crate::log::{Options as LogOptions, Sender};
use crate::provider::{NotImplementedError, Provider};
use futures::{FutureExt, StreamExt};
use http::status::StatusCode;
use http::Response;
use hyper::Body;
/// Server is an HTTP(S) server for answering Kubelet callbacks.
///
/// Logs and exec calls are the main things that a server should handle.
use log::{debug, error};
use std::convert::Infallible;
use std::sync::Arc;
use warp::ws::{Message, WebSocket};
use warp::Filter;

const PING: &str = "this is the Krustlet HTTP server";

/// Start the Krustlet HTTP(S) server
///
/// This is a primitive implementation of an HTTP provider for the internal API.
pub(crate) async fn start<T: Provider>(
    provider: Arc<T>,
    config: &ServerConfig,
) -> anyhow::Result<()> {
    let health = warp::get().and(warp::path("healthz")).map(|| PING);
    let ping = warp::get().and(warp::path::end()).map(|| PING);

    let logs_provider = provider.clone();
    let logs = warp::get()
        .and(warp::path!("containerLogs" / String / String / String))
        .and(warp::query::<LogOptions>())
        .and_then(move |namespace, pod, container, opts| {
            let provider = logs_provider.clone();
            get_container_logs(provider, namespace, pod, container, opts)
        });

    let ws_exec_provider = provider.clone();
    let ws_exec = warp::path!("exec" / String / String / String)
        // The default query filter doesn't allow duplicate command query, which instead happens with
        // exec, e.g. `?command=add&command=1&command=2`
        .and(warp::query::raw())
        .and(warp::ws())
        .map(move |namespace, pod, container, opts, ws: warp::ws::Ws| {
            let provider = ws_exec_provider.clone();

            ws.on_upgrade(move |websocket| {
                handle_exec(provider, namespace, pod, container, opts, websocket)
            })
        });

    let routes = ping.or(health).or(logs).or(ws_exec).with(warp::log("api"));

    warp::serve(routes)
        .tls()
        .cert_path(&config.cert_file)
        .key_path(&config.private_key_file)
        .run((config.addr, config.port))
        .await;
    Ok(())
}

/// Get the logs from the running container.
///
/// Implements the kubelet path /containerLogs/{namespace}/{pod}/{container}
async fn get_container_logs<T: Provider>(
    provider: Arc<T>,
    namespace: String,
    pod: String,
    container: String,
    opts: LogOptions,
) -> Result<Response<Body>, Infallible> {
    debug!(
        "Got container log request for container {} in pod {} in namespace {}. Options: {:?}.",
        container, pod, namespace, opts
    );
    let (sender, log_body) = Body::channel();
    let log_sender = Sender::new(sender, opts);

    match provider.logs(namespace, pod, container, log_sender).await {
        Ok(()) => Ok(Response::new(log_body)),
        Err(e) => {
            error!("Error fetching logs: {}", e);
            if e.is::<NotImplementedError>() {
                return_with_code(
                    StatusCode::NOT_IMPLEMENTED,
                    "Logs not implemented in provider.".to_owned(),
                )
            } else {
                return_with_code(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Server error: {}", e),
                )
            }
        }
    }
}

// Byte indicating the message channel
// https://github.com/clux/kube-rs/blob/33207f9589/kube/src/api/remote_command.rs#L160
// const STDIN_CHANNEL: u8 = 0;
const STDOUT_CHANNEL: u8 = 1;
const STDERR_CHANNEL: u8 = 2;
// const STATUS_CHANNEL: u8 = 3;
// const RESIZE_CHANNEL: u8 = 4;

async fn handle_exec<T: Provider>(
    provider: Arc<T>,
    namespace: String,
    pod: String,
    container: String,
    query: String,
    socket: WebSocket,
) {
    debug!(
        "Got container exec request for container {} in pod {} in namespace {}. Query: {:?}",
        namespace, pod, container, query
    );

    let opts = parse_exec_query(&query);

    if let Ok(opts) = opts {
        let (ws_tx, _) = socket.split();

        let result = futures::stream::once(async {
            let result = provider.exec(namespace, pod, container, opts).await;

            match result {
                Ok(s) => {
                    let mut payload = vec![STDOUT_CHANNEL];
                    payload.append(&mut s.into_bytes());

                    Ok(Message::binary(payload))
                }
                Err(e) => {
                    let mut payload = vec![STDERR_CHANNEL];
                    let mut error_msg = format!("{}", e).into_bytes();
                    payload.append(&mut error_msg);

                    Ok(Message::binary(payload))
                }
            }
        });

        result
            .forward(ws_tx)
            .map(|result| {
                if let Err(e) = result {
                    log::info!("Websocket error: {:?}", e);
                }
            })
            .await
    }
}

fn return_with_code(code: StatusCode, body: String) -> Result<Response<Body>, Infallible> {
    let mut response = Response::new(body.into());
    *response.status_mut() = code;
    Ok(response)
}

fn parse_exec_query(query: &String) -> anyhow::Result<CommandOptions> {
    let mut function: Option<String> = None;
    let mut args: Vec<String> = Vec::new();
    let pairs = query.split("&");

    for pair in pairs {
        let keyvalue: Vec<&str> = pair.split("=").collect();
        let key = keyvalue
            .get(0)
            .ok_or_else(|| anyhow::anyhow!("Cannot get the query key"))?
            .to_string();
        let value = keyvalue
            .get(1)
            .ok_or_else(|| anyhow::anyhow!("Cannot get the query value"))?
            .to_string();

        if key.as_str() == "command" && !function.is_some() {
            function = Some(value);
        } else if key.as_str() == "command" && function.is_some() {
            args.push(value);
        }

        // TODO: any other query param like stderr, stdin, tty is not supported yet
    }

    function
        .map(|f| {
            let command = Command { function: f, args };

            CommandOptions {
                command,
                stdin: false,
                stdout: false,
                stderr: false,
                tty: false,
            }
        })
        .ok_or_else(|| anyhow::anyhow!("Error while parsing the exec query string"))
}
