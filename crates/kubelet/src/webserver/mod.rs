use crate::config::ServerConfig;
use crate::exec::{Command, CommandOptions};
use crate::log::{Options as LogOptions, Sender};
use crate::provider::{NotImplementedError, Provider};
use http::status::StatusCode;
use http::Response;
use hyper::Body;
/// Server is an HTTP(S) server for answering Kubelet callbacks.
///
/// Logs and exec calls are the main things that a server should handle.
use log::{debug, error};
use std::convert::Infallible;
use std::sync::Arc;
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

    let exec_provider = provider.clone();
    let exec = warp::post()
        .and(warp::path!("exec" / String / String / String))
        // The default query filter doesn't allow duplicate command query, which instead happens with
        // exec, e.g. `?command=add&command=1&command=2`
        .and(warp::query::raw())
        .and_then(move |namespace, pod, container, opts| {
            let provider = exec_provider.clone();
            post_exec(provider, namespace, pod, container, opts)
        });

    let routes = ping.or(health).or(logs).or(exec);

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

/// Run a pod exec command and get the output
///
/// Implements the kubelet path /exec/{namespace}/{pod}/{container}
async fn post_exec<T: Provider>(
    provider: Arc<T>,
    namespace: String,
    pod: String,
    container: String,
    query: String,
) -> Result<Response<Body>, Infallible> {
    debug!(
        "Got container exec request for container {} in pod {} in namespace {}. Query: {:?}",
        namespace, pod, container, query
    );

    let opts = parse_exec_query(&query);

    match opts {
        Ok(opts) => match provider.exec(namespace, pod, container, opts).await {
            Ok(result) => {
                let body = Body::from(result);

                Ok(Response::new(body))
            }
            Err(e) => return_with_code(StatusCode::INTERNAL_SERVER_ERROR, format!("{}", e)),
        },
        Err(e) => return_with_code(StatusCode::INTERNAL_SERVER_ERROR, format!("{}", e)),
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
