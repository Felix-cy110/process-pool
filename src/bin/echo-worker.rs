//! Example worker implementing the pool's stdin/stdout NDJSON protocol.

use std::time::Duration;

use process_pool::{WorkerError, WorkerRequest, WorkerResponse};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let stdin = tokio::io::stdin();
    let mut lines = BufReader::new(stdin).lines();
    let mut stdout = tokio::io::stdout();

    while let Some(line) = lines.next_line().await? {
        let request: WorkerRequest = serde_json::from_str(&line)?;
        let response = handle(request).await;
        let mut encoded = serde_json::to_vec(&response)?;
        encoded.push(b'\n');
        stdout.write_all(&encoded).await?;
        stdout.flush().await?;
    }
    Ok(())
}

async fn handle(request: WorkerRequest) -> WorkerResponse {
    let operation = request
        .payload
        .get("op")
        .and_then(Value::as_str)
        .unwrap_or("echo");

    match operation {
        "echo" => WorkerResponse::success(
            request.id,
            json!({
                "pid": std::process::id(),
                "value": request.payload.get("value").cloned().unwrap_or(Value::Null)
            }),
        ),
        "sum" => {
            let values = request
                .payload
                .get("values")
                .and_then(Value::as_array)
                .cloned();
            match values {
                Some(values) => {
                    let sum = values.iter().filter_map(Value::as_f64).sum::<f64>();
                    WorkerResponse::success(
                        request.id,
                        json!({ "pid": std::process::id(), "sum": sum }),
                    )
                }
                None => invalid_input(request.id, "values must be a JSON number array"),
            }
        }
        "sleep" => {
            let millis = request
                .payload
                .get("millis")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            tokio::time::sleep(Duration::from_millis(millis)).await;
            WorkerResponse::success(
                request.id,
                json!({
                    "pid": std::process::id(),
                    "value": request.payload.get("value").cloned().unwrap_or(Value::Null)
                }),
            )
        }
        "fail" => WorkerResponse::failure(
            request.id,
            WorkerError {
                code: "EXAMPLE_FAILURE".into(),
                message: "failure requested by caller".into(),
                details: None,
            },
        ),
        "crash" => std::process::exit(17),
        _ => invalid_input(request.id, "unknown operation"),
    }
}

fn invalid_input(id: u64, message: &str) -> WorkerResponse {
    WorkerResponse::failure(
        id,
        WorkerError {
            code: "INVALID_INPUT".into(),
            message: message.into(),
            details: None,
        },
    )
}
