use std::{collections::BTreeMap, time::Duration};

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use process_pool::{
    ProcessFactoryConfig,
    server::{AppState, router},
};
use serde_json::{Value, json};
use tower::ServiceExt;

fn setup() -> (AppState, Router) {
    let factory = ProcessFactoryConfig {
        program: env!("CARGO_BIN_EXE_echo-worker").into(),
        args: vec![],
        env: BTreeMap::from([(
            "PRIVATE_WORKER_SETTING".into(),
            "not-exposed-to-client".into(),
        )]),
        current_dir: None,
    };
    let state = AppState::new(
        BTreeMap::from([("test-worker".into(), factory)]),
        Duration::from_secs(1),
    );
    (state.clone(), router(state))
}

fn parameters() -> Value {
    json!({
        "core_pool_size": 3,
        "maximum_pool_size": 5,
        "keep_alive_time": 1234,
        "time_unit": "milliseconds",
        "work_queue": {"type":"bounded", "capacity":7},
        "process_factory": "test-worker",
        "rejected_execution_handler": "discard"
    })
}

async fn get(app: &Router, path: &str) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), 1_000_000).await.unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}

async fn rpc(app: &Router, method: &str, params: Value) -> Value {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/rpc")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"jsonrpc":"2.0", "id":1, "method":method, "params":params}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    serde_json::from_slice(&to_bytes(response.into_body(), 1_000_000).await.unwrap()).unwrap()
}

#[tokio::test]
async fn startup_is_uninitialized_but_http_and_factory_catalog_are_available() {
    let (state, app) = setup();
    assert_eq!(
        get(&app, "/api/stats").await.1,
        json!({"initialized":false})
    );
    assert_eq!(
        get(&app, "/readyz").await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(get(&app, "/healthz").await.0, StatusCode::OK);
    assert_eq!(
        get(&app, "/api/factories").await.1,
        json!({"factories":["test-worker"]})
    );
    assert_eq!(
        rpc(&app, "pool.stats", json!({})).await["result"],
        json!({"initialized":false})
    );
    assert_eq!(
        rpc(&app, "pool.execute", json!({"payload":{}})).await["error"]["code"],
        -32006
    );
    assert_eq!(
        rpc(&app, "pool.prestart", json!({})).await["error"]["code"],
        -32006
    );
    state.shutdown().await;
}

#[tokio::test]
async fn supplied_parameters_take_effect_without_workers_until_execute_or_prewarm() {
    let (state, app) = setup();
    let result = rpc(&app, "pool.initialize", parameters()).await;
    let stats = &result["result"];
    assert_eq!(stats["initialized"], true);
    assert_eq!(stats["worker_count"], 0);
    assert_eq!(stats["core_pool_size"], 3);
    assert_eq!(stats["maximum_pool_size"], 5);
    assert_eq!(stats["keep_alive_ms"], 1234);
    assert_eq!(stats["work_queue_capacity"], 7);
    assert_eq!(stats["rejection_policy"], "discard");
    assert_eq!(get(&app, "/readyz").await.0, StatusCode::OK);

    assert!(
        rpc(&app, "pool.execute", json!({"payload":{"op":"echo"}}))
            .await
            .get("result")
            .is_some()
    );
    let stats = get(&app, "/api/stats").await.1;
    assert_eq!(stats["worker_count"], 1);
    assert_eq!(stats["completed_task_count"], 1);

    assert_eq!(
        rpc(&app, "pool.prestart", json!({})).await["result"]["started_worker_count"],
        2
    );
    assert_eq!(
        rpc(&app, "pool.prestart", json!({})).await["result"]["started_worker_count"],
        0
    );
    assert_eq!(get(&app, "/api/stats").await.1["worker_count"], 3);
    assert_eq!(
        rpc(&app, "pool.initialize", parameters()).await["error"]["code"],
        -32007
    );
    assert_eq!(get(&app, "/api/stats").await.1["completed_task_count"], 1);
    state.shutdown().await;
}

#[tokio::test]
async fn invalid_or_unregistered_configuration_does_not_initialize_the_pool() {
    let (state, app) = setup();
    let mut candidates = vec![json!({})];
    let mut invalid = parameters();
    invalid["maximum_pool_size"] = json!(0);
    candidates.push(invalid);
    let mut unknown = parameters();
    unknown["process_factory"] = json!("/bin/sh");
    candidates.push(unknown);
    let mut command = parameters();
    command["process_factory"] = json!({"program":"/bin/sh","args":["-c","false"]});
    candidates.push(command);
    let mut extra = parameters();
    extra["extra"] = json!(true);
    candidates.push(extra);
    for candidate in candidates {
        assert_eq!(
            rpc(&app, "pool.initialize", candidate).await["error"]["code"],
            -32602
        );
        assert_eq!(get(&app, "/api/stats").await.1["initialized"], false);
    }
    assert!(
        rpc(&app, "pool.initialize", parameters())
            .await
            .get("result")
            .is_some()
    );
    state.shutdown().await;
}

#[tokio::test]
async fn simultaneous_initialization_has_exactly_one_winner() {
    let (state, app) = setup();
    let (first, second) = tokio::join!(
        rpc(&app, "pool.initialize", parameters()),
        rpc(&app, "pool.initialize", parameters()),
    );
    assert_eq!(
        [&first, &second]
            .iter()
            .filter(|value| value.get("result").is_some())
            .count(),
        1
    );
    assert_eq!(
        [&first, &second]
            .iter()
            .filter(|value| value["error"]["code"] == -32007)
            .count(),
        1
    );
    assert_eq!(get(&app, "/api/stats").await.1["worker_count"], 0);
    state.shutdown().await;
}

#[tokio::test]
async fn dashboard_and_assets_are_served_with_safe_content_types() {
    let (_, app) = setup();
    for (path, content_type) in [
        ("/", "text/html"),
        ("/assets/dashboard.js", "text/javascript"),
        ("/assets/dashboard.css", "text/css"),
    ] {
        let response = app
            .clone()
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            response.headers()["content-type"]
                .to_str()
                .unwrap()
                .starts_with(content_type)
        );
        assert_eq!(response.headers()["cache-control"], "no-store");
        assert!(
            !to_bytes(response.into_body(), 1_000_000)
                .await
                .unwrap()
                .is_empty()
        );
    }
}
