use anyhow::Result;
use axum::{
    Json, Router,
    extract::{Query, State},
    routing::get,
};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;

use crate::db;

struct AppState {
    pool: db::ConnPool,
}

#[derive(Deserialize)]
struct SearchParams {
    q: String,
    #[serde(default = "default_limit")]
    limit: usize,
    #[serde(default)]
    offset: usize,
}

fn default_limit() -> usize {
    10
}

#[derive(Serialize)]
struct SearchResponse {
    query: String,
    count: usize,
    results: Vec<db::AddressResult>,
}

async fn search_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<SearchParams>,
) -> Json<SearchResponse> {
    let q = params.q.clone();
    let limit = params.limit;
    let offset = params.offset;

    let results = tokio::task::spawn_blocking(move || {
        db::search_with_pool(&state.pool, &q, limit, offset).unwrap_or_default()
    })
    .await
    .unwrap_or_default();

    Json(SearchResponse {
        query: params.q,
        count: results.len(),
        results,
    })
}

async fn health_handler() -> &'static str {
    "OK"
}

pub async fn serve(db_path: &Path, port: u16) -> Result<()> {
    let pool = db::ConnPool::new(db_path, 4)?;
    let state = Arc::new(AppState { pool });

    let app = Router::new()
        .route("/addresses", get(search_handler))
        .route("/health", get(health_handler))
        .with_state(state);

    let addr = format!("0.0.0.0:{}", port);
    println!("Addressr server listening on http://{}", addr);
    println!("  GET /addresses?q=<query>&limit=10&offset=0");
    println!("  GET /health");

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
