//! Runs the guides' self-contained snippets.

#[tokio::test]
async fn the_mock_transport_snippet_runs() {
    doc_client::counts_with_a_mock().await;
}

#[tokio::test]
async fn the_fake_queue_snippet_runs() {
    doc_client::worker_against_a_fake_queue().await;
}
