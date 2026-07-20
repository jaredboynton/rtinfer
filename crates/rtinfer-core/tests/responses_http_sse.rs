use rtinfer_core::assemble_codex_responses_sse;

#[test]
fn assembles_sse_across_arbitrary_chunk_boundaries() {
    let chunks: Vec<&[u8]> = vec![
        b"event: response.output_text.delta\r\ndata: {\"type\":\"response.output_",
        b"text.delta\",\"delta\":\"hel\"}\r\n\r\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"lo\"}\n\n",
        b"data: {\"type\":\"response.output_text.done\",\"text\":\"hello\"}\n\ndata: {\"type\":\"response.completed\"}\n\n",
    ];

    assert_eq!(assemble_codex_responses_sse(&chunks).unwrap(), "hello");
}

#[test]
fn surfaces_provider_failures_from_sse() {
    let chunks: Vec<&[u8]> = vec![
        b"data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"rate_limit_exceeded\",\"message\":\"busy\"}}}\n\n",
    ];

    let error = assemble_codex_responses_sse(&chunks).unwrap_err();
    assert_eq!(error.code_or_label(), "provider:rate_limit_exceeded");
}

#[test]
fn incomplete_sse_is_rejected() {
    let chunks: Vec<&[u8]> = vec![
        b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"hel\"}\n\n",
        b"data: {\"type\":\"response.output_text.done\",\"text\":\"hello\"}\n\n",
    ];
    let err = assemble_codex_responses_sse(&chunks).unwrap_err();
    assert_eq!(err.code_or_label(), "protocol");
}

#[test]
fn malformed_sse_data_is_rejected() {
    let chunks: Vec<&[u8]> = vec![b"data: {not-json\n\n"];
    let err = assemble_codex_responses_sse(&chunks).unwrap_err();
    assert_eq!(err.code_or_label(), "protocol");
}

#[test]
fn done_marker_is_not_semantic_completion() {
    let chunks: Vec<&[u8]> = vec![
        b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"x\"}\n\n",
        b"data: [DONE]\n\n",
    ];
    let err = assemble_codex_responses_sse(&chunks).unwrap_err();
    assert_eq!(err.code_or_label(), "protocol");
}

#[test]
fn incomplete_sse_after_partial_delta_is_protocol() {
    // Assembler-only: incomplete stream after deltas is protocol, never success.
    // Exact HTTP POST send-count / no-replay is owned by responses_dual_transport
    // (or live proof when cleartext H2 is unavailable).
    let chunks: Vec<&[u8]> =
        vec![b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n"];
    let err = assemble_codex_responses_sse(&chunks).unwrap_err();
    assert_eq!(err.code_or_label(), "protocol");
    assert!(!format!("{err}").contains("partial"));
}

#[test]
fn sse_decoder_is_chunk_boundary_invariant() {
    let full = b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"ab\"}\r\n\r\ndata: {\"type\":\"response.output_text.done\",\"text\":\"ab\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n";
    let expected = assemble_codex_responses_sse(&[full.as_slice()]).unwrap();

    for split_at in 1..full.len() {
        let (a, b) = full.split_at(split_at);
        let got = assemble_codex_responses_sse(&[a, b]).unwrap();
        assert_eq!(got, expected, "split_at={split_at}");
    }
}
