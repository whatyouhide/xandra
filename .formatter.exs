[
  inputs: [
    "{mix,.formatter}.exs",
    "{config,lib,test,test_clustering}/**/*.{ex,exs}",
    "pages/*.exs"
  ],
  import_deps: [:stream_data],
  locals_without_parens: [assert_telemetry: 2]
]
