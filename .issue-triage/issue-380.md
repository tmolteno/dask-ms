=== ISSUE #380 | pyarrow test cases segfault flakily | labels: bug | comments: [] | created: 2026-07-20T11:07:15Z ===
The github test suite started failing recently with the release of pyarrow 25.0.0. Likely this is due to:

- https://github.com/apache/arrow/issues/50471#issuecomment-5020675468
