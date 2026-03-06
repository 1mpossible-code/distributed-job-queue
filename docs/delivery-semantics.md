# exactly-once vs at-least-once delivery

this queue implements **at-least-once** delivery.

- workers can crash after reserve but before ack.
- at-least-once avoids silent data loss by re-delivering expired leases.
- duplicates are possible, so handlers should be idempotent.

exactly-once is harder in real distributed systems because side effects usually happen outside the broker transaction boundary.
this repo instead uses practical controls: enqueue idempotency keys, retries with backoff, and lease-based recovery.
