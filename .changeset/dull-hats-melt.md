---
'@workflow/core': patch
'@workflow/world': patch
---

Ignore events written for a correlation id that already reached its terminal state instead of failing the run with a corrupted event log
