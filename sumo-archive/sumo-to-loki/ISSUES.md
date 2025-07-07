2022/10/11/07/46.json.gz: 400 Client Error: Bad Request

```
10-28T23:10:16Z',\nentry with timestamp 2022-10-10 13:25:44 +0000 UTC ignored, reason: 'entry too far behind, entry timestamp is: 2022-10-10T13:25:44Z, oldest acceptable timestamp is: 2022-10-28T23:10:16Z',\nentry with timestamp 2022-10-10 13:25:44 +0000 UTC ignored, reason: 'entry too far behind, entry timestamp is: 2022-10-10T13:25:44Z, oldest acceptable timestamp is: 2022-10-28T23:10:16Z',\nentry with timestamp 2022-10-10 13:25:44 +0000 UTC ignored, reason: 'entry too far behind, entry timestamp is: 2022-10-10T13:25:44Z, oldest acceptable timestamp is: 2022-10-28T23:10:16Z',\nentry with timestamp 2022-10-10 13:25:44 +0000 UTC ignored, reason: 'entry too far behind, entry
```

When you first pushed those Oct 10 logs into an empty stream, Loki had no “most recent” timestamp yet—so it happily accepted everything. But once that initial batch landed, the stream’s head timestamp advanced to Oct 10 (and then beyond, if you’ve sent newer logs since). On your second push, Loki compared each entry’s timestamp (Oct 10 13:25:44 UTC) to the *most recent* timestamp it already had for that **same** `{…labels}` stream (Oct 28 23:10:16 UTC) and saw it was too far behind. By default, Loki only allows out-of-order entries up to **max\_chunk\_age / 2**, which with the default `max_chunk_age=2h` means a 1 hour window—any entry older than that relative to the stream head is dropped with “entry too far behind.” ([grafana.com][1], [grafana.com][2])

---

## Fixes

### 1. Widen the out-of-order window

Increase the ingester’s `max_chunk_age` so that half of it (`max_chunk_age/2`) comfortably exceeds the delay between your head timestamp and the oldest entry you’re replaying. For a 7-day replay window, for example:

```yaml
# values.yaml
loki:
  structuredConfig:
    ingester:
      # default = 2h; here we set to 168h (7 days)
      max_chunk_age: 168h
```

```bash
helm upgrade loki grafana/loki \
  --reuse-values \
  --set loki.structuredConfig.ingester.max_chunk_age=168h
```

Now Loki will accept entries up to **84 hours** (168 h ÷ 2) behind the current stream head. ([grafana.com][2])

> **Warning:** Increasing `max_chunk_age` can bloat memory and create many under-utilized chunks. Use sparingly.

---

### 2. Send backfill to a separate stream

Instead of weakening out-of-order protection globally, add a distinguishing label to your replayed logs—e.g.:

```
{job="myjob", replay="oct-backfill", …}
```

That creates a *new* series whose head starts at Oct 10, so your Oct 10 entries will all be in-order for *that* stream. No config changes needed. ([grafana.com][3])

---

### 3. (Less common) Purge or rotate the existing series

If you really need to replay into the *same* series but can’t change labels or config, you can:

1. **Delete** the existing time range in your storage (if supported), so the head resets.
2. **Restart** Loki with `--ingester.flush-on-shutdown` to clear in-memory state (only works if you’re ok losing newer data).

—but these are disruptive and usually not recommended.

---

**In practice**, splitting streams via labels is the safest and most surgical approach for backfilling old logs, while widening `max_chunk_age` is the go-to when you have legitimate long delivery delays.

[1]: https://grafana.com/blog/2024/01/04/the-concise-guide-to-loki-how-to-work-with-out-of-order-and-older-logs/?utm_source=chatgpt.com "The concise guide to Loki: How to work with out-of-order and older ..."
[2]: https://grafana.com/docs/loki/latest/configure/?utm_source=chatgpt.com "Grafana Loki configuration parameters"
[3]: https://grafana.com/docs/loki/latest/operations/request-validation-rate-limits/?utm_source=chatgpt.com "Enforce rate limits and push request validation | Grafana Loki ..."

