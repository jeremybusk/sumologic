# SumoLogic API Limitations

## Quick Summary

https://help.sumologic.com/docs/api/search-job/

'https://api.sumologic.com/api/v1/search/jobs/37589506F194FC80/records?offset=0&limit=1'

limit	Int	Yes	The number of records starting at offset to return. The maximum value for limit is 10,000 records.

Total messages per query says 100k in docs but appears to be 200k from actual API.

## Primary Limitations

There are important limits to be aware of when querying Sumo Logic, especially through the Search Job API, to ensure you retrieve all messages.

Here's a summary of the largest time window you can query and the associated limits:

**The primary limit is on the number of messages, not strictly the time window.**

* **100,000 Messages per Search Job:** The most critical limit is that a single Sumo Logic search job, regardless of the time range, can return a maximum of **100,000 messages**. This applies to all data tiers (Continuous, Frequent, Infrequent). If your query over a given time range would yield more than 100,000 messages, the job will complete, but it will only return the first 100,000 messages found.

**What does this mean for your time window?**

It means there isn't a fixed "largest time window" you can query to guarantee all messages. The "largest" safe time window depends entirely on the **data density** of your logs for that specific query.

* **Sparse Data:** If you have very few logs for a given query, you might be able to query a very long time range (e.g., days or even weeks) and still stay under the 100,000 message limit.
* **Dense Data:** If you have a high volume of logs, even a small time window (e.g., a few minutes or an hour) could quickly exceed the 100,000 message limit.

**To get all messages, you MUST break up your search into smaller time ranges.**

This is precisely why the provided script implements the "adaptive chunking" logic. When a search job for a given time window returns 100,000 messages (or hits a higher internal limit if Sumo Logic internally adjusts it, though 100K is the documented public limit), the script intelligently halves that time window and recursively queries the smaller chunks. This ensures that you eventually get all messages by staying within the per-job message limit.

**Other relevant limits from Sumo Logic documentation:**

* **API Request Rate Limit:** 4 API requests per second per user (240 requests per minute).
* **Concurrent API Requests:** 10 concurrent requests to any API endpoint per access key.
* **Concurrent Search Jobs:**
    * **200 active concurrent search jobs** for Continuous and Infrequent Tiers, or when searching "All Tiers."
    * **20 concurrent search jobs** when searching *only* the Frequent Tier.
* **`limit` Operator:** The `limit` operator in a search query can only return up to 10,000 results. This is distinct from the 100,000 message limit for the overall search job API. The script is not using the `limit` operator in the query itself, but rather relying on the API's ability to stream messages in pages after the job completes, up to the 100,000 total.

**In summary:**

The **100,000 messages per search job** is the most critical limit that dictates how large your time window can be while still guaranteeing all messages are retrieved from a single job. To overcome this, you must **subdivide your queries into smaller time ranges** until each sub-query yields less than 100,000 messages. This is the core principle behind the adaptive chunking implemented in the script.


## Other options

We've identified the key challenge: Sumo Logic's 100,000 message limit per search job. To ensure you get *all* data/messages within a time window in the shortest amount of time, while respecting this limit and other API constraints, the adaptive chunking strategy is paramount. The script you have is a great foundation for this.

Beyond that, here's a summary of additional recommendations from Sumo Logic's API docs and best practices to optimize your data extraction for speed and completeness:

### 1. Optimize Your Sumo Logic Queries

This is fundamental and will have the biggest impact on search job performance, regardless of how you break down the time windows.

* **Be Specific with Search Scope:**
    * Always use metadata tags like `_sourceCategory`, `_source`, `_sourceName`, `_sourceHost`, or `_collector` at the beginning of your query to narrow down the data scanned.
    * Include keywords early in the query.
    * **Example:** `_sourceCategory=prod/web/access AND status=500` is better than `_sourceCategory=prod/web/access | where status=500`.
* **Leverage Field Extraction Rules (FERs):** If you're consistently parsing specific fields, define FERs. Querying on pre-extracted fields is significantly faster than using `parse` operators in your query, especially with large datasets.
* **Filter Before Aggregation:** If your query includes aggregation (e.g., `count by`, `sum`), apply all filtering (keywords, metadata, `where` clauses) *before* the aggregation step to reduce the dataset size.
* **Avoid Expensive Operations:**
    * Be cautious with `parse regex` and avoid generic patterns like `.*`. Be as specific as possible. Consider `parse anchor` for structured logs.
    * `lookup` operations should be performed on the smallest possible dataset, ideally after aggregation.
* **Use Partitions and Scheduled Views:** These are index-based optimization features. If your data is routed into partitions or pre-aggregated into scheduled views, searching these will be much faster as the search is run against a smaller, optimized dataset. Consider if the data you need to extract is already in or can be directed to a partition/view.
* **Specify Data Tier:** As noted in your script, explicitly including `_dataTier=Frequent` or `_dataTier=Infrequent` at the beginning of your query can route your search to the appropriate tier and potentially influence performance or concurrency limits for that specific search type.

### 2. Optimize API Interaction and Job Management

* **Aggressive Concurrent Job Management (with caution):**
    * Sumo Logic allows **200 concurrent search jobs** (or 20 for Frequent Tier only). Your `api_request_rate_limit` (4 per second) is about general API calls, but you can technically launch more search jobs concurrently if you manage your requests within the overall API rate limit.
    * If your script is currently single-threaded for search job submission and polling, consider using a `ThreadPoolExecutor` (as your script already does) for submitting new search jobs for different time chunks in parallel, up to the concurrent job limit.
    * **However, remember the 4 API requests/second limit.** You need to balance the number of *concurrent jobs* with the rate at which you *create and poll* those jobs. The `api_request_rate_limit` parameter in your script already helps here.
* **Poll Smartly:** The `job_status_poll_initial_delay_seconds` is good. After that, continue polling at a reasonable interval (e.g., 5-10 seconds) to quickly determine job completion. Avoid excessive polling which could hit the API rate limit unnecessarily.
* **Delete Completed Jobs:** Once you've successfully retrieved all messages from a search job, explicitly delete it using the Search Job API. This frees up resources on the Sumo Logic side and helps you stay under the **200 (or 20) active concurrent search job limit**. The script doesn't explicitly show job deletion after message retrieval, which is a good addition for long-running processes.
    * `DELETE /api/v1/search/jobs/{searchJobId}`
* **Error Handling and Retries:** Your `_request_with_retry` function is excellent for handling transient network issues and 429 (Too Many Requests) errors. This is crucial for robust data extraction.
* **Session Timeout:** Remember that a search job times out after eight hours if not kept alive by polling. Your polling logic correctly handles this by continuously checking status.

### 3. Refinements to Your Script's Logic

* **Recursive Splitting is Key:** Your `process_query_chunk_recursive` function correctly handles the 100,000 message limit by recursively splitting chunks. This is *the* most important mechanism for ensuring completeness with large datasets.
* **Adaptive Sizing Improvement:**
    * Your adaptive logic is strong. One subtle refinement could be to explicitly separate the "test query" for adaptive sizing from the actual data extraction query. The test query can use `count by _timeslice` over a larger range to quickly estimate density, then use that estimate to set the `search_job_time_window_initial_minutes` for the actual data queries. However, your current method of testing with the actual query and adjusting is also valid and perhaps more accurate for real-world scenarios.
    * Ensure the `GLOBAL_OPTIMAL_CHUNK_MINUTES` is properly initialized and updated for *each unique query*. Your current `adaptive_chunk_state` dictionary keyed by `query_key_for_adaptive` does this well.
* **Progress Tracking:** For very long exports, more granular logging or a progress indicator (e.g., "X% of minutes processed for current year/month/day") could be helpful.
* **Idempotency (handled by `overwrite_if_archive_file_exists`):** Your check for existing files and the `overwrite_if_archive_file_exists` flag are crucial for idempotency, allowing you to re-run the script without re-downloading existing data unless desired.

By combining the intelligent time-window splitting with optimized queries and proper API management (like concurrent job submission and explicit job deletion), you'll achieve the most efficient and complete data extraction from Sumo Logic.

## 200000 vs 100000 limit

Is it possible to do 200000 message limit on api?

```
2025-05-24 04:10:53,800 - INFO - 🔎 Querying for job 2024Jan01H00M00: 2024-01-01T00:00:00+00:00 → 2024-01-01T00:59:59+00:00
2025-05-24 04:11:24,484 - INFO - 📬 Received 200000 messages for job 2024Jan01H00M00.
2025-05-24 04:11:24,485 - INFO - Messages (200000) exceed max_messages_per_file (100000). Splitting chunk.
2025-05-24 04:11:24,485 - INFO -   🔎 Querying for job 2024Jan01H00M00: 2024-01-01T00:00:00+00:00 → 2024-01-01T00:00:59+00:00
2025-05-24 04:11:44,629 - INFO -   📬 Received 20869 messages for job 2024Jan01H00M00.
2025-05-24 04:11:53,633 - INFO - Finished streaming for job 51C7E2E1B31F0B2C. Total messages yielded: 20869
2025-05-24 04:11:53,699 - INFO - 💾 Attempting to save 20869 messages to: sumo-archive/trun_sumologic_default/2024/Jan/01/H00/M00/trun_sumologic_default_2024Jan01H00M00.json.zst
2025-05-24 04:11:54,004 - INFO - ✅ Saved: sumo-archive/trun_sumologic_default/2024/Jan/01/H00/M00/trun_sumologic_default_2024Jan01H00M00.json.zst (20869 messages)
```

The script *could* technically handle 200,000 messages in a single query time window **if you adjusted the `max_messages_per_file` argument to be 200,000 or higher.**

Here's a breakdown:

* **`max_messages_per_file` (default 100,000):** This argument acts as a threshold. If the number of messages found for a given time chunk (e.g., an hour, if that's your `current_optimal_minutes`) exceeds this value, the script decides to *split* that time chunk into smaller sub-chunks (e.g., 1-minute chunks) and process them individually. This is primarily a strategy to manage file size and memory consumption.

**If you set `max_messages_per_file` to `200000` or more:**

The line:
`Messages (200000) exceed max_messages_per_file (100000). Splitting chunk.`
would no longer trigger for a 200,000 message chunk. The script would then proceed to:

1.  Retrieve all 200,000 messages from the Sumo Logic job.
2.  Attempt to write all 200,000 messages into one or more files, depending on the `output_granularity` and how the timestamps of those messages distribute across the files.

**Considerations when increasing `max_messages_per_file`:**

1.  **Memory Usage:** When the script fetches messages, it stores them in memory (as a Python list of dictionaries) before writing them to disk. 200,000 messages, especially if they are large JSON objects, could consume a significant amount of RAM. If you hit memory limits, the script might crash or run very slowly.
    * **Estimate:** A rough estimate for JSON data size is often 1KB-5KB per message. 200,000 messages at 2KB each would be 400MB. This is generally manageable, but if your messages are much larger, it could become an issue.
2.  **File Size:** A single Zstandard-compressed JSON file containing 200,000 messages could still be quite large (e.g., tens or hundreds of MBs). While Zstandard is very efficient, consider if you or downstream processes need to handle such large files.
3.  **Network Bandwidth/API Limits:** While `max_messages_per_file` is primarily a client-side limit for processing, fetching 200,000 messages from Sumo Logic for a single job might take longer and consume more network bandwidth. The `messages_per_api_request` argument (`DEFAULT_MESSAGES_PER_API_REQUEST = 10000`) helps with this by paginating the fetch, even if `max_messages_per_file` is higher.
4.  **Error Recovery/Retry:** If a single large request fails due to a transient network issue or API timeout, you lose the progress on that entire large chunk. Splitting it means smaller failures are isolated and can be retried more granularly.

**Recommendation:**

The current adaptive splitting logic is a robust design choice for several reasons (memory management, file size, network stability, and adapting to variable data volumes). Unless you have a strong reason to keep messages from a wide time range in a single file despite high volume, sticking with the current adaptive splitting is generally better.

However, if you're confident your system has enough memory and you prefer larger individual files for a specific reason, you can absolutely adjust `max_messages_per_file` using the command-line argument:

```bash
python your_script_name.py --sumo-query "your query" --years 2024 --max-messages-per-file 200000
```

## Reasons not to do 200000 and stick to API limit of 100000

Here's a breakdown of why you might see this apparent contradiction:

1.  **"Limit" Parameter vs. Total Messages per Search Job:**
    * **The `limit` parameter in the "Get Messages" endpoint:** The Sumo Logic API documentation for the "Get Messages" endpoint within a search job specifies a `limit` parameter. This `limit` typically refers to the **maximum number of messages you can request in *a single API call (page)*** when fetching results from an *already running* search job. The documentation states this is `10,000 messages or 100 MB in total message size`. This is a per-request limit for paging through results.
    * **Total messages for a Non-Aggregate Search Job:** The `100,000` message limit often cited in documentation (e.g., for "Continuous" or "Frequent" data tiers) refers to the **total number of raw messages a *single non-aggregate search job* can return *before it might be paused or stopped by Sumo Logic***. The documentation explicitly states: "Flex Licensing model can return up to 100K messages per search." and "If you need more results, you'll need to break up your search into several searches that span smaller blocks of the time range needed."

2.  **The "Force Paused" State (and what it means):**
    The documentation mentions a `FORCE_PAUSED` state for a search job: "Query that is paused by the system. It is true only for non-aggregate queries that are paused at the limit of 100k. This limit is dynamic and may vary from customer to customer."

    This is the key. When your script's initial hourly job `2024Jan01H00M00` returned 200,000 messages, it likely means:
    * Sumo Logic *did* process the query for that full hour.
    * It *did* count and identify 200,000 messages matching your query.
    * However, because this exceeded its internal threshold (which it usually states as 100,000 for non-aggregate searches), the job likely entered a `FORCE_PAUSED` state or completed but indicated that more results were available than the standard limit.

3.  **How your script handles this (and why it's good):**
    Your script's logic for `max_messages_per_file` (which you have set to 100,000 by default) is a **client-side limit** designed to align with, or even anticipate, Sumo Logic's server-side limits and best practices for large data exports.

    * When your script sees `Received 200000 messages for job...`, it's getting the *total count* for that job from Sumo Logic's status.
    * It then compares this `200000` to its *own* `max_messages_per_file` (100,000).
    * Since `200000 > 100000`, it triggers the "Splitting chunk" logic.

    This client-side splitting prevents the job from hitting the server-side `FORCE_PAUSED` state (or other less desirable behaviors) where you might not be able to retrieve *all* 200,000 messages from that *single* job due to the server's internal limits. By proactively breaking it down into smaller time windows (e.g., minutes), your script ensures that each sub-job falls *within* the expected limits for a single search job, allowing for complete and successful extraction.

**Conclusion:**

The documentation stating a 100,000 message limit likely refers to the "safe" or "guaranteed" maximum for a single non-aggregate search job to avoid hitting the `FORCE_PAUSED` state or truncation. The fact that you *saw* 200,000 messages counted by Sumo Logic for the job indicates that the query *execution* phase identified that many results. However, to reliably *extract* all those messages, breaking the query into smaller time windows is the recommended and often necessary approach, which your script correctly implements.

Think of it like this: a library might say "You can borrow up to 5 books at a time." But if you go to check out 8 books, they might tell you "You have 8 books, but you can only take 5 now. Please return the others or make another trip." Your script is essentially making multiple "trips" (smaller time window queries) to stay within the "5 books at a time" rule.
