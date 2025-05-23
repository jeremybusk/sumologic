# SumoLogic API Limitations

https://help.sumologic.com/docs/api/search-job/

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
