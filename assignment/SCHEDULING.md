# Part C: Schedule both DAGs for 9:00 AM IST

Both `nammamart_etl_sudatta` and `nammamart_elt_sudatta` use:

```python
default_args = {
    "owner": "sudatta",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "execution_timeout": timedelta(minutes=5),
}

schedule="0 9 * * 1-6",
start_date=pendulum.datetime(2026, 7, 1, tz="Asia/Kolkata"),
catchup=False,
```

1. **Cron expression:** `0 9 * * 1-6`. Minute 0, hour 9, every day of the month, every month, Monday through Saturday. Sunday has no scheduled run. The timezone-aware start date establishes the DAG timezone as Asia/Kolkata.

2. **UTC time in the Airflow UI:** 9:00 AM IST is **03:30 UTC** on the same day. If the UI timezone selector is set to UTC, the scheduled trigger time displays as 03:30. Actual task start times can be later because of scheduler or worker delays. A data-interval timetable can label a run with the preceding interval's logical date; that label is not its actual start time. [Airflow timezones](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/timezone.html)

3. **If start_date had stayed in UTC:** The same cron expression would mean 09:00 UTC, which is **2:30 PM IST**. Neither DAG would start at 9:00 AM IST; it would miss the 9:30 AM leadership stand-up. Asia/Kolkata is UTC+05:30 throughout the year.

4. **If the laptop or Docker is off at 9 AM:** No tasks can execute, so the on-time SLA is missed. In this local experiment, after Docker restarted and the DAGs were unpaused, Airflow created the latest missed 09:00 IST run; it did not replay every interval since July. Existing queued work can also resume. `catchup=False` suppresses full historical catchup, but does not guarantee that the most recent missed run will be skipped: the timetable, scheduler state and configuration determine recovery. Use a controlled manual trigger if needed, with the correct source snapshot. [Airflow timetable and catchup behavior](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/timetable.html)

**Why catchup=False:** These pipelines read current CSV snapshots, rather than fetching data separately for each historical logical date. Backfilling every interval since July 1 would repeatedly process the same files, append unnecessary ELT raw snapshots and run duplicate reports. Historical recovery should be a deliberate manual run or backfill with appropriate historical inputs.

**Retries and timeout:** Every task inherits two retries, a 30-second retry delay, and a five-minute execution timeout per attempt. This handles temporary failures while bounding individual attempts. It is not a 30-minute timeout for the whole DAG and does not guarantee successful completion before 9:30 AM; that requires the scheduler/workers to stay running and the full pipeline to finish within the available time. [Airflow task timeouts](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html)

Enable both DAGs in Airflow and keep Astro/Docker running during the scheduling window. The code changes define the schedule; local Python syntax checks do not activate the scheduler.

**Requirement ambiguity:** the brief asks for data before 09:00 but explicitly requires a 09:00 start. These cannot both be guaranteed. This implementation follows Part C: 09:00 IST start and the 09:30 review target; an earlier start requires agreement with the client.
