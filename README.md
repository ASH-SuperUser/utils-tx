# utils_tx

A lightweight, threading-safe utility toolkit for Python trading and automation systems. Built with reliability and 24/7 operation in mind.

## Modules

| Module | Description |
|--------|-------------|
| `logger` | TCP-based logging server/client with 5 log levels, colourised output, file logging, and in-memory ring buffers |
| `scheduler` | Time-based, frequency-based, and cron-driven job schedulers with overlap protection, retries, timeouts, runtime job management, and stats |
| `quick_ipc` | Lightweight inter-process list manager — a TCP server that maintains named lists that remote clients can read/write |

---

## Installation

```bash
pip install utils_tx           # production
pip install -e ".[dev]"        # editable install with dev dependencies (pytest, pytest-cov)
```

---

## 1. `logger` Module — Logging Server & Client

A centralised logging system. A `LoggerServer` listens on TCP, and one or more `LoggerClient` instances send logs to it.

### Quick Start

**Start the server:**

```python
from utils_tx.logger import LoggerServer

server = LoggerServer(port=5050)
server.start_server()    # runs in a daemon thread — non-blocking
```

**Send logs from anywhere (same machine or network):**

```python
from utils_tx.logger import LoggerClient

client = LoggerClient("localhost:5050", "my_app")
client.info("System started")
client.warning("Disk space low")
client.error("Connection lost", name="db_module")
```

**Stop the server:**

```python
server.stop_server()
```

---

### LoggerServer — Full Reference

```python
server = LoggerServer(
    server_host='localhost',    # bind address
    port=5050,                  # TCP port
    print_debug=True,           # print DEBUG messages to stdout
    debug_log_file=None,        # file path to append DEBUG messages
    debug_max_in_mem=0,         # keep last N DEBUG messages in memory (0 = off)
    print_info=True,
    info_log_file=None,
    info_max_in_mem=0,
    print_success=True,
    success_log_file=None,
    success_max_in_mem=0,
    print_warning=True,
    warning_log_file=None,
    warning_max_in_mem=0,
    print_error=True,
    error_log_file=None,
    error_max_in_mem=0,
    color_print=True,           # colourise console output (via colorama)
    max_workers=20              # thread pool size for client handling
)
server.start_server()
server.stop_server()
```

#### Retrieving In-Memory Logs

```python
# All levels as 5 nested lists: [debug, info, success, warning, error]
all_logs = server.get_logs()

# Single level by constant:
info_logs = server.get_logs(LOG_INFO)

# Single level by string name:
info_logs = server.get_logs("info")
```

#### Per-Level File Logging

Enable file output by passing a path. Directories are auto-created.

```python
server = LoggerServer(
    info_log_file="logs/info.log",
    error_log_file="logs/error.log",
    info_max_in_mem=100,      # keep last 100 INFO messages in memory too
)
```

#### Colour Reference

| Level | Constant | Colour |
|-------|----------|--------|
| DEBUG | `LOG_DEBUG = 1` | Cyan |
| INFO | `LOG_INFO = 2` | Blue |
| SUCCESS | `LOG_SUCCESS = 3` | Green |
| WARNING | `LOG_WARNING = 4` | Yellow |
| ERROR | `LOG_ERROR = 5` | Red |

---

### LoggerClient — Full Reference

```python
client = LoggerClient(
    "host:port",               # server address (e.g. "localhost:5050")
    "client_name",             # name embedded in every log message
    timeout=5.0                # socket connection timeout
)

client.debug("message", name=None)     # optional secondary name
client.info("message", name=None)
client.success("message", name=None)
client.warning("message", name=None)
client.error("message", name=None)
```

If the server is unreachable, the client prints an error to stdout instead of raising an exception — safe for production use where logging should never crash the application.

---

### Architecture

```
┌──────────────┐     TCP (length-prefixed JSON)      ┌──────────────┐
│ LoggerClient │ ──────────────────────────────────> │ LoggerServer │
│  (process A) │     connect → send → close          │  port 5050   │
└──────────────┘                                     │              │
                                                     │  ┌─ stdout   │
┌──────────────┐                                     │  ├─ file     │
│ LoggerClient │                                     │  └─ mem      │
│  (process B) │                                     └──────────────┘
└──────────────┘
```

Wire format: **4-byte big-endian payload length** followed by **JSON** `{"type": <int>, "message": <str>}`.

---

## 2. `scheduler` Module — Job Schedulers

Two scheduler flavours for different use cases.

### JResponse & Job

Every job produces a `JResponse` that carries either the return value or the exception.

```python
from utils_tx.scheduler import Job, JResponse

def add(a, b):
    return a + b

job = Job(add, 2, 3)

# Threaded (default) — run() returns immediately, result via wait_for_result():
resp = job.run()                       # JResponse(data=<Thread>, is_error=False)
result = job.wait_for_result(timeout=5)  # JResponse(data=5, is_error=False)
print(result.data)  # 5

# Synchronous — run() blocks until done:
job.use_thread = False
resp = job.run()                       # JResponse(data=5, is_error=False)
print(resp.data)  # 5

# Error handling — exceptions are wrapped, never propagated:
def will_fail():
    raise ValueError("oops")

job = Job(will_fail)
job.use_thread = False
resp = job.run()
print(resp.is_error)  # True
print(resp.error)     # ValueError("oops")
```

### TimeBasedScheduler

Fires jobs at fixed wall-clock times each day.

```python
from utils_tx.scheduler import TimeBasedScheduler, Job

def ping():
    print("Ping!")

# Configure: run ping() at 09:30 and 16:45 every weekday
scheduler = TimeBasedScheduler(
    job_dict={
        "09:30:00": Job(ping),
        "16:45:00": Job(ping),
    },
    check_freq=5,                        # check every 5 seconds
    skip_days=('saturday', 'sunday'),    # skip weekends (default)
    skip_dates=('25-12-2025', '01-01-2026'),  # skip holidays
    verbose=True,                        # print scheduler messages
    logger_client=None,                  # optional LoggerClient for remote logging
)

scheduler.start_scheduler()   # non-blocking, runs in daemon thread
# ... let it run ...
scheduler.stop_scheduler()

# View today's execution history:
history = scheduler.get_runned_today()
# [{"timestamp": "09:30:00", "executed_at": "09:30:05", "jobs": [...]}, ...]
```

**Constructor parameters:**

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `job_dict` | `Dict[str, Job \| list \| tuple]` | required | Map `"HH:MM:SS"` → job(s) |
| `check_freq` | `int \| float` | `10` | Loop interval in seconds |
| `skip_days` | `list \| tuple \| None` | `('saturday', 'sunday')` | Day names to skip |
| `skip_dates` | `list \| tuple \| None` | `None` | Date strings `"DD-MM-YYYY"` to skip |
| `verbose` | `bool` | `True` | Print messages to stdout |
| `logger_client` | `LoggerClient \| None` | `None` | Remote log all messages |

### FrequencyBasedScheduler

Fires jobs at fixed time intervals (e.g. every 30 seconds). Supports an optional daily operation window.

```python
from utils_tx.scheduler import FrequencyBasedScheduler, Job

def poll_sensor():
    print("Polling sensor...")

scheduler = FrequencyBasedScheduler(
    job_dict={
        30: Job(poll_sensor),          # every 30 seconds
        300: [Job(task_a), Job(task_b)],  # every 5 minutes, two jobs
    },
    check_freq=1,                       # tick every 1 second
    skip_days=('saturday', 'sunday'),
    skip_dates=None,
    operation_window=("09:00:00", "17:00:00"),  # only run during market hours
    verbose=True,
    logger_client=None,
)

scheduler.start_scheduler()
# ... let it run ...
scheduler.stop_scheduler()

# View last-run timestamps (monotonic):
last = scheduler.get_last_runned()
# {30: 12345.678, 300: 12345.678}
```

**Operation window supports midnight-crossing windows:**

```python
# Allow execution from 10pm to 4am:
operation_window=("22:00:00", "04:00:00")
```

---

## 4. `Scheduler` — Cron-Driven Production Scheduler

A cron-driven scheduler designed for 24x7 operation. The job dict maps **cron expressions** to `Job` instances (or lists of jobs). Built-in reliability controls:

- **Overlap protection** — a job won't start a new instance while one is running (`allow_overlap=False` by default; raise it with `max_concurrent`).
- **Retry on failure** — retry failed jobs up to `max_retries` times with `retry_delay` between attempts.
- **Timeout watchdog** — jobs exceeding `job_timeout` are flagged and reported (the runaway thread is never forcibly killed, but its slot stays blocked from overlapping).
- **Catch-up** — optionally fire once for cron minutes missed while the scheduler was stalled (`catch_up_window` bounds how far back).
- **Runtime job management** — add/remove/enable/disable jobs without restarting.
- **Execution stats** — runs, successes, failures, retries, timeouts, durations, and per-schedule aggregates for monitoring.

All of the reliability features are fully configurable via `__init__` and can be turned off.

### Quick Start

```python
from utils_tx.scheduler import Scheduler, Job

def ping():
    print("Ping!")

# Fire ping() every 5 minutes, Mon-Fri, and at 16:45 daily:
scheduler = Scheduler(
    {
        "*/5 * * * 1-5": Job(ping),
        "45 16 * * *":   Job(ping),
    },
    check_freq=1,
    skip_days=None,                      # cron already encodes weekdays
    operation_window=("09:30:00", "16:00:00"),  # optional market-hours gate
    allow_overlap=False,                 # overlap protection ON (default)
    retry_on_failure=True, max_retries=3, retry_delay=2.0,
    job_timeout=60,                      # flag jobs running > 60s
    catch_up=False,
    logger_client=None,
)

scheduler.start_scheduler()   # non-blocking daemon thread
scheduler.stop_scheduler()
```

### Cron Expressions

Two field layouts are supported:

- **5-field** (standard): `minute hour day-of-month month day-of-week`.
- **6-field** (second precision): `second minute hour day-of-month month day-of-week` — e.g. `*/10 * * * * *` fires every 10 seconds. For second-level cron keep `check_freq` at 1 or lower.

Fields support `*`, `?`, lists (`1,3,5`), ranges (`1-5`), steps (`*/5`, `0-30/5`), and month/day names (`jan`, `mon-fri`). Aliases: `@hourly`, `@daily`/`@midnight`, `@weekly`, `@monthly`, `@yearly`/`@annually`, and `@startup` (run once when the scheduler starts).

```python
from utils_tx.scheduler import CronExpression

cron = CronExpression("0 9 * * mon-fri")
cron.matches(datetime(2026, 8, 3, 9, 0))   # True (Monday 09:00)
cron.next(datetime(2026, 8, 2, 10, 0))      # next Monday 09:00

sec = CronExpression("*/10 * * * * *")      # every 10 seconds
sec.matches(datetime(2026, 8, 3, 9, 30, 10))  # True
sec.next(datetime(2026, 8, 3, 9, 30, 35))     # 09:30:40
```

#### `@startup` — run jobs when the scheduler starts

Jobs under the `@startup` key run once each time `start_scheduler()` is called
(and immediately if added at runtime while the scheduler is already running).
They ignore `skip_days`, `skip_dates`, and `operation_window`.

```python
scheduler = Scheduler(
    {
        "@startup": [Job(load_config), Job(warm_up_cache)],
        "*/10 * * * * *": Job(refresh_ticker),   # every 10 seconds
    },
    check_freq=1,          # required for second-level cron
)
scheduler.start_scheduler()   # loads config + warms cache immediately
```

### Runtime Job Management

```python
scheduler.add_job("30 9 * * 1-5", Job(open_positions))   # new schedule
scheduler.append_job("30 9 * * 1-5", Job(send_report))   # add to existing
scheduler.remove_job("30 9 * * 1-5")                     # remove entirely
scheduler.remove_job_index("30 9 * * 1-5", 0)            # remove one job
scheduler.disable_job("30 9 * * 1-5")                    # pause a schedule
scheduler.enable_job("30 9 * * 1-5")
scheduler.set_enabled("30 9 * * 1-5", True)
scheduler.get_jobs()                                     # snapshot
scheduler.next_run("30 9 * * 1-5")                       # next fire time
```

### Stats & Introspection

```python
scheduler.is_running()          # bool
scheduler.uptime()              # int seconds since start_scheduler()
scheduler.get_start_time()      # wall-clock datetime of last start
scheduler.pause() / .resume()   # global execution gate
scheduler.wait_for_all(timeout=5)

stats = scheduler.get_stats()   # global + per-schedule breakdown
# {"runs": ..., "successes": ..., "failures": ..., "retries": ...,
#  "timeouts": ..., "overlap_skipped": ..., "catch_up_fired": ...,
#  "uptime": ..., "jobs": {"*/5 * * * 1-5": {...aggregate stats...}}}

scheduler.get_job_stats("*/5 * * * 1-5")   # per-schedule aggregate
scheduler.get_history(limit=10)            # recent execution records
scheduler.get_running_jobs()               # currently running schedules
scheduler.get_runned_today()               # today's execution records
```

> **Note on timeouts:** Python threads cannot be forcibly terminated. A job that exceeds `job_timeout` is flagged, reported, and its slot remains blocked from overlapping until the runaway finishes. Use it as a monitoring/safety signal, not a hard kill.

---

## 3. `quick_ipc` Module — Inter-Process List Manager

A TCP server that maintains named lists in memory. Multiple processes can connect, append, read, and delete items from shared lists.

### Server

```python
from utils_tx.quick_ipc import qipc_server

server = qipc_server(host='127.0.0.1', port=5051)
server.start_server()          # non-blocking daemon thread
# ... use from other processes ...
server.stop_server()
```

### Client

```python
from utils_tx.quick_ipc import qipc_client

client = qipc_client("127.0.0.1", 5051)

# The list must exist before adding to it:
client.create_list("prices")
client.add_item("prices", 100.5)
client.add_item("prices", 101.2)
client.add_item("prices", 99.8)

# Or create explicitly with a size limit (oldest items auto-evicted):
client.create_list("trades", size_limit=1000)

# Read:
latest = client.get_last("prices")      # 99.8
all_prices = client.get_list("prices")   # [100.5, 101.2, 99.8]

# Find:
idx = client.get_element_index("prices", 101.2)  # 1

# Delete:
client.delete_last_element("prices")
client.delete_element_index("prices", 0)
```

**`qipc_client` methods:**

| Method | Description |
|--------|-------------|
| `create_list(name, size_limit=None)` | Create a named list (optional max size) |
| `add_item(name, item)` | Append item (list must already exist) |
| `get_last(name)` | Return most recent item |
| `get_list(name)` | Return full list |
| `delete_last_element(name)` | Remove most recent item |
| `delete_element_index(name, index)` | Remove item at index |
| `get_element_index(name, element)` | Return index of first match, or `-1` |

### Full Integration Example

```python
# Process 1 — Server
server = qipc_server(port=5051)
server.start_server()

# Process 2 — Producer
producer = qipc_client("127.0.0.1", 5051)
producer.create_list("events")
producer.add_item("events", "login")
producer.add_item("events", "trade_executed")

# Process 3 — Consumer
consumer = qipc_client("127.0.0.1", 5051)
events = consumer.get_list("events")  # ["login", "trade_executed"]
latest = consumer.get_last("events")  # "trade_executed"
```

---

## Putting It All Together

A realistic usage pattern: a logger server, a scheduler that polls market data, and an IPC list to share results.

```python
from utils_tx.logger import LoggerServer, LoggerClient
from utils_tx.scheduler import FrequencyBasedScheduler, Job
from utils_tx.quick_ipc import qipc_server, qipc_client
import time

# 1. Start the logger server
log_server = LoggerServer(port=5050, info_max_in_mem=100)
log_server.start_server()

# 2. Start the IPC server
ipc_server = qipc_server(port=5051)
ipc_server.start_server()

# 3. Create clients
logger = LoggerClient("localhost:5050", "market_bot")
ipc = qipc_client("127.0.0.1", 5051)
ipc.create_list("prices")

# 4. Define a job
def fetch_price():
    price = 100.0  # pretend we fetched from an exchange
    ipc.add_item("prices", price)
    logger.info(f"Fetched price: {price}")

# 5. Schedule it every 10 seconds during market hours
scheduler = FrequencyBasedScheduler(
    job_dict={10: Job(fetch_price)},
    operation_window=("09:30:00", "16:00:00"),
    logger_client=logger,
)
scheduler.start_scheduler()

time.sleep(30)
scheduler.stop_scheduler()

# 6. Check what was collected
prices = ipc.get_list("prices")
logger.info(f"Collected {len(prices)} price points")

# 7. Read logs from server memory
info_logs = log_server.get_logs("info")

log_server.stop_server()
ipc_server.stop_server()
```

---

## Development

```bash
git clone https://github.com/ASH-SuperUser/utils_tx.git
cd utils_tx
pip install -e ".[dev]"
pytest tests/ -v
```

---

## License

MIT
