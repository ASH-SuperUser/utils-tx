import re
import threading
import time
from collections import deque
from datetime import datetime, timedelta
from typing import Any, Optional, Union, Dict, List, Tuple

from utils_tx.logger import LoggerClient


class JResponse:
    """Wraps a job execution result, carrying either a return value or an error.

    Every ``Job.run()`` call produces a ``JResponse``, allowing the caller to
    inspect both the data and error state without exception handling.

    Attributes:
        data: The return value of the executed function, or ``None`` on error.
        is_error: ``True`` if the function raised an exception.
        error: The exception instance, or ``None`` on success.
    """

    def __init__(self, return_data: Any, error_bool: bool, error: Optional[Exception] = None) -> None:
        self.data = return_data
        self.is_error = error_bool
        self.error = error

    def __repr__(self) -> str:
        return f"JResponse(data={self.data}, is_error={self.is_error}, error={self.error})"

    def __str__(self) -> str:
        return f"JResponse(data={self.data}, is_error={self.is_error}, error={self.error})"


class Job:
    """Wraps a callable and its arguments for execution inside a scheduler.

    Supports both threaded (default) and synchronous execution. When run in a
    thread, the actual result is obtained later via ``wait_for_result()``.

    Attributes:
        use_thread: If ``True`` (default), ``run()`` launches the callable in a
            daemon thread and returns immediately with a ``JResponse`` whose
            ``data`` is the ``threading.Thread`` object. If ``False``, ``run()``
            blocks and returns the final ``JResponse`` directly.
        result: Populated after execution completes. Either a ``JResponse`` or
            ``None`` if the job has not been run yet.
    """

    def __init__(self, func, *args, **kwargs) -> None:
        """Wrap a callable for deferred execution.

        Args:
            func: The callable to execute.
            *args: Positional arguments forwarded to ``func``.
            **kwargs: Keyword arguments forwarded to ``func``.
        """
        self._fn = func
        self._args = args
        self._kwargs = kwargs
        self.use_thread: bool = True

        self.result: Optional[JResponse] = None
        self._thread: Optional[threading.Thread] = None

    def _execute_wrapper(self) -> JResponse:
        """Execute the wrapped function and return a ``JResponse``.

        Catches any exception raised by ``func`` and wraps it in a ``JResponse``
        with ``is_error=True`` rather than propagating it.

        Returns:
            A ``JResponse`` containing either the function's return value or the
            exception that was raised.
        """
        try:
            fun_out = self._fn(*self._args, **self._kwargs)
            return JResponse(fun_out, False, None)
        except Exception as e:
            return JResponse(None, True, e)

    def run(self) -> JResponse:
        """Execute the wrapped callable.

        Behaviour depends on ``self.use_thread``:

        - **Threaded** (default): Starts the callable in a daemon thread and
          returns a ``JResponse`` whose ``data`` is the ``threading.Thread``
          object. Call ``wait_for_result()`` later to obtain the actual result.
        - **Synchronous**: Executes the callable in the current thread and
          returns the final ``JResponse`` directly.

        Returns:
            A ``JResponse``. In threaded mode the ``data`` field holds the
            ``Thread`` object, not the function's return value.
        """
        if not self.use_thread:
            self.result = self._execute_wrapper()
            return self.result
        else:
            def thread_target():
                self.result = self._execute_wrapper()

            self._thread = threading.Thread(target=thread_target, daemon=True)
            self._thread.start()

            return JResponse(return_data=self._thread, error_bool=False, error=None)

    def wait_for_result(self, timeout: Optional[float] = None) -> Optional[JResponse]:
        """Block until the background thread finishes and return the actual result.

        Only meaningful when ``use_thread`` is ``True``. If the thread is already
        finished or ``use_thread`` was ``False``, returns immediately.

        Args:
            timeout: Maximum seconds to wait. ``None`` means wait indefinitely.

        Returns:
            The ``JResponse`` produced by the callable, or ``None`` if the job
            has not been started yet.
        """
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)
        return self.result

    def execute(self) -> JResponse:
        """Execute the wrapped callable synchronously and return a ``JResponse``.

        Unlike ``run()`` this never spawns a thread and ignores ``use_thread``:
        it always blocks until the callable finishes and returns the final
        ``JResponse`` directly. Safe to call from any thread, including from a
        scheduler that needs retry/timeout control over the execution.

        Returns:
            A ``JResponse`` containing either the function's return value or the
            exception that was raised.
        """
        self.result = self._execute_wrapper()
        return self.result


class TimeBasedScheduler:
    """Schedules jobs at fixed wall-clock times each day.

    The scheduler accepts a dict mapping time strings (``"HH:MM:SS"``) to
    ``Job`` instances (or lists of jobs). It runs a background loop that checks
    whether the current time has reached or passed each configured time slot.
    Each slot fires at most once per calendar day. Support for skip days
    (e.g. weekends) and skip dates (specific holidays) is built in.

    The scheduler tolerates individual job failures and top-level loop
    exceptions without crashing, making it suitable for long-running unattended
    operation.

    Attributes:
        job_dict: Dict mapping ``"HH:MM:SS"`` keys to ``Job``, ``List[Job]``,
            or ``Tuple[Job, ...]`` values.
        check_freq: Seconds between loop iterations.
        skip_days: Set of lowercase day names (e.g. ``{"saturday", "sunday"}``)
            on which no jobs are executed.
        skip_dates: Set of date strings (``"DD-MM-YYYY"``) on which no jobs
            are executed.
        verbose: If ``True``, scheduler messages are printed to stdout.
        logger_client: Optional ``LoggerClient`` for remote logging of
            scheduler messages and job errors.
    """

    def __init__(
        self,
        job_dict: Dict[str, Union[Job, List[Job], Tuple[Job, ...]]],
        check_freq: Union[int, float] = 10,
        skip_days: Optional[Union[list, tuple]] = ('saturday', 'sunday'),
        skip_dates: Optional[Union[list, tuple]] = None,
        verbose: bool = True,
        logger_client: Optional[LoggerClient] = None,
    ) -> None:
        """Initialise a TimeBasedScheduler.

        Args:
            job_dict: Dict mapping ``"HH:MM:SS"`` schedule times to the
                ``Job`` (or list/tuple of jobs) to execute at that time.
            check_freq: How often (in seconds) the background loop checks the
                current time. Lower values give more precise scheduling at the
                cost of higher CPU usage. Defaults to 10.
            skip_days: Sequence of day names to skip, e.g. ``("saturday",)``.
                Case-insensitive; whitespace is stripped. Defaults to weekends.
                Pass an empty sequence or ``None`` to disable.
            skip_dates: Sequence of date strings in ``"DD-MM-YYYY"`` format to
                skip. Defaults to ``None`` (no date-based skipping).
            verbose: If ``True`` (default), print scheduler messages to stdout.
            logger_client: Optional ``LoggerClient`` instance. When provided,
                scheduler events and job errors are also logged via
                ``logger_client.info()``.
        """
        self.job_dict = job_dict
        self.check_freq = check_freq
        self.verbose = verbose
        self.logger_client = logger_client

        self.skip_days = set(day.strip().lower() for day in skip_days) if skip_days else set()
        self.skip_dates = set(date.strip() for date in skip_dates) if skip_dates else set()

        self._current_date_str: str = datetime.now().strftime('%Y-%m-%d')
        self._runned_today: List[Dict[str, Any]] = []
        self._executed_timestamps_today = set()

        self._scheduler_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()

    def start_scheduler(self) -> None:
        """Start the scheduler background loop in a daemon thread.

        The loop runs until ``stop_scheduler()`` is called. Safe to call
        multiple times — subsequent calls are no-ops if the scheduler is
        already running.
        """
        with self._lock:
            if self._scheduler_thread and self._scheduler_thread.is_alive():
                return

            self._stop_event.clear()
            self._scheduler_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
            self._scheduler_thread.start()

    def stop_scheduler(self) -> None:
        """Gracefully stop the scheduler background loop.

        Signals the stop event, then waits up to 5 seconds for the scheduler
        thread to finish.
        """
        with self._lock:
            self._stop_event.set()
        if self._scheduler_thread:
            self._scheduler_thread.join(timeout=5)

    def get_runned_today(self) -> List[Dict[str, Any]]:
        """Return a snapshot of today's execution history.

        Each entry is a dict with keys ``"timestamp"`` (the scheduled time),
        ``"executed_at"`` (wall-clock time when the slot was triggered), and
        ``"jobs"`` (list of ``Job`` objects that were started).

        Returns:
            A list of execution records for the current calendar day. An empty
            list if no jobs have fired yet.
        """
        with self._lock:
            return list(self._runned_today)

    def _log(self, message: str, is_error: bool = False) -> None:
        """Conditionally print and/or remotely log a scheduler message.

        Args:
            message: The message string to output.
            is_error: Reserved for future severity differentiation. Currently
                unused in the method body.
        """
        if self.verbose:
            print(message)
        if self.logger_client:
            self.logger_client.info(message)

    def _scheduler_loop(self) -> None:
        """Main background loop: check time, execute due jobs, then sleep.

        On each iteration:

        1. Detect calendar-day rollover and clear execution tracking.
        2. Skip if the current day or date is in the skip sets.
        3. Iterate over ``job_dict``; for every time slot whose key is
           ``<=`` current time and has not yet fired today, run the
           associated job(s).
        4. Individual job ``run()`` failures are caught and logged; they
           never crash the loop.
        5. A top-level ``try/except`` ensures any other unexpected exception
           is logged and the loop continues.
        """
        while not self._stop_event.is_set():
            try:
                now = datetime.now()
                today_str = now.strftime('%Y-%m-%d')

                with self._lock:
                    if today_str != self._current_date_str:
                        self._current_date_str = today_str
                        self._runned_today.clear()
                        self._executed_timestamps_today.clear()

                day_name = now.strftime('%A').lower()
                formatted_ddmmyyyy = now.strftime('%d-%m-%Y')

                if day_name in self.skip_days or formatted_ddmmyyyy in self.skip_dates:
                    self._stop_event.wait(self.check_freq)
                    continue

                current_time_str = now.strftime('%H:%M:%S')

                for target_time_str, execution_unit in self.job_dict.items():
                    if target_time_str <= current_time_str:
                        with self._lock:
                            if target_time_str in self._executed_timestamps_today:
                                continue

                        jobs_to_run = []
                        if isinstance(execution_unit, (list, tuple)):
                            jobs_to_run = list(execution_unit)
                        elif isinstance(execution_unit, Job):
                            jobs_to_run = [execution_unit]

                        executed_jobs = []
                        for job in jobs_to_run:
                            try:
                                job.run()
                                executed_jobs.append(job)
                            except Exception as job_err:
                                msg = f"[Scheduler Error] Error launching job for {target_time_str}: {job_err}"
                                self._log(msg, is_error=True)

                        with self._lock:
                            self._executed_timestamps_today.add(target_time_str)
                            self._runned_today.append({
                                "timestamp": target_time_str,
                                "executed_at": datetime.now().strftime('%H:%M:%S'),
                                "jobs": executed_jobs
                            })

            except Exception as loop_error:
                msg = f"[Scheduler Loop Error] Unexpected anomaly: {loop_error}"
                self._log(msg, is_error=True)

            self._stop_event.wait(self.check_freq)


class FrequencyBasedScheduler:
    """Schedules jobs to run at fixed time intervals (e.g. every 30 seconds).

    Unlike ``TimeBasedScheduler``, this scheduler uses ``time.monotonic()`` to
    measure elapsed real time between executions. Multiple job groups, each
    with a different interval, can be configured simultaneously. The scheduler
    also supports skip days, skip dates, and an optional daily operation window.

    The scheduler tolerates individual job failures and top-level loop
    exceptions without crashing.

    Attributes:
        job_dict: Dict mapping interval-in-seconds (``int``) to ``Job``,
            ``List[Job]``, or ``Tuple[Job, ...]`` values.
        check_freq: Seconds between loop ticks. A lower value improves
            interval precision. Defaults to 1.
        skip_days: Set of lowercase day names to skip entirely.
        skip_dates: Set of date strings (``"DD-MM-YYYY"``) to skip.
        operation_window: Optional ``(start, end)`` tuple of ``"HH:MM:SS"``
            strings. Jobs only execute when the current time is within the
            window. Supports windows that cross midnight (e.g. ``"22:00:00"``
            to ``"04:00:00"``).
        verbose: If ``True``, scheduler messages are printed to stdout.
        logger_client: Optional ``LoggerClient`` for remote logging.
    """

    def __init__(
        self,
        job_dict: Dict[int, Union[Job, List[Job], Tuple[Job, ...]]],
        check_freq: Union[int, float] = 1,
        skip_days: Optional[Union[list, tuple]] = ('saturday', 'sunday'),
        skip_dates: Optional[Union[list, tuple]] = None,
        operation_window: Optional[Union[list, tuple]] = None,
        verbose: bool = True,
        logger_client: Optional[LoggerClient] = None,
    ) -> None:
        """Initialise a FrequencyBasedScheduler.

        Args:
            job_dict: Dict mapping interval-in-seconds (``int``) to the
                ``Job`` (or list/tuple of jobs) to execute at that interval.
            check_freq: How often (in seconds) the background loop evaluates
                interval conditions. Defaults to 1.
            skip_days: Sequence of day names to skip, e.g. ``("saturday",)``.
                Case-insensitive; whitespace is stripped. Defaults to weekends.
                Pass an empty sequence or ``None`` to disable.
            skip_dates: Sequence of date strings in ``"DD-MM-YYYY"`` format to
                skip. Defaults to ``None``.
            operation_window: Optional tuple of two ``"HH:MM:SS"`` strings
                defining the daily window in which jobs may run. Supports
                midnight-crossing windows (e.g. start > end).
            verbose: If ``True`` (default), print scheduler messages to stdout.
            logger_client: Optional ``LoggerClient`` instance. When provided,
                scheduler events and job errors are also logged via
                ``logger_client.info()``.
        """
        self.job_dict = job_dict
        self.check_freq = check_freq
        self.verbose = verbose
        self.logger_client = logger_client

        self.skip_days = set(day.strip().lower() for day in skip_days) if skip_days else set()
        self.skip_dates = set(date.strip() for date in skip_dates) if skip_dates else set()
        self.operation_window = operation_window

        self._scheduler_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()

        self._last_run_timestamps: Dict[int, float] = {}

        current_monotonic = time.monotonic()
        for interval in self.job_dict.keys():
            self._last_run_timestamps[interval] = current_monotonic

    def start_scheduler(self) -> None:
        """Start the frequency scheduler background loop in a daemon thread.

        The loop runs until ``stop_scheduler()`` is called. Safe to call
        multiple times — subsequent calls are no-ops if already running.
        """
        with self._lock:
            if self._scheduler_thread and self._scheduler_thread.is_alive():
                return

            self._stop_event.clear()
            self._scheduler_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
            self._scheduler_thread.start()

    def stop_scheduler(self) -> None:
        """Gracefully stop the frequency scheduler background loop.

        Signals the stop event, then waits up to 5 seconds for the scheduler
        thread to finish.
        """
        with self._lock:
            self._stop_event.set()
        if self._scheduler_thread:
            self._scheduler_thread.join(timeout=5)

    def get_last_runned(self) -> Dict[int, float]:
        """Return a snapshot of the last-run timestamps for each interval.

        The values are ``time.monotonic()`` timestamps representing the most
        recent execution of each interval group.

        Returns:
            A dict mapping interval (seconds) to its last-run monotonic time.
        """
        with self._lock:
            return dict(self._last_run_timestamps)

    def _log(self, message: str, is_error: bool = False) -> None:
        """Conditionally print and/or remotely log a scheduler message.

        Args:
            message: The message string to output.
            is_error: Reserved for future severity differentiation. Currently
                unused in the method body.
        """
        if self.verbose:
            print(message)
        if self.logger_client:
            self.logger_client.info(message)

    def _is_within_operation_window(self, current_time_str: str) -> bool:
        """Check whether the current time falls within the configured operation window.

        Supports three scenarios:

        - No window configured: always returns ``True``.
        - Normal window (start <= end): returns ``True`` if start <= time <= end.
        - Midnight-crossing window (start > end): returns ``True`` if time >=
          start or time <= end (e.g. ``"22:00:00"`` to ``"04:00:00"``).

        Args:
            current_time_str: Time string in ``"HH:MM:SS"`` format.

        Returns:
            ``True`` if the time is within the window, ``False`` otherwise.
            Returns ``False`` (and logs the error) if the window configuration
            is malformed.
        """
        if not self.operation_window:
            return True

        try:
            start_str, end_str = self.operation_window

            if start_str <= end_str:
                return start_str <= current_time_str <= end_str
            else:
                return current_time_str >= start_str or current_time_str <= end_str
        except Exception as e:
            self._log(f"[Scheduler Window Error] Bad operation_window configuration format: {e}", is_error=True)
            return False

    def _scheduler_loop(self) -> None:
        """Main background loop: check interval conditions and execute due jobs.

        On each iteration:

        1. Skip entirely if today is a skip day or skip date.
        2. Skip if the current time is outside ``operation_window``.
        3. For each interval group in ``job_dict``, check whether the elapsed
           monotonic time since the last run exceeds the configured interval.
           If so, execute the associated job(s) and update the last-run
           timestamp.
        4. Individual job failures are caught and logged; they never crash
           the loop.
        5. A top-level ``try/except`` ensures any other unexpected exception
           is logged and execution continues.
        """
        while not self._stop_event.is_set():
            try:
                now = datetime.now()

                day_name = now.strftime('%A').lower()
                formatted_ddmmyyyy = now.strftime('%d-%m-%Y')

                if day_name in self.skip_days or formatted_ddmmyyyy in self.skip_dates:
                    self._stop_event.wait(self.check_freq)
                    continue

                current_time_str = now.strftime('%H:%M:%S')
                if not self._is_within_operation_window(current_time_str):
                    self._stop_event.wait(self.check_freq)
                    continue

                current_monotonic = time.monotonic()

                for interval, execution_unit in self.job_dict.items():
                    with self._lock:
                        last_run = self._last_run_timestamps.get(interval, 0.0)

                    if current_monotonic - last_run >= interval:

                        jobs_to_run = []
                        if isinstance(execution_unit, (list, tuple)):
                            jobs_to_run = list(execution_unit)
                        elif isinstance(execution_unit, Job):
                            jobs_to_run = [execution_unit]

                        for job in jobs_to_run:
                            try:
                                job.run()
                            except Exception as job_err:
                                self._log(f"[Scheduler Job Error] Error executing job in interval {interval}s: {job_err}", is_error=True)

                        with self._lock:
                            self._last_run_timestamps[interval] = current_monotonic

            except Exception as loop_error:
                self._log(f"[Scheduler Loop Error] Global failure in frequency routine loop: {loop_error}", is_error=True)

            self._stop_event.wait(self.check_freq)


# ─────────────────────────────────────────────────────────────────────
# Cron expression parsing
# ─────────────────────────────────────────────────────────────────────

CRON_MONTH_NAMES = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "jun": 6, "jul": 7, "aug": 8,
    "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

CRON_DAY_NAMES = {
    "sunday": 0, "monday": 1, "tuesday": 2, "wednesday": 3, "thursday": 4,
    "friday": 5, "saturday": 6,
    "sun": 0, "mon": 1, "tue": 2, "wed": 3, "thu": 4, "fri": 5, "sat": 6,
}

CRON_ALIASES = {
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
    "@monthly": "0 0 1 * *",
    "@weekly": "0 0 * * 0",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@hourly": "0 * * * *",
}

_CRON_NAME_PATTERN = re.compile(r"\b[A-Za-z]{3,9}\b")


def _cron_name_to_number(field_str: str, name_map: Dict[str, int]) -> str:
    """Replace weekday/month names inside a cron field with their numeric value."""

    def repl(match: "re.Match[str]") -> str:
        return str(name_map.get(match.group(0).lower(), match.group(0)))

    return _CRON_NAME_PATTERN.sub(repl, field_str)


def _cron_field_value(text: str, lo: int, hi: int, field_str: str) -> int:
    """Parse and range-check a single numeric cron value."""
    if not text.isdigit():
        raise ValueError(f"Invalid cron value '{text}' in field '{field_str}'")
    value = int(text)
    if not (lo <= value <= hi):
        raise ValueError(f"Cron value {value} out of range [{lo}, {hi}] in field '{field_str}'")
    return value


def _parse_cron_field(field_str: str, lo: int, hi: int, name_map: Optional[Dict[str, int]] = None) -> set:
    """Parse a single cron field (minute/hour/dom/month/dow) into a set of values.

    Supports ``*``, ``?``, comma lists (``1,3,5``), ranges (``1-5``), steps
    (``*/5``, ``0-30/5``, ``5/10``), and their combinations. Month and day-of-week
    fields also accept English names (e.g. ``jan``, ``mon-fri``).
    """
    if name_map:
        field_str = _cron_name_to_number(field_str, name_map)

    values: set = set()
    for part in field_str.split(","):
        part = part.strip()
        if not part:
            continue
        if part in ("*", "?"):
            values.update(range(lo, hi + 1))
            continue

        if "/" in part:
            base, _, step_text = part.partition("/")
            if not step_text.isdigit() or int(step_text) < 1:
                raise ValueError(f"Invalid step '{step_text}' in cron field '{field_str}'")
            step = int(step_text)
            if base in ("*", "?"):
                start, end = lo, hi
            elif "-" in base:
                start_text, _, end_text = base.partition("-")
                start = _cron_field_value(start_text, lo, hi, field_str)
                end = _cron_field_value(end_text, lo, hi, field_str)
            else:
                start = _cron_field_value(base, lo, hi, field_str)
                end = hi
            for value in range(start, end + 1, step):
                values.add(value)
            continue

        if "-" in part:
            start_text, _, end_text = part.partition("-")
            start = _cron_field_value(start_text, lo, hi, field_str)
            end = _cron_field_value(end_text, lo, hi, field_str)
            if start > end:
                raise ValueError(f"Descending range '{part}' not supported in cron field '{field_str}'")
            values.update(range(start, end + 1))
            continue

        values.add(_cron_field_value(part, lo, hi, field_str))

    if not values:
        raise ValueError(f"Cron field '{field_str}' produced no values")
    return values


class CronExpression:
    """A parsed cron expression with matching and next-run support.

    Supports two field layouts:

    - **5-field** (standard): ``minute hour day-of-month month day-of-week``.
    - **6-field** (second precision): ``second minute hour day-of-month month
      day-of-week``. When seconds are present, matches are evaluated to the
      second (e.g. ``*/10 * * * * *`` fires every 10 seconds).

    Fields accept ``*``, ``?``, comma lists (``1,3,5``), ranges (``1-5``), steps
    (``*/5``, ``0-30/5``), month/day names (``jan``, ``mon-fri``), and the common
    ``@daily`` / ``@hourly`` / ``@weekly`` aliases.

    Following the standard cron rule, when both ``day-of-month`` and
    ``day-of-week`` are restricted (not ``*``), a match occurs if **either** one
    matches; when only one is restricted, only that one is enforced.
    """

    def __init__(self, expression: str) -> None:
        """Parse a cron expression.

        Args:
            expression: A 5-field or 6-field cron string or one of the ``@``
                aliases.

        Raises:
            ValueError: If the expression is not a valid cron string.
        """
        self.expression = str(expression).strip()
        if not self.expression:
            raise ValueError("Cron expression cannot be empty")

        canonical = CRON_ALIASES.get(self.expression.lower(), self.expression)
        parts = canonical.split()
        if len(parts) not in (5, 6):
            raise ValueError(
                f"Invalid cron expression '{expression}'. Expected 5 fields "
                "(minute hour dom month dow) or 6 fields (second minute hour "
                "dom month dow), or an alias like '@daily'."
            )
        self.has_seconds = len(parts) == 6
        self.canonical = " ".join(parts)

        try:
            if self.has_seconds:
                self.seconds = _parse_cron_field(parts[0], 0, 59)
                self.minutes = _parse_cron_field(parts[1], 0, 59)
                self.hours = _parse_cron_field(parts[2], 0, 23)
                self.dom = _parse_cron_field(parts[3], 1, 31)
                self.months = _parse_cron_field(parts[4], 1, 12, CRON_MONTH_NAMES)
                self.dow = _parse_cron_field(parts[5], 0, 7, CRON_DAY_NAMES)
            else:
                self.seconds = set(range(0, 60))
                self.minutes = _parse_cron_field(parts[0], 0, 59)
                self.hours = _parse_cron_field(parts[1], 0, 23)
                self.dom = _parse_cron_field(parts[2], 1, 31)
                self.months = _parse_cron_field(parts[3], 1, 12, CRON_MONTH_NAMES)
                self.dow = _parse_cron_field(parts[4], 0, 7, CRON_DAY_NAMES)
        except ValueError as exc:
            raise ValueError(f"Invalid cron expression '{expression}': {exc}")

        # Normalise Sunday 7 -> 0
        if 7 in self.dow:
            self.dow.discard(7)
            self.dow.add(0)

        self._dom_restricted = len(self.dom) < 31
        self._dow_restricted = len(self.dow) < 7

    def _matches_minute_level(self, dt: datetime) -> bool:
        """Check all fields except seconds (i.e. the minute/half-of-day part)."""
        if dt.minute not in self.minutes:
            return False
        if dt.hour not in self.hours:
            return False
        if dt.month not in self.months:
            return False

        dom_match = dt.day in self.dom
        # cron dow uses 0=Sunday; python weekday() uses 0=Monday -> shift by +1
        dow_match = (dt.weekday() + 1) % 7 in self.dow

        if self._dom_restricted and self._dow_restricted:
            return dom_match or dow_match
        if self._dom_restricted:
            return dom_match
        return dow_match

    def matches(self, dt: datetime) -> bool:
        """Return ``True`` if the given datetime matches this cron expression.

        Args:
            dt: The datetime to test against the expression.

        Returns:
            ``True`` if all fields match, ``False`` otherwise.
        """
        if self.has_seconds and dt.second not in self.seconds:
            return False
        return self._matches_minute_level(dt)

    def next(self, after: Optional[datetime] = None) -> Optional[datetime]:
        """Compute the next datetime strictly after ``after`` that matches.

        Args:
            after: The reference datetime (defaults to now). Seconds and
                microseconds are truncated to the precision of the expression.

        Returns:
            The next matching datetime, or ``None`` if no match exists within a
            10-year horizon.
        """
        base = after if after is not None else datetime.now()

        if not self.has_seconds:
            candidate = base.replace(second=0, microsecond=0) + timedelta(minutes=1)
            deadline = candidate + timedelta(days=3650)
            while candidate <= deadline:
                if self.matches(candidate):
                    return candidate
                candidate += timedelta(minutes=1)
            return None

        seconds = sorted(self.seconds)
        minute = base.replace(second=0, microsecond=0)
        deadline = minute + timedelta(days=3650)
        while minute <= deadline:
            if self._matches_minute_level(minute):
                for second in seconds:
                    candidate = minute.replace(second=second)
                    if candidate > base:
                        return candidate
            minute += timedelta(minutes=1)
        return None

    def __str__(self) -> str:
        return self.canonical

    def __repr__(self) -> str:
        return f"CronExpression({self.expression!r})"


# ─────────────────────────────────────────────────────────────────────
# Cron-driven production scheduler
# ─────────────────────────────────────────────────────────────────────


class _CronUnit:
    """Internal registry entry holding one cron expression and its jobs."""

    __slots__ = ("cron", "cron_str", "jobs", "enabled", "last_fired", "startup",
                 "running_count", "running_started")

    def __init__(self, cron: CronExpression, cron_str: str, jobs: List[Job],
                 startup: bool = False) -> None:
        self.cron = cron
        self.cron_str = cron_str
        self.jobs: List[Job] = jobs
        self.enabled: bool = True
        self.last_fired: Optional[str] = None
        self.startup: bool = startup
        # id(job) -> int instances currently running
        self.running_count: Dict[int, int] = {id(job): 0 for job in jobs}
        # (id(job), index) -> {instance_token: [monotonic_start, reported]}
        self.running_started: Dict[Tuple[int, int], Dict[int, List[Any]]] = {}


class Scheduler:
    """Cron-driven, production-grade scheduler with overlap/retry/timeout control.

    Unlike ``TimeBasedScheduler`` and ``FrequencyBasedScheduler``, this scheduler
    is driven by standard cron expressions. The job dict maps a cron string to a
    ``Job`` (or a list/tuple of ``Job`` instances) that should fire whenever the
    current time matches the expression. Each cron slot fires at most once per
    matching tick — every matching minute for 5-field expressions, every
    matching second for 6-field expressions (e.g. ``*/10 * * * * *`` fires every
    10 seconds). For second-level cron, keep ``check_freq`` at 1 or lower.

    The special key ``"@startup"`` runs its job(s) once each time the scheduler
    starts (and immediately if added at runtime while the scheduler is already
    running).

    Reliability features (all controllable from the constructor):

    - **Overlap protection** — a job will not start a new instance while one of
      its instances is already running (``allow_overlap=False`` by default). The
      concurrency cap per job can be raised via ``max_concurrent``.
    - **Retry on failure** — failed jobs can be retried up to ``max_retries``
      times with a ``retry_delay`` between attempts. Disabled by default.
    - **Execution timeout watchdog** — jobs exceeding ``job_timeout`` seconds are
      flagged and reported. Because Python threads cannot be forcibly killed, the
      runaway thread keeps running but is reported and its slot stays blocked
      from overlapping. Disabled by default.
    - **Catch-up** — optionally run once for cron minutes that were missed while
      the scheduler was stalled (e.g. a long blocking operation), bounded by
      ``catch_up_window``. Disabled by default.

    Jobs can be added, removed, enabled and disabled at runtime without
    restarting the scheduler, and full execution statistics are tracked for
    monitoring purposes.

    The scheduler tolerates individual job failures and top-level loop
    exceptions without crashing, making it suitable for 24x7 unattended
    operation. All scheduler methods are non-blocking; jobs run in daemon
    threads and never block the scheduler loop.

    Attributes:
        check_freq: Seconds between loop ticks.
        skip_days: Set of lowercase day names on which no jobs execute.
        skip_dates: Set of ``"DD-MM-YYYY"`` dates on which no jobs execute.
        operation_window: Optional ``(start, end)`` window of ``"HH:MM:SS"``
            strings (supports midnight-crossing windows).
        allow_overlap: If ``True``, multiple instances of a job may run
            concurrently (bounded by ``max_concurrent``).
        max_concurrent: Optional hard cap on concurrent instances per job.
        retry_on_failure: If ``True``, failed jobs are retried.
        max_retries: Max retry attempts per execution.
        retry_delay: Seconds to sleep between retry attempts.
        job_timeout: Seconds after which a running job is flagged as timed out.
        catch_up: If ``True``, missed cron minutes are executed once.
        catch_up_window: Seconds of history considered for catch-up.
        verbose: If ``True``, scheduler messages are printed to stdout.
        logger_client: Optional ``LoggerClient`` for remote logging.
    """

    def __init__(
        self,
        job_dict: Dict[str, Union[Job, List[Job], Tuple[Job, ...]]],
        check_freq: Union[int, float] = 1.0,
        skip_days: Optional[Union[list, tuple]] = ('saturday', 'sunday'),
        skip_dates: Optional[Union[list, tuple]] = None,
        operation_window: Optional[Union[list, tuple]] = None,
        verbose: bool = True,
        logger_client: Optional[LoggerClient] = None,
        allow_overlap: bool = False,
        max_concurrent: Optional[int] = None,
        retry_on_failure: bool = False,
        max_retries: int = 3,
        retry_delay: Union[int, float] = 1.0,
        job_timeout: Optional[Union[int, float]] = None,
        catch_up: bool = False,
        catch_up_window: Union[int, float] = 300,
        history_max: int = 200,
    ) -> None:
        """Initialise the cron scheduler.

        Args:
            job_dict: Dict mapping a cron expression string (5-field, 6-field
                with second precision, or the special ``"@startup"`` key) to a
                ``Job`` (or a list/tuple of jobs) to execute on each matching
                tick.
            check_freq: How often (in seconds) the background loop evaluates the
                schedule. Defaults to 1.
            skip_days: Sequence of lowercase day names (e.g. ``("saturday",)``)
                on which no jobs run. Defaults to weekends. Pass an empty
                sequence or ``None`` to disable.
            skip_dates: Sequence of ``"DD-MM-YYYY"`` dates to skip. Defaults to
                ``None``.
            operation_window: Optional tuple of two ``"HH:MM:SS"`` strings
                defining the daily window in which jobs may run. Supports
                midnight-crossing windows. Defaults to ``None`` (always open).
            verbose: If ``True`` (default), print scheduler messages to stdout.
            logger_client: Optional ``LoggerClient`` for remote logging of
                scheduler events and job outcomes.
            allow_overlap: If ``True``, a job may start a new instance while a
                previous instance is still running (bounded by ``max_concurrent``
                when set). Defaults to ``False`` (overlap protection on).
            max_concurrent: Optional hard cap on concurrent instances per job.
                When ``None`` and ``allow_overlap`` is ``False`` the cap is 1;
                when ``None`` and ``allow_overlap`` is ``True`` the cap is
                unlimited.
            retry_on_failure: If ``True``, jobs that raise are retried. Defaults
                to ``False``.
            max_retries: Maximum number of retries per execution. Defaults to 3.
            retry_delay: Seconds to wait between retries. Defaults to 1.0.
            job_timeout: Optional maximum seconds a single job instance may run
                before it is flagged as timed out. ``None`` disables the
                watchdog. Defaults to ``None``.
            catch_up: If ``True``, a cron slot that was missed while the
                scheduler was stalled fires once (bounded by
                ``catch_up_window``). Defaults to ``False``.
            catch_up_window: Seconds of history to consider when catching up.
                Defaults to 300.
            history_max: Maximum number of execution history records retained.
                Defaults to 200. ``0`` disables history.

        Raises:
            ValueError: If ``job_dict`` is not a dict, a cron key is invalid, or
                a numeric option is out of range.
            TypeError: If a job value is not a ``Job`` (or list/tuple of them).
        """
        if not isinstance(job_dict, dict):
            raise ValueError("job_dict must be a dict mapping cron strings to Job(s)")
        if check_freq is None or float(check_freq) <= 0:
            raise ValueError("check_freq must be a positive number")
        if max_concurrent is not None and max_concurrent < 1:
            raise ValueError("max_concurrent must be >= 1 or None")
        if max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        if float(retry_delay) < 0:
            raise ValueError("retry_delay must be >= 0")
        if job_timeout is not None and float(job_timeout) <= 0:
            raise ValueError("job_timeout must be > 0 or None")
        if float(catch_up_window) <= 0:
            raise ValueError("catch_up_window must be > 0")
        if history_max < 0:
            raise ValueError("history_max must be >= 0")

        self.job_dict = dict(job_dict)
        self.check_freq = float(check_freq)
        self.verbose = verbose
        self.logger_client = logger_client
        self.skip_days = set(day.strip().lower() for day in skip_days) if skip_days else set()
        self.skip_dates = set(date.strip() for date in skip_dates) if skip_dates else set()
        self.operation_window = operation_window
        self.allow_overlap = allow_overlap
        self.max_concurrent = max_concurrent
        self.retry_on_failure = retry_on_failure
        self.max_retries = max_retries
        self.retry_delay = float(retry_delay)
        self.job_timeout = job_timeout
        self.catch_up = catch_up
        self.catch_up_window = float(catch_up_window)
        self.history_max = history_max

        self._lock = threading.RLock()
        self._units: Dict[str, _CronUnit] = {}
        self._paused = False
        self._stop_event = threading.Event()
        self._scheduler_thread: Optional[threading.Thread] = None
        self._start_monotonic: Optional[float] = None
        self._start_wall: Optional[datetime] = None

        self._stats: Dict[str, Any] = {
            "runs": 0,
            "successes": 0,
            "failures": 0,
            "retries": 0,
            "timeouts": 0,
            "overlap_skipped": 0,
            "catch_up_fired": 0,
        }
        self._job_stats: Dict[Tuple[str, int], Dict[str, Any]] = {}
        self._history: deque = deque(maxlen=history_max)
        self._instance_seq = 0

        for cron_str, execution_unit in self.job_dict.items():
            self._register(cron_str, execution_unit)

    # ── registration helpers ─────────────────────────────────────────

    @staticmethod
    def _coerce_jobs(value: Any) -> List[Job]:
        """Normalise a job_dict value into a list of ``Job`` instances."""
        if isinstance(value, Job):
            return [value]
        if isinstance(value, (list, tuple)):
            jobs = list(value)
            for job in jobs:
                if not isinstance(job, Job):
                    raise TypeError(f"Each job in a list must be a Job, got {type(job).__name__}")
            return jobs
        raise TypeError(f"Job value must be a Job or list/tuple of Jobs, got {type(value).__name__}")

    @staticmethod
    def _new_job_stats() -> Dict[str, Any]:
        """Return a fresh per-job statistics accumulator."""
        return {
            "runs": 0,
            "successes": 0,
            "failures": 0,
            "retries": 0,
            "timeouts": 0,
            "overlap_skipped": 0,
            "total_duration": 0.0,
            "last_duration": 0.0,
            "last_run": None,
            "last_result": None,
            "last_error": None,
            "consecutive_failures": 0,
        }

    def _register(self, cron_str: str, execution_unit: Any, append: bool = False) -> str:
        """Register a cron expression and its job(s) into the unit registry."""
        key = cron_str.strip()
        startup = key.lower() == "@startup"
        # A never-matching expression used to hold @startup units in the loop
        cron = CronExpression("0 0 31 2 *") if startup else CronExpression(key)
        jobs = self._coerce_jobs(execution_unit)

        with self._lock:
            if key in self._units:
                if not append:
                    raise ValueError(f"Job schedule '{key}' is already registered")
                unit = self._units[key]
                for job in jobs:
                    unit.jobs.append(job)
                    unit.running_count[id(job)] = 0
            else:
                self._units[key] = _CronUnit(cron, key, jobs, startup=startup)
                unit = self._units[key]

        # A @startup schedule added while the scheduler is already running
        # should fire immediately.
        if startup and self.is_running():
            self._dispatch_unit(unit)
        return key

    # ── lifecycle ────────────────────────────────────────────────────

    def start_scheduler(self) -> None:
        """Start the scheduler background loop in a daemon thread.

        Records the start time (for ``uptime()``) and runs until
        ``stop_scheduler()`` is called. Any ``@startup`` schedules are dispatched
        once. Safe to call multiple times — subsequent calls are no-ops if the
        scheduler is already running.
        """
        with self._lock:
            if self._scheduler_thread and self._scheduler_thread.is_alive():
                return
            self._stop_event.clear()
            self._start_monotonic = time.monotonic()
            self._start_wall = datetime.now()
            self._scheduler_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
            self._scheduler_thread.start()
            for unit in self._units.values():
                if unit.startup:
                    self._dispatch_unit(unit)

    def stop_scheduler(self) -> None:
        """Gracefully stop the scheduler background loop.

        Signals the stop event and waits up to 5 seconds for the loop thread to
        finish. Already-running job threads are daemon threads and are left to
        finish on their own.
        """
        with self._lock:
            self._stop_event.set()
        if self._scheduler_thread:
            self._scheduler_thread.join(timeout=5)

    def is_running(self) -> bool:
        """Return ``True`` if the scheduler loop is currently running."""
        return bool(self._scheduler_thread and self._scheduler_thread.is_alive())

    def uptime(self) -> int:
        """Return whole seconds since ``start_scheduler()`` was called.

        Returns 0 if the scheduler has never been started.
        """
        if self._start_monotonic is None:
            return 0
        return int(time.monotonic() - self._start_monotonic)

    def get_uptime(self) -> int:
        """Alias for :meth:`uptime`."""
        return self.uptime()

    def get_start_time(self) -> Optional[datetime]:
        """Return the wall-clock datetime when the scheduler was last started."""
        return self._start_wall

    def pause(self) -> None:
        """Globally pause job execution.

        The loop keeps running but no jobs are dispatched until ``resume()`` is
        called. Already-running jobs are not interrupted.
        """
        with self._lock:
            self._paused = True

    def resume(self) -> None:
        """Resume job execution after a :meth:`pause`."""
        with self._lock:
            self._paused = False

    def is_paused(self) -> bool:
        """Return ``True`` if the scheduler is paused."""
        with self._lock:
            return self._paused

    def wait_for_all(self, timeout: Optional[float] = None) -> bool:
        """Block until no job instances are running.

        Args:
            timeout: Maximum seconds to wait. ``None`` waits indefinitely.

        Returns:
            ``True`` if all jobs finished within the timeout, ``False``
            otherwise.
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            with self._lock:
                busy = any(
                    any(count > 0 for count in unit.running_count.values())
                    for unit in self._units.values()
                )
            if not busy:
                return True
            if deadline is not None and time.monotonic() >= deadline:
                return False
            time.sleep(0.05)

    # ── runtime job management ───────────────────────────────────────

    def add_job(self, cron_str: str, execution_unit: Any) -> str:
        """Register a new cron schedule at runtime.

        Args:
            cron_str: The cron expression key.
            execution_unit: A ``Job`` or list/tuple of jobs.

        Returns:
            The normalised cron key.

        Raises:
            ValueError: If the cron key is already registered or invalid.
            TypeError: If ``execution_unit`` is not a ``Job``.
        """
        return self._register(cron_str, execution_unit, append=False)

    def append_job(self, cron_str: str, execution_unit: Any) -> None:
        """Append job(s) to an existing cron schedule, creating it if missing."""
        self._register(cron_str, execution_unit, append=True)

    def remove_job(self, cron_str: str) -> None:
        """Remove a cron schedule entirely.

        Args:
            cron_str: The cron expression key.

        Raises:
            KeyError: If the schedule does not exist.
        """
        with self._lock:
            if cron_str not in self._units:
                raise KeyError(cron_str)
            del self._units[cron_str]

    def remove_job_index(self, cron_str: str, index: int) -> None:
        """Remove a single job from a cron schedule by index.

        Args:
            cron_str: The cron expression key.
            index: Index of the job within the schedule's job list.

        Raises:
            KeyError: If the schedule does not exist.
            IndexError: If the index is out of range.
        """
        with self._lock:
            unit = self._units.get(cron_str)
            if unit is None:
                raise KeyError(cron_str)
            if index < 0 or index >= len(unit.jobs):
                raise IndexError(f"Index {index} out of range for schedule '{cron_str}'")
            job = unit.jobs.pop(index)
            unit.running_count.pop(id(job), None)
            shifted = {}
            for (jid, idx), starts in unit.running_started.items():
                if idx == index:
                    continue
                shifted[(jid, idx - 1 if idx > index else idx)] = starts
            unit.running_started = shifted

    def get_jobs(self) -> Dict[str, List[Job]]:
        """Return a snapshot of the current schedule: cron key -> job list."""
        with self._lock:
            return {cron_str: list(unit.jobs) for cron_str, unit in self._units.items()}

    def get_schedule(self) -> Dict[str, List[Job]]:
        """Alias for :meth:`get_jobs`."""
        return self.get_jobs()

    def set_enabled(self, cron_str: str, enabled: bool) -> None:
        """Enable or disable a cron schedule.

        Disabled schedules stay registered but are not dispatched until
        re-enabled.

        Raises:
            KeyError: If the schedule does not exist.
        """
        with self._lock:
            unit = self._units.get(cron_str)
            if unit is None:
                raise KeyError(cron_str)
            unit.enabled = bool(enabled)

    def enable_job(self, cron_str: str) -> None:
        """Enable a cron schedule."""
        self.set_enabled(cron_str, True)

    def disable_job(self, cron_str: str) -> None:
        """Disable a cron schedule (kept registered, not dispatched)."""
        self.set_enabled(cron_str, False)

    def is_job_enabled(self, cron_str: str) -> bool:
        """Return ``True`` if the cron schedule is enabled.

        Raises:
            KeyError: If the schedule does not exist.
        """
        with self._lock:
            unit = self._units.get(cron_str)
            if unit is None:
                raise KeyError(cron_str)
            return unit.enabled

    def next_run(self, cron_str: str, after: Optional[datetime] = None) -> Optional[datetime]:
        """Return the next datetime that the cron schedule will fire.

        Args:
            cron_str: The cron expression key.
            after: Reference datetime (defaults to now).

        Raises:
            KeyError: If the schedule does not exist.
        """
        with self._lock:
            unit = self._units.get(cron_str)
        if unit is None:
            raise KeyError(cron_str)
        return unit.cron.next(after)

    # ── stats and introspection ──────────────────────────────────────

    def _aggregate_job_stats(self, cron_str: str) -> Dict[str, Any]:
        """Aggregate per-index statistics for a cron schedule (lock held by caller)."""
        agg = self._new_job_stats()
        for (key_cron, _), stats in self._job_stats.items():
            if key_cron != cron_str:
                continue
            for field in ("runs", "successes", "failures", "retries", "timeouts",
                          "overlap_skipped", "consecutive_failures"):
                agg[field] += stats[field]
            agg["total_duration"] += stats["total_duration"]
            if stats["last_duration"]:
                agg["last_duration"] = stats["last_duration"]
            if stats["last_run"] and (agg["last_run"] is None or stats["last_run"] > agg["last_run"]):
                agg["last_run"] = stats["last_run"]
            if stats["last_result"] is not None:
                agg["last_result"] = stats["last_result"]
            if stats["last_error"]:
                agg["last_error"] = stats["last_error"]
        agg["avg_duration"] = agg["total_duration"] / agg["runs"] if agg["runs"] else 0.0
        return agg

    def get_job_stats(self, cron_str: str) -> Dict[str, Any]:
        """Return aggregated execution statistics for a cron schedule.

        Raises:
            KeyError: If the schedule does not exist.
        """
        with self._lock:
            unit = self._units.get(cron_str)
            if unit is None:
                raise KeyError(cron_str)
            stats = self._aggregate_job_stats(cron_str)
            stats["enabled"] = unit.enabled
            stats["running_now"] = sum(len(starts) for starts in unit.running_started.values())
            nxt = unit.cron.next()
            stats["next_run"] = nxt.isoformat() if nxt else None
            return stats

    def get_stats(self) -> Dict[str, Any]:
        """Return a full snapshot of global and per-schedule statistics.

        Suitable for feeding into monitoring dashboards.
        """
        with self._lock:
            stats = dict(self._stats)
            stats["started_at"] = self._start_wall.isoformat() if self._start_wall else None
            stats["uptime"] = self.uptime()
            stats["running"] = self.is_running()
            stats["paused"] = self._paused
            stats["jobs"] = {}
            for cron_str, unit in self._units.items():
                job_stats = self._aggregate_job_stats(cron_str)
                job_stats["enabled"] = unit.enabled
                job_stats["running_now"] = sum(len(starts) for starts in unit.running_started.values())
                nxt = unit.cron.next()
                job_stats["next_run"] = nxt.isoformat() if nxt else None
                stats["jobs"][cron_str] = job_stats
            return stats

    def get_history(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Return recent execution history records.

        Args:
            limit: Maximum number of records to return (most recent first). If
                ``None`` (default) all retained records are returned.
        """
        with self._lock:
            entries = list(self._history)
        if limit is not None and limit >= 0:
            entries = entries[-limit:]
        return entries

    def get_runned_today(self) -> List[Dict[str, Any]]:
        """Return history records for the current calendar day."""
        today = datetime.now().strftime("%Y-%m-%d")
        with self._lock:
            return [
                entry for entry in self._history
                if str(entry.get("started_at", "")).startswith(today)
            ]

    def get_running_jobs(self) -> List[Dict[str, Any]]:
        """Return the cron schedules (with instance counts) currently running."""
        with self._lock:
            running = []
            for cron_str, unit in self._units.items():
                for (_, index), starts in unit.running_started.items():
                    if starts:
                        running.append({"cron": cron_str, "index": index, "running_instances": len(starts)})
            return running

    # ── logging helper ───────────────────────────────────────────────

    def _log(self, message: str, is_error: bool = False, level: str = "info") -> None:
        """Conditionally print and/or remotely log a scheduler message."""
        if self.verbose:
            print(message)
        if not self.logger_client:
            return
        if level == "warning":
            self.logger_client.warning(message)
        elif is_error:
            self.logger_client.error(message)
        else:
            self.logger_client.info(message)

    # ── guard helpers ────────────────────────────────────────────────

    def _is_skipped(self, now: datetime) -> bool:
        """Return ``True`` if the current day/date is in the skip sets."""
        day_name = now.strftime('%A').lower()
        formatted_ddmmyyyy = now.strftime('%d-%m-%Y')
        return day_name in self.skip_days or formatted_ddmmyyyy in self.skip_dates

    def _is_within_window(self, now: datetime) -> bool:
        """Check the current time against the configured operation window."""
        if not self.operation_window:
            return True
        try:
            start_str, end_str = self.operation_window
            current_time_str = now.strftime('%H:%M:%S')
            if start_str <= end_str:
                return start_str <= current_time_str <= end_str
            return current_time_str >= start_str or current_time_str <= end_str
        except Exception as exc:
            self._log(f"[Scheduler Window Error] Bad operation_window configuration: {exc}", is_error=True)
            return False

    # ── dispatch ─────────────────────────────────────────────────────

    def _dispatch_unit(self, unit: _CronUnit) -> None:
        """Spawn runner threads for each job in a unit, honouring overlap caps."""
        if not unit.enabled:
            return

        with self._lock:
            jobs = list(unit.jobs)

        for index, job in enumerate(jobs):
            jid = id(job)
            cap = self.max_concurrent
            if cap is None and not self.allow_overlap:
                cap = 1

            with self._lock:
                running = unit.running_count.get(jid, 0)
                if cap is not None and running >= cap:
                    self._stats["overlap_skipped"] += 1
                    job_stats = self._job_stats.setdefault((unit.cron_str, index), self._new_job_stats())
                    job_stats["overlap_skipped"] += 1
                    self._log(
                        f"[Scheduler Overlap] Skipped '{unit.cron_str}' (job #{index}): "
                        f"{running} instance(s) already running",
                        is_error=True, level="warning",
                    )
                    continue
                unit.running_count[jid] = running + 1
                start = time.monotonic()
                self._instance_seq += 1
                token = self._instance_seq
                unit.running_started.setdefault((jid, index), {})[token] = [start, False]

            runner = threading.Thread(
                target=self._job_runner, args=(unit, job, index, token), daemon=True
            )
            runner.start()

    def _resolved_job_index(self, unit: _CronUnit, job: Job) -> int:
        """Return the job's current index within the unit, or -1 if removed."""
        try:
            return unit.jobs.index(job)
        except ValueError:
            return -1

    def _job_runner(self, unit: _CronUnit, job: Job, index: int, token: int) -> None:
        """Execute a single job with retry handling, timing and stats recording."""
        jid = id(job)
        start = time.monotonic()
        result: Optional[JResponse] = None
        attempts = 0
        local_runs = 0
        local_retries = 0

        try:
            while True:
                attempts += 1
                local_runs += 1
                with self._lock:
                    self._stats["runs"] += 1

                result = job.execute()

                if result.is_error and self.retry_on_failure and attempts <= self.max_retries:
                    with self._lock:
                        self._stats["retries"] += 1
                        local_retries += 1
                    self._log(
                        f"[Scheduler Retry] '{unit.cron_str}' (job #{index}) attempt "
                        f"{attempts}/{self.max_retries} failed: {result.error}",
                        is_error=True, level="warning",
                    )
                    time.sleep(self.retry_delay)
                    continue
                break
        except Exception as exc:
            result = JResponse(None, True, exc)
        finally:
            duration = time.monotonic() - start
            with self._lock:
                stats_index = self._resolved_job_index(unit, job)
                job_stats = self._job_stats.setdefault((unit.cron_str, stats_index), self._new_job_stats())
                job_stats["runs"] += local_runs
                job_stats["retries"] += local_retries
                job_stats["last_duration"] = duration
                job_stats["total_duration"] += duration
                job_stats["last_run"] = time.monotonic()

                if result is not None and result.is_error:
                    self._stats["failures"] += 1
                    job_stats["failures"] += 1
                    job_stats["consecutive_failures"] += 1
                    job_stats["last_result"] = False
                    job_stats["last_error"] = repr(result.error)
                    self._log(
                        f"[Scheduler Job Error] '{unit.cron_str}' (job #{stats_index}) failed: {result.error}",
                        is_error=True,
                    )
                else:
                    self._stats["successes"] += 1
                    job_stats["successes"] += 1
                    job_stats["consecutive_failures"] = 0
                    job_stats["last_result"] = True
                    job_stats["last_error"] = None

                if jid in unit.running_count:
                    unit.running_count[jid] = max(unit.running_count[jid] - 1, 0)

                starts = unit.running_started.get((jid, stats_index)) if stats_index >= 0 else None
                if starts is not None and token in starts:
                    starts.pop(token, None)
                    if not starts:
                        unit.running_started.pop((jid, stats_index), None)
                else:
                    for (jid_key, idx_key), starts_map in list(unit.running_started.items()):
                        if jid_key == jid and token in starts_map:
                            starts_map.pop(token, None)
                            if not starts_map:
                                unit.running_started.pop((jid_key, idx_key), None)
                            break

                if self.history_max:
                    self._history.append({
                        "cron": unit.cron_str,
                        "index": stats_index,
                        "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "duration": round(duration, 4),
                        "is_error": bool(result is not None and result.is_error),
                        "error": repr(result.error) if result is not None and result.is_error else None,
                    })

    # ── watchdog and catch-up ────────────────────────────────────────

    def _watchdog(self, now_monotonic: Optional[float] = None) -> None:
        """Flag any running job that has exceeded ``job_timeout`` seconds."""
        if self.job_timeout is None:
            return
        now_monotonic = now_monotonic if now_monotonic is not None else time.monotonic()

        with self._lock:
            units = list(self._units.items())

        for cron_str, unit in units:
            for (_, index), starts in list(unit.running_started.items()):
                for token, (start, reported) in list(starts.items()):
                    if not reported and now_monotonic - start > self.job_timeout:
                        starts[token][1] = True
                        with self._lock:
                            self._stats["timeouts"] += 1
                            self._job_stats.setdefault((cron_str, index), self._new_job_stats())["timeouts"] += 1
                        self._log(
                            f"[Scheduler Timeout] '{cron_str}' (job #{index}) exceeded "
                            f"{self.job_timeout}s and is flagged as timed out",
                            is_error=True,
                        )

    def _maybe_catch_up(self, unit: _CronUnit, now: datetime) -> None:
        """Fire once for the most recent cron tick missed while stalled."""
        if unit.startup:
            return
        with self._lock:
            last_str = unit.last_fired
            if last_str is None:
                return

            fmt = "%Y-%m-%d %H:%M:%S" if unit.cron.has_seconds else "%Y-%m-%d %H:%M"
            try:
                last = datetime.strptime(last_str, fmt)
            except ValueError:
                return

            step = timedelta(seconds=1) if unit.cron.has_seconds else timedelta(minutes=1)
            bound = now - timedelta(seconds=self.catch_up_window)
            cursor = last + step
            while cursor < now:
                if cursor >= bound and unit.cron.matches(cursor):
                    matched = cursor.strftime(fmt)
                    unit.last_fired = matched
                    self._stats["catch_up_fired"] += 1
                    self._log(
                        f"[Scheduler Catch-Up] Running missed '{unit.cron_str}' for {matched}",
                        level="warning",
                    )
                    self._dispatch_unit(unit)
                    return
                cursor += step

    # ── main loop ────────────────────────────────────────────────────

    def _scheduler_loop(self) -> None:
        """Main background loop: evaluate cron matches, then sleep.

        On each iteration:

        1. Skip if the current day/date is in the skip sets.
        2. Skip if outside ``operation_window``.
        3. For every enabled cron unit, fire it once if the current minute
           matches, or (optionally) catch up a single missed minute.
        4. Run the timeout watchdog.
        5. Any unexpected exception is logged and the loop continues.
        """
        while not self._stop_event.is_set():
            try:
                now = datetime.now()

                if self._is_skipped(now):
                    self._stop_event.wait(self.check_freq)
                    continue
                if not self._is_within_window(now):
                    self._stop_event.wait(self.check_freq)
                    continue

                with self._lock:
                    units = list(self._units.items())
                    paused = self._paused

                for cron_str, unit in units:
                    if paused or not unit.enabled or unit.startup:
                        continue

                    if unit.cron.matches(now):
                        key = now.strftime(
                            "%Y-%m-%d %H:%M:%S" if unit.cron.has_seconds else "%Y-%m-%d %H:%M"
                        )
                        with self._lock:
                            if unit.last_fired == key:
                                continue
                            unit.last_fired = key
                        self._dispatch_unit(unit)
                    elif self.catch_up:
                        self._maybe_catch_up(unit, now)

                if self.job_timeout is not None:
                    self._watchdog()

            except Exception as loop_error:
                self._log(
                    f"[Scheduler Loop Error] Unexpected anomaly in cron scheduler loop: {loop_error}",
                    is_error=True,
                )

            self._stop_event.wait(self.check_freq)
