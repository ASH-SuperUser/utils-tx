import threading
import time
from datetime import datetime, timedelta
from unittest.mock import MagicMock

import pytest

from utils_tx.scheduler import JResponse, Job, CronExpression, Scheduler


def _wait_until(condition, timeout=5.0, interval=0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(interval)
    return condition()


# ── CronExpression ───────────────────────────────────────────────────

class TestCronExpressionParse:

    def test_every_minute(self):
        cron = CronExpression("* * * * *")
        assert cron.minutes == set(range(0, 60))
        assert cron.hours == set(range(0, 24))
        assert cron.dom == set(range(1, 32))
        assert cron.months == set(range(1, 13))
        assert cron.dow == set(range(0, 7))

    def test_specific_values(self):
        cron = CronExpression("30 9 * * 1-5")
        assert cron.minutes == {30}
        assert cron.hours == {9}
        assert cron.dow == {1, 2, 3, 4, 5}

    def test_steps(self):
        cron = CronExpression("*/15 * * * *")
        assert cron.minutes == {0, 15, 30, 45}

    def test_range_with_step(self):
        cron = CronExpression("0-30/10 * * * *")
        assert cron.minutes == {0, 10, 20, 30}

    def test_lists(self):
        cron = CronExpression("1,2,3 * * * *")
        assert cron.minutes == {1, 2, 3}

    def test_names(self):
        cron = CronExpression("0 12 * * MON-FRI")
        assert cron.dow == {1, 2, 3, 4, 5}
        cron2 = CronExpression("0 12 * JAN,MAY *")
        assert cron2.months == {1, 5}

    def test_sunday_seven_normalised(self):
        cron = CronExpression("0 0 * * 7")
        assert cron.dow == {0}

    def test_alias(self):
        cron = CronExpression("@daily")
        assert cron.minutes == {0}
        assert cron.hours == {0}
        assert cron.canonical == "0 0 * * *"

    def test_invalid_field_count(self):
        with pytest.raises(ValueError):
            CronExpression("* * * *")

    def test_invalid_value(self):
        with pytest.raises(ValueError):
            CronExpression("61 * * * *")

    def test_invalid_name(self):
        with pytest.raises(ValueError):
            CronExpression("0 0 * * xyz")

    def test_empty(self):
        with pytest.raises(ValueError):
            CronExpression("")


class TestCronExpressionSeconds:

    def test_six_field_has_seconds(self):
        cron = CronExpression("*/10 * * * * *")
        assert cron.has_seconds is True
        assert cron.seconds == {0, 10, 20, 30, 40, 50}
        assert cron.minutes == set(range(0, 60))

    def test_five_field_no_seconds(self):
        cron = CronExpression("* * * * *")
        assert cron.has_seconds is False
        assert cron.seconds == set(range(0, 60))

    def test_matches_seconds(self):
        cron = CronExpression("*/10 * * * * *")
        assert cron.matches(datetime(2026, 8, 3, 9, 30, 10)) is True
        assert cron.matches(datetime(2026, 8, 3, 9, 30, 11)) is False
        assert cron.matches(datetime(2026, 8, 3, 9, 30, 50)) is True

    def test_matches_seconds_with_minute_constraint(self):
        cron = CronExpression("30 0 9 * * *")  # 09:00:30
        assert cron.matches(datetime(2026, 8, 3, 9, 0, 30)) is True
        assert cron.matches(datetime(2026, 8, 3, 9, 1, 30)) is False

    def test_next_seconds(self):
        cron = CronExpression("*/10 * * * * *")
        nxt = cron.next(datetime(2026, 8, 3, 9, 30, 35))
        assert nxt == datetime(2026, 8, 3, 9, 30, 40)
        nxt2 = cron.next(datetime(2026, 8, 3, 9, 30, 0))
        assert nxt2 == datetime(2026, 8, 3, 9, 30, 10)

    def test_next_seconds_rolls_into_next_minute(self):
        cron = CronExpression("0 * * * * *")
        nxt = cron.next(datetime(2026, 8, 3, 9, 30, 1))
        assert nxt == datetime(2026, 8, 3, 9, 31, 0)

    def test_invalid_six_field_count(self):
        with pytest.raises(ValueError):
            CronExpression("* * * * * * *")


class TestCronExpressionMatch:

    def test_matches_simple(self):
        cron = CronExpression("30 9 * * *")
        assert cron.matches(datetime(2026, 8, 2, 9, 30)) is True
        assert cron.matches(datetime(2026, 8, 2, 9, 31)) is False
        assert cron.matches(datetime(2026, 8, 2, 10, 30)) is False

    def test_matches_dow(self):
        cron = CronExpression("0 0 * * mon")
        # 2026-08-03 is a Monday
        assert cron.matches(datetime(2026, 8, 3)) is True
        # 2026-08-02 is a Sunday
        assert cron.matches(datetime(2026, 8, 2)) is False

    def test_matches_dom(self):
        cron = CronExpression("0 0 1 * *")
        assert cron.matches(datetime(2026, 8, 1)) is True
        assert cron.matches(datetime(2026, 8, 2)) is False

    def test_dom_and_dow_or_semantics(self):
        # both restricted -> OR
        cron = CronExpression("0 0 1 * mon")
        assert cron.matches(datetime(2026, 8, 1)) is True       # 1st of month (Sat)
        assert cron.matches(datetime(2026, 8, 3)) is True       # Monday
        assert cron.matches(datetime(2026, 8, 2)) is False      # Sunday, not 1st

    def test_next_run(self):
        cron = CronExpression("0 9 * * *")
        nxt = cron.next(datetime(2026, 8, 2, 8, 0))
        assert nxt == datetime(2026, 8, 2, 9, 0)

    def test_next_run_wraps_to_next_day(self):
        cron = CronExpression("0 9 * * *")
        nxt = cron.next(datetime(2026, 8, 2, 10, 0))
        assert nxt == datetime(2026, 8, 3, 9, 0)

    def test_next_run_every_minute(self):
        cron = CronExpression("* * * * *")
        nxt = cron.next(datetime(2026, 8, 2, 9, 30, 15))
        assert nxt == datetime(2026, 8, 2, 9, 31, 0)


# ── Job.execute ──────────────────────────────────────────────────────

class TestJobExecute:

    def test_execute_success(self):
        job = Job(lambda a, b: a + b, 2, 3)
        resp = job.execute()
        assert resp.data == 5
        assert resp.is_error is False

    def test_execute_error(self):
        def fail():
            raise RuntimeError("boom")
        job = Job(fail)
        resp = job.execute()
        assert resp.is_error is True
        assert isinstance(resp.error, RuntimeError)


# ── Scheduler ────────────────────────────────────────────────────────

class TestSchedulerInit:

    def test_defaults(self):
        sched = Scheduler({})
        assert sched.check_freq == 1.0
        assert sched.allow_overlap is False
        assert sched.retry_on_failure is False
        assert sched.job_timeout is None
        assert sched.catch_up is False
        assert 'saturday' in sched.skip_days
        assert sched._stats["runs"] == 0

    def test_invalid_cron_raises(self):
        with pytest.raises(ValueError):
            Scheduler({"bad cron": Job(lambda: None)})

    def test_invalid_job_type_raises(self):
        with pytest.raises(TypeError):
            Scheduler({"* * * * *": "not a job"})

    def test_job_value_can_be_list(self):
        sched = Scheduler({"* * * * *": [Job(lambda: None), Job(lambda: None)]})
        assert len(sched.get_jobs()["* * * * *"]) == 2

    def test_invalid_options(self):
        with pytest.raises(ValueError):
            Scheduler({}, check_freq=0)
        with pytest.raises(ValueError):
            Scheduler({}, max_retries=-1)
        with pytest.raises(ValueError):
            Scheduler({}, job_timeout=-5)

    def test_init_tracks_job_stats_key(self):
        sched = Scheduler({})
        assert sched.get_stats()["jobs"] == {}


class TestSchedulerLifecycle:

    def test_start_stop(self):
        sched = Scheduler({}, check_freq=0.5)
        sched.start_scheduler()
        assert sched.is_running() is True
        sched.stop_scheduler()
        assert sched.is_running() is False

    def test_start_twice_noop(self):
        sched = Scheduler({}, check_freq=0.5)
        sched.start_scheduler()
        thread_id = id(sched._scheduler_thread)
        sched.start_scheduler()
        assert id(sched._scheduler_thread) == thread_id
        sched.stop_scheduler()

    def test_uptime_before_start(self):
        sched = Scheduler({})
        assert sched.uptime() == 0
        assert sched.get_start_time() is None

    def test_uptime_after_start(self):
        sched = Scheduler({}, check_freq=0.5)
        sched.start_scheduler()
        time.sleep(1.2)
        assert sched.uptime() >= 1
        assert sched.get_start_time() is not None
        sched.stop_scheduler()

    def test_pause_resume(self):
        sched = Scheduler({})
        sched.pause()
        assert sched.is_paused() is True
        sched.resume()
        assert sched.is_paused() is False

    def test_runned_today_empty(self):
        sched = Scheduler({})
        assert sched.get_runned_today() == []
        assert sched.get_history() == []


class TestSchedulerDispatch:

    def test_dispatch_executes_job(self):
        results = []
        sched = Scheduler({})
        sched.add_job("* * * * *", Job(lambda: results.append("x")))
        unit = sched._units["* * * * *"]
        sched._dispatch_unit(unit)
        assert _wait_until(lambda: len(results) == 1)
        assert sched.get_stats()["runs"] == 1
        assert sched.get_stats()["successes"] == 1
        history = sched.get_history()
        assert len(history) == 1
        assert history[0]["is_error"] is False

    def test_dispatch_records_failure(self):
        def fail():
            raise ValueError("nope")
        sched = Scheduler({})
        sched.add_job("* * * * *", Job(fail))
        unit = sched._units["* * * * *"]
        sched._dispatch_unit(unit)
        assert _wait_until(lambda: sched.get_stats()["failures"] == 1)
        assert sched.get_stats()["successes"] == 0
        assert sched.get_job_stats("* * * * *")["last_result"] is False

    def test_overlap_protection(self):
        block = threading.Event()
        released = []

        def slow():
            released.append(1)
            block.wait(timeout=5)

        sched = Scheduler({}, allow_overlap=False)
        sched.add_job("* * * * *", Job(slow))
        unit = sched._units["* * * * *"]
        sched._dispatch_unit(unit)
        assert _wait_until(lambda: len(released) == 1)
        # second dispatch while first still running -> skipped
        sched._dispatch_unit(unit)
        assert _wait_until(lambda: sched.get_stats()["overlap_skipped"] >= 1)
        assert sched.get_stats()["runs"] == 1
        block.set()
        assert sched.wait_for_all(timeout=5) is True

    def test_allow_overlap_runs_concurrently(self):
        block = threading.Event()
        started = []
        finished = []

        def slow():
            started.append(1)
            block.wait(timeout=5)
            finished.append(1)

        sched = Scheduler({}, allow_overlap=True)
        sched.add_job("* * * * *", Job(slow))
        unit = sched._units["* * * * *"]
        sched._dispatch_unit(unit)
        assert _wait_until(lambda: len(started) == 1)
        sched._dispatch_unit(unit)
        assert _wait_until(lambda: len(started) == 2)
        running = sched.get_running_jobs()
        assert running and running[0]["running_instances"] == 2
        block.set()
        assert sched.wait_for_all(timeout=5) is True

    def test_retry_on_failure(self):
        calls = {"n": 0}

        def flaky():
            calls["n"] += 1
            if calls["n"] < 3:
                raise ValueError("flaky")

        sched = Scheduler(
            {},
            retry_on_failure=True,
            max_retries=5,
            retry_delay=0.01,
        )
        sched.add_job("* * * * *", Job(flaky))
        unit = sched._units["* * * * *"]
        sched._dispatch_unit(unit)
        assert _wait_until(lambda: sched.get_stats()["runs"] >= 3)
        assert sched.get_stats()["retries"] == 2
        assert sched.get_stats()["failures"] == 0
        assert sched.get_stats()["successes"] == 1

    def test_retry_disabled_by_default(self):
        calls = {"n": 0}

        def flaky():
            calls["n"] += 1
            raise ValueError("always fails")

        sched = Scheduler({})
        sched.add_job("* * * * *", Job(flaky))
        unit = sched._units["* * * * *"]
        sched._dispatch_unit(unit)
        assert _wait_until(lambda: sched.get_stats()["runs"] == 1)
        assert sched.get_stats()["retries"] == 0

    def test_timeout_watchdog(self):
        block = threading.Event()

        def slow():
            block.wait(timeout=10)

        sched = Scheduler({}, job_timeout=0.1)
        sched.add_job("* * * * *", Job(slow))
        unit = sched._units["* * * * *"]
        sched._dispatch_unit(unit)
        assert _wait_until(lambda: len(sched.get_running_jobs()) == 1)
        sched._watchdog(time.monotonic() + 1.0)
        assert sched.get_stats()["timeouts"] >= 1
        assert sched.get_job_stats("* * * * *")["timeouts"] >= 1
        block.set()
        assert sched.wait_for_all(timeout=5) is True

    def test_dispatch_respects_disabled(self):
        results = []
        sched = Scheduler({})
        sched.add_job("* * * * *", Job(lambda: results.append("x")))
        sched.disable_job("* * * * *")
        unit = sched._units["* * * * *"]
        sched._dispatch_unit(unit)
        time.sleep(0.3)
        assert results == []
        assert sched.get_stats()["runs"] == 0


class TestSchedulerRuntimeManagement:

    def test_add_job(self):
        sched = Scheduler({})
        sched.add_job("*/5 * * * *", Job(lambda: None))
        assert "*/5 * * * *" in sched.get_jobs()

    def test_add_duplicate_raises(self):
        sched = Scheduler({"* * * * *": Job(lambda: None)})
        with pytest.raises(ValueError):
            sched.add_job("* * * * *", Job(lambda: None))

    def test_append_job(self):
        sched = Scheduler({"* * * * *": Job(lambda: None)})
        sched.append_job("* * * * *", Job(lambda: None))
        assert len(sched.get_jobs()["* * * * *"]) == 2

    def test_remove_job(self):
        sched = Scheduler({"* * * * *": Job(lambda: None)})
        sched.remove_job("* * * * *")
        assert sched.get_jobs() == {}

    def test_remove_job_missing_raises(self):
        sched = Scheduler({})
        with pytest.raises(KeyError):
            sched.remove_job("* * * * *")

    def test_remove_job_index(self):
        job_a, job_b = Job(lambda: 1), Job(lambda: 2)
        sched = Scheduler({"* * * * *": [job_a, job_b]})
        sched.remove_job_index("* * * * *", 0)
        assert sched.get_jobs()["* * * * *"] == [job_b]


class TestRemoveJobIndexRunningStarted:

    def test_remove_job_index_shifts_running_started_keys(self):
        sched = Scheduler({}, allow_overlap=True)
        job_a = Job(lambda: None)
        job_b = Job(lambda: None)
        sched.add_job("* * * * *", [job_a, job_b])
        unit = sched._units["* * * * *"]

        with sched._lock:
            unit.running_started[(id(job_b), 1)] = {999: [time.monotonic(), False]}

        sched.remove_job_index("* * * * *", 0)

        with sched._lock:
            assert (id(job_b), 0) in unit.running_started
            assert (id(job_b), 1) not in unit.running_started

    def test_runner_cleans_shifted_key_after_removal(self):
        block = threading.Event()
        started = threading.Event()

        def b_fn():
            started.set()
            block.wait(timeout=5)

        sched = Scheduler({}, allow_overlap=True)
        job_a = Job(lambda: None)
        job_b = Job(b_fn)
        sched.add_job("* * * * *", [job_a, job_b])
        unit = sched._units["* * * * *"]

        sched._dispatch_unit(unit)
        assert started.wait(timeout=5), "job_b should have started"

        sched.remove_job_index("* * * * *", 0)

        block.set()
        assert sched.wait_for_all(timeout=5)

        with sched._lock:
            assert unit.running_started == {}, "no stale running_started entries should remain"

    def test_overlap_tracking_after_removal(self):
        block = threading.Event()
        started = threading.Event()

        def b_fn():
            started.set()
            block.wait(timeout=5)

        sched = Scheduler({}, allow_overlap=False)
        job_a = Job(lambda: None)
        job_b = Job(b_fn)
        sched.add_job("* * * * *", [job_a, job_b])
        unit = sched._units["* * * * *"]

        sched._dispatch_unit(unit)
        assert started.wait(timeout=5)
        sched.remove_job_index("* * * * *", 0)

        with sched._lock:
            assert unit.running_count[id(job_b)] == 1
            assert (id(job_b), 0) in unit.running_started

        block.set()
        assert sched.wait_for_all(timeout=5)

    def test_enable_disable(self):
        sched = Scheduler({"* * * * *": Job(lambda: None)})
        assert sched.is_job_enabled("* * * * *") is True
        sched.disable_job("* * * * *")
        assert sched.is_job_enabled("* * * * *") is False
        sched.enable_job("* * * * *")
        assert sched.is_job_enabled("* * * * *") is True

    def test_next_run(self):
        sched = Scheduler({"30 9 * * *": Job(lambda: None)})
        nxt = sched.next_run("30 9 * * *", datetime(2026, 8, 2, 8, 0))
        assert nxt == datetime(2026, 8, 2, 9, 30)


class TestSchedulerStats:

    def test_global_stats_structure(self):
        sched = Scheduler({})
        sched.start_scheduler()
        stats = sched.get_stats()
        sched.stop_scheduler()
        for key in ("runs", "successes", "failures", "retries", "timeouts",
                    "overlap_skipped", "catch_up_fired", "uptime", "running",
                    "paused", "started_at", "jobs"):
            assert key in stats

    def test_job_stats_aggregate(self):
        sched = Scheduler({})
        sched.add_job("* * * * *", [Job(lambda: 1), Job(lambda: 2)])
        unit = sched._units["* * * * *"]
        sched._dispatch_unit(unit)
        assert _wait_until(lambda: sched.get_stats()["runs"] == 2)
        job_stats = sched.get_job_stats("* * * * *")
        assert job_stats["runs"] == 2
        assert job_stats["successes"] == 2
        assert "next_run" in job_stats

    def test_logger_client_records_errors(self):
        mock_logger = MagicMock()
        def fail():
            raise ValueError("boom")
        sched = Scheduler({}, verbose=False, logger_client=mock_logger)
        sched.add_job("* * * * *", Job(fail))
        unit = sched._units["* * * * *"]
        sched._dispatch_unit(unit)
        assert _wait_until(lambda: sched.get_stats()["failures"] == 1)
        assert mock_logger.error.called


class TestSchedulerCatchUp:

    def test_catch_up_fires_single_missed_minute(self):
        sched = Scheduler({}, catch_up=True, catch_up_window=300)
        sched.add_job("* * * * *", Job(lambda: None))
        unit = sched._units["* * * * *"]
        unit.last_fired = (datetime.now() - timedelta(minutes=2)).strftime("%Y-%m-%d %H:%M")
        sched._maybe_catch_up(unit, datetime.now())
        assert sched.get_stats()["catch_up_fired"] == 1
        assert sched.get_stats()["runs"] == 1

    def test_no_catch_up_when_never_fired(self):
        sched = Scheduler({}, catch_up=True)
        sched.add_job("* * * * *", Job(lambda: None))
        unit = sched._units["* * * * *"]
        unit.last_fired = None
        sched._maybe_catch_up(unit, datetime.now())
        assert sched.get_stats()["catch_up_fired"] == 0

    def test_catch_up_disabled_by_default(self):
        assert Scheduler({}).catch_up is False


class TestSchedulerSecondPrecision:

    def test_every_second_fires_multiple_times(self):
        results = []
        sched = Scheduler(
            {"* * * * * *": Job(lambda: results.append(1))},
            check_freq=0.2,
            skip_days=None,
            verbose=False,
        )
        sched.start_scheduler()
        time.sleep(2.5)
        sched.stop_scheduler()
        sched.wait_for_all(timeout=3)
        assert len(results) >= 2
        assert sched.get_stats()["runs"] >= 2
        assert sched.next_run("* * * * * *") is not None

    def test_second_cron_last_fired_has_second_precision(self):
        sched = Scheduler({}, skip_days=None)
        sched.add_job("* * * * * *", Job(lambda: None))
        unit = sched._units["* * * * * *"]
        assert unit.cron.has_seconds is True
        sched._dispatch_unit(unit)
        assert _wait_until(lambda: sched.get_stats()["runs"] == 1)
        # manual tick path: simulate loop firing for this second
        now = datetime.now()
        key = now.strftime("%Y-%m-%d %H:%M:%S")
        with sched._lock:
            unit.last_fired = key
        assert unit.last_fired == key


class TestSchedulerStartup:

    def test_startup_runs_on_start(self):
        results = []
        sched = Scheduler(
            {"@startup": Job(lambda: results.append("boot"))},
            skip_days=None,
            verbose=False,
        )
        sched.start_scheduler()
        assert _wait_until(lambda: len(results) == 1)
        sched.stop_scheduler()
        assert sched.get_stats()["runs"] == 1
        assert results == ["boot"]

    def test_startup_reruns_on_restart(self):
        results = []
        sched = Scheduler(
            {"@startup": Job(lambda: results.append("boot"))},
            skip_days=None,
            verbose=False,
        )
        sched.start_scheduler()
        assert _wait_until(lambda: len(results) == 1)
        sched.stop_scheduler()
        sched.start_scheduler()
        assert _wait_until(lambda: len(results) == 2)
        sched.stop_scheduler()

    def test_startup_added_runtime_fires_immediately(self):
        results = []
        sched = Scheduler({}, skip_days=None, verbose=False)
        sched.start_scheduler()
        try:
            sched.add_job("@startup", Job(lambda: results.append("now")))
            assert _wait_until(lambda: len(results) == 1)
        finally:
            sched.stop_scheduler()

    def test_startup_not_dispatched_by_loop(self):
        results = []
        sched = Scheduler(
            {"@startup": Job(lambda: results.append("x"))},
            skip_days=None,
            verbose=False,
        )
        unit = sched._units["@startup"]
        sched._dispatch_unit(unit)
        assert _wait_until(lambda: sched.get_stats()["runs"] == 1)
        # loop should never fire it again
        sched._scheduler_loop_tick = None  # ensure no accidental attribute
        assert unit.startup is True

    def test_startup_appears_in_jobs(self):
        sched = Scheduler({"@startup": Job(lambda: None)})
        assert "@startup" in sched.get_jobs()
        assert sched.next_run("@startup") is None


class TestSchedulerSkipping:

    def test_skip_day(self):
        sched = Scheduler({})
        sunday = datetime(2026, 8, 2)  # Sunday
        monday = datetime(2026, 8, 3)
        assert sched._is_skipped(sunday) is True
        assert sched._is_skipped(monday) is False

    def test_window(self):
        sched = Scheduler({}, operation_window=("09:00:00", "17:00:00"))
        assert sched._is_within_window(datetime(2026, 8, 3, 12, 0)) is True
        assert sched._is_within_window(datetime(2026, 8, 3, 8, 0)) is False

    def test_window_midnight_crossing(self):
        sched = Scheduler({}, operation_window=("22:00:00", "04:00:00"))
        assert sched._is_within_window(datetime(2026, 8, 3, 23, 0)) is True
        assert sched._is_within_window(datetime(2026, 8, 3, 12, 0)) is False
