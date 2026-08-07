import time
from datetime import datetime
from unittest.mock import patch, MagicMock
import pytest

from utils_tx.scheduler import JResponse, Job, TimeBasedScheduler, FrequencyBasedScheduler


class TestJResponse:

    def test_success_response(self):
        resp = JResponse(42, False)
        assert resp.data == 42
        assert resp.is_error is False
        assert resp.error is None

    def test_error_response(self):
        exc = ValueError("bad")
        resp = JResponse(None, True, exc)
        assert resp.data is None
        assert resp.is_error is True
        assert resp.error is exc

    def test_str_repr(self):
        resp = JResponse("hello", False)
        expected = "JResponse(data=hello, is_error=False, error=None)"
        assert str(resp) == expected
        assert repr(resp) == expected


class TestJob:

    def test_run_success_threaded(self):
        def add(a, b):
            return a + b
        job = Job(add, 2, 3)
        resp = job.run()
        assert isinstance(resp, JResponse)
        assert resp.is_error is False
        result = job.wait_for_result(timeout=2)
        assert result.data == 5

    def test_run_success_sync(self):
        def add(a, b):
            return a + b
        job = Job(add, 2, 3)
        job.use_thread = False
        resp = job.run()
        assert resp.data == 5
        assert resp.is_error is False

    def test_run_exception(self):
        def fail():
            raise RuntimeError("fail")
        job = Job(fail)
        resp = job.run()
        assert resp.is_error is False
        assert resp.data is not None
        result = job.wait_for_result(timeout=2)
        assert result.is_error is True
        assert isinstance(result.error, RuntimeError)

    def test_run_exception_sync(self):
        def fail():
            raise RuntimeError("fail")
        job = Job(fail)
        job.use_thread = False
        resp = job.run()
        assert resp.is_error is True
        assert isinstance(resp.error, RuntimeError)

    def test_wait_for_result_no_thread(self):
        job = Job(lambda: 1)
        assert job.wait_for_result() is None

    def test_run_returns_thread_in_data(self):
        def fn():
            pass
        job = Job(fn)
        resp = job.run()
        assert resp.data is not None
        import threading
        assert isinstance(resp.data, threading.Thread)

    def test_kwargs_passed(self):
        def fn(a, b=10):
            return a + b
        job = Job(fn, 5, b=20)
        job.use_thread = False
        resp = job.run()
        assert resp.data == 25


class TestTimeBasedScheduler:

    def test_init_defaults(self):
        sched = TimeBasedScheduler({})
        assert sched.check_freq == 10
        assert 'saturday' in sched.skip_days
        assert 'sunday' in sched.skip_days
        assert sched.skip_dates == set()

    def test_init_custom_skip(self):
        sched = TimeBasedScheduler({}, skip_days=('monday',), skip_dates=('25-12-2025',))
        assert 'monday' in sched.skip_days
        assert 'saturday' not in sched.skip_days
        assert '25-12-2025' in sched.skip_dates

    def test_start_stop_scheduler(self):
        sched = TimeBasedScheduler({}, check_freq=0.5)
        sched.start_scheduler()
        assert sched._scheduler_thread is not None
        assert sched._scheduler_thread.is_alive()
        sched.stop_scheduler()
        assert not sched._scheduler_thread.is_alive()

    def test_start_twice_no_error(self):
        sched = TimeBasedScheduler({}, check_freq=0.5)
        sched.start_scheduler()
        thread_id = id(sched._scheduler_thread)
        sched.start_scheduler()
        assert id(sched._scheduler_thread) == thread_id
        sched.stop_scheduler()

    def test_get_runned_today_empty(self):
        sched = TimeBasedScheduler({})
        assert sched.get_runned_today() == []

    def test_verbose_true_prints(self, capsys):
        sched = TimeBasedScheduler({}, verbose=True)
        sched._log("test message")
        captured = capsys.readouterr()
        assert "test message" in captured.out

    def test_verbose_false_suppresses_prints(self, capsys):
        sched = TimeBasedScheduler({}, verbose=False)
        sched._log("test message")
        captured = capsys.readouterr()
        assert captured.out == ""

    def test_logger_client_receives_log(self):
        mock_logger = MagicMock()
        sched = TimeBasedScheduler({}, verbose=False, logger_client=mock_logger)
        sched._log("test message")
        mock_logger.info.assert_called_once_with("test message")

    def test_logger_client_not_called_when_none(self):
        mock_logger = MagicMock()
        sched = TimeBasedScheduler({}, verbose=False)
        sched._log("test message")
        mock_logger.info.assert_not_called()

    def test_get_runned_today_after_execution(self):
        results = []
        def track():
            results.append(1)

        job = Job(track)
        sched = TimeBasedScheduler(
            {"00:00:00": job},
            check_freq=0.3,
            skip_days=None,
            skip_dates=None
        )

        with patch.object(sched, '_scheduler_loop') as mock_loop:
            sched.start_scheduler()
            import threading
            sched._runned_today.append({
                "timestamp": "00:00:00",
                "executed_at": "00:00:01",
                "jobs": [job]
            })
            sched._executed_timestamps_today.add("00:00:00")
            runned = sched.get_runned_today()
            assert len(runned) == 1
            assert runned[0]["timestamp"] == "00:00:00"
            sched.stop_scheduler()


class TestFrequencyBasedScheduler:

    def test_init_defaults(self):
        sched = FrequencyBasedScheduler({})
        assert sched.check_freq == 1
        assert 'saturday' in sched.skip_days
        assert 'sunday' in sched.skip_days
        assert sched.operation_window is None

    def test_init_custom(self):
        sched = FrequencyBasedScheduler({60: None}, operation_window=("09:00:00", "17:00:00"))
        assert sched.operation_window == ("09:00:00", "17:00:00")
        assert 60 in sched._last_run_timestamps

    def test_start_stop_scheduler(self):
        sched = FrequencyBasedScheduler({}, check_freq=0.5)
        sched.start_scheduler()
        assert sched._scheduler_thread is not None
        assert sched._scheduler_thread.is_alive()
        sched.stop_scheduler()
        assert not sched._scheduler_thread.is_alive()

    def test_start_twice_no_error(self):
        sched = FrequencyBasedScheduler({}, check_freq=0.5)
        sched.start_scheduler()
        thread_id = id(sched._scheduler_thread)
        sched.start_scheduler()
        assert id(sched._scheduler_thread) == thread_id
        sched.stop_scheduler()

    def test_get_last_runned(self):
        sched = FrequencyBasedScheduler({30: None})
        with sched._lock:
            sched._last_run_timestamps[30] = 12345.0
        last = sched.get_last_runned()
        assert last[30] == 12345.0

    def test_is_within_operation_window_no_window(self):
        sched = FrequencyBasedScheduler({})
        assert sched._is_within_operation_window("12:00:00") is True

    def test_is_within_operation_window_normal(self):
        sched = FrequencyBasedScheduler({}, operation_window=("09:00:00", "17:00:00"))
        assert sched._is_within_operation_window("12:00:00") is True
        assert sched._is_within_operation_window("08:59:59") is False
        assert sched._is_within_operation_window("17:00:01") is False

    def test_is_within_operation_window_midnight(self):
        sched = FrequencyBasedScheduler({}, operation_window=("22:00:00", "04:00:00"))
        assert sched._is_within_operation_window("23:00:00") is True
        assert sched._is_within_operation_window("03:00:00") is True
        assert sched._is_within_operation_window("12:00:00") is False

    def test_is_within_operation_window_bad_config(self):
        sched = FrequencyBasedScheduler({})
        sched.operation_window = "not_a_tuple"
        assert sched._is_within_operation_window("12:00:00") is False

    def test_verbose_true_prints(self, capsys):
        sched = FrequencyBasedScheduler({}, verbose=True)
        sched._log("test message")
        captured = capsys.readouterr()
        assert "test message" in captured.out

    def test_verbose_false_suppresses_prints(self, capsys):
        sched = FrequencyBasedScheduler({}, verbose=False)
        sched._log("test message")
        captured = capsys.readouterr()
        assert captured.out == ""

    def test_logger_client_receives_log(self):
        mock_logger = MagicMock()
        sched = FrequencyBasedScheduler({}, verbose=False, logger_client=mock_logger)
        sched._log("test message")
        mock_logger.info.assert_called_once_with("test message")

    def test_logger_client_not_called_when_none(self):
        mock_logger = MagicMock()
        sched = FrequencyBasedScheduler({}, verbose=False)
        sched._log("test message")
        mock_logger.info.assert_not_called()

    def test_job_execution_on_interval(self):
        call_count = 0
        def sample():
            nonlocal call_count
            call_count += 1

        job = Job(sample)
        sched = FrequencyBasedScheduler(
            {1: job},
            check_freq=0.2,
            skip_days=None,
            skip_dates=None
        )
        sched.start_scheduler()
        time.sleep(2.5)
        sched.stop_scheduler()
        assert call_count >= 1
