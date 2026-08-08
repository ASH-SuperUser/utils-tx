import socket
import struct
import json
import threading
import time
from unittest.mock import ANY, patch, MagicMock, call

import pytest

from utils_tx.logger import (
    LoggerClient, LoggerServer,
    LOG_DEBUG, LOG_INFO, LOG_SUCCESS, LOG_WARNING, LOG_ERROR,
    STR_TO_LOG_TYPE
)


# ── Helpers ──────────────────────────────────────────────────────────

def _find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))
        return s.getsockname()[1]


# ── LoggerClient ─────────────────────────────────────────────────────

class TestLoggerClientInit:

    def test_valid_address(self):
        client = LoggerClient("localhost:5050", "test")
        assert client.server_host == "localhost"
        assert client.server_port == 5050
        assert client.name == "test"

    def test_invalid_address_raises(self):
        with pytest.raises(ValueError, match="host:port"):
            LoggerClient("invalid-address", "test")

    def test_default_timeout(self):
        client = LoggerClient("localhost:5050", "test")
        assert client.timeout == 5.0

    def test_custom_timeout(self):
        client = LoggerClient("localhost:5050", "test", timeout=10.0)
        assert client.timeout == 10.0


class TestLoggerClientSendLog:

    @patch("utils_tx.logger.socket.create_connection")
    def test_send_log_success(self, mock_create):
        mock_sock = MagicMock()
        mock_create.return_value.__enter__.return_value = mock_sock

        client = LoggerClient("localhost:5050", "tester")
        client._send_log(LOG_INFO, "info", "hello world")

        mock_create.assert_called_once_with(("localhost", 5050), timeout=5.0)
        assert mock_sock.sendall.called
        call_args = mock_sock.sendall.call_args[0][0]
        # first 4 bytes: length prefix, rest: json payload
        payload_len = struct.unpack('!I', call_args[:4])[0]
        payload = json.loads(call_args[4:4 + payload_len])
        assert payload["type"] == LOG_INFO
        assert "hello world" in payload["message"]
        assert "tester" in payload["message"]

    @patch("utils_tx.logger.socket.create_connection")
    def test_send_log_with_custom_name(self, mock_create):
        mock_sock = MagicMock()
        mock_create.return_value.__enter__.return_value = mock_sock

        client = LoggerClient("localhost:5050", "tester")
        client._send_log(LOG_DEBUG, "debug", "msg", custom_name="sub")

        call_args = mock_sock.sendall.call_args[0][0]
        payload_len = struct.unpack('!I', call_args[:4])[0]
        payload = json.loads(call_args[4:4 + payload_len])
        assert "[tester] : [sub]" in payload["message"]

    @patch("utils_tx.logger.socket.create_connection")
    def test_connection_refused_no_raise(self, mock_create):
        mock_create.side_effect = ConnectionRefusedError("refused")
        client = LoggerClient("localhost:5050", "tester")
        client._send_log(LOG_INFO, "info", "msg")
        # should not raise

    @patch("utils_tx.logger.socket.create_connection")
    def test_timeout_no_raise(self, mock_create):
        mock_create.side_effect = socket.timeout("timeout")
        client = LoggerClient("localhost:5050", "tester")
        client._send_log(LOG_INFO, "info", "msg")
        # should not raise


class TestLoggerClientLogMethods:

    @patch.object(LoggerClient, "_send_log")
    def test_debug(self, mock_send):
        LoggerClient("localhost:5050", "t").debug("d")
        mock_send.assert_called_once_with(LOG_DEBUG, "debug", "d", None)

    @patch.object(LoggerClient, "_send_log")
    def test_info(self, mock_send):
        LoggerClient("localhost:5050", "t").info("i")
        mock_send.assert_called_once_with(LOG_INFO, "info", "i", None)

    @patch.object(LoggerClient, "_send_log")
    def test_success(self, mock_send):
        LoggerClient("localhost:5050", "t").success("s")
        mock_send.assert_called_once_with(LOG_SUCCESS, "success", "s", None)

    @patch.object(LoggerClient, "_send_log")
    def test_warning(self, mock_send):
        LoggerClient("localhost:5050", "t").warning("w")
        mock_send.assert_called_once_with(LOG_WARNING, "warning", "w", None)

    @patch.object(LoggerClient, "_send_log")
    def test_error(self, mock_send):
        LoggerClient("localhost:5050", "t").error("e")
        mock_send.assert_called_once_with(LOG_ERROR, "error", "e", None)

    @patch.object(LoggerClient, "_send_log")
    def test_methods_with_custom_name(self, mock_send):
        LoggerClient("localhost:5050", "t").info("m", name="custom")
        mock_send.assert_called_once_with(LOG_INFO, "info", "m", "custom")


# ── LoggerServer ─────────────────────────────────────────────────────

class TestLoggerServerInit:

    def test_defaults(self):
        server = LoggerServer()
        assert server.host == 'localhost'
        assert server.port == 5050
        assert server.color_print is True
        assert len(server.configs) == 5
        assert server.mem_logs == {}

    def test_with_mem_logs(self):
        server = LoggerServer(debug_max_in_mem=10, info_max_in_mem=5)
        assert LOG_DEBUG in server.mem_logs
        assert server.mem_logs[LOG_DEBUG].maxlen == 10
        assert server.mem_logs[LOG_INFO].maxlen == 5
        assert LOG_SUCCESS not in server.mem_logs

    def test_prints_disabled(self):
        server = LoggerServer(print_debug=False, print_info=False)
        assert server.configs[LOG_DEBUG]["print"] is False
        assert server.configs[LOG_INFO]["print"] is False
        assert server.configs[LOG_SUCCESS]["print"] is True


class TestLoggerServerLifecycle:

    @pytest.fixture
    def server(self):
        port = _find_free_port()
        srv = LoggerServer(port=port, color_print=False)
        yield srv
        srv.stop_server()

    def test_start_and_stop(self, server):
        server.start_server()
        assert server._server_thread is not None
        assert server._server_thread.is_alive()
        server.stop_server()
        assert server._server_thread is None or not server._server_thread.is_alive()
        assert server.server_sock is None

    def test_start_twice(self, server):
        server.start_server()
        server.start_server()
        server.stop_server()

    def test_stop_when_not_started(self, server):
        server.stop_server()
        assert server.server_sock is None

    def test_server_receives_log(self, server):
        server.start_server()
        time.sleep(0.2)

        client = LoggerClient(f"localhost:{server.port}", "ut")
        client.info("integration test")

        time.sleep(0.3)
        server.stop_server()

    def test_get_logs_empty(self, server):
        # no mem logs configured
        assert server.get_logs() == [[], [], [], [], []]

    def test_get_logs_with_mem_storage(self, server):
        server.mem_logs[LOG_INFO] = []
        server.mem_logs[LOG_INFO].append("test message")
        logs = server.get_logs(LOG_INFO)
        assert logs == ["test message"]

    def test_get_logs_by_string(self, server):
        server.mem_logs[LOG_INFO] = []
        server.mem_logs[LOG_INFO].append("msg")
        logs = server.get_logs("info")
        assert logs == ["msg"]

    def test_get_logs_invalid_type(self, server):
        assert server.get_logs(999) == []

    def test_get_logs_invalid_string(self, server):
        assert server.get_logs("unknown") == []


class TestLoggerServerHandleClient:

    def _send_payload(self, sock, log_type, message):
        payload = json.dumps({"type": log_type, "message": message}).encode("utf-8")
        sock.sendall(struct.pack('!I', len(payload)) + payload)

    def test_handle_client_valid(self):
        server = LoggerServer(color_print=False)

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s1, \
             socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s2:
            s1.bind(('127.0.0.1', 0))
            s1.listen(1)
            s1.settimeout(2)
            host, port = s1.getsockname()
            s2.connect((host, port))
            client_sock, _ = s1.accept()

            self._send_payload(s2, LOG_INFO, "test message")

            server._handle_client(client_sock)

    def test_handle_client_incomplete_header(self):
        server = LoggerServer()
        mock_sock = MagicMock()
        mock_sock.recv.side_effect = [b""]
        server._handle_client(mock_sock)
        mock_sock.close.assert_called_once()

    def test_handle_client_incomplete_payload(self):
        server = LoggerServer()
        port = _find_free_port()

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s1, \
             socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s2:
            s1.bind(('127.0.0.1', 0))
            s1.listen(1)
            s1.settimeout(2)
            host, port = s1.getsockname()
            s2.connect((host, port))
            client_sock, _ = s1.accept()

            payload = json.dumps({"type": LOG_INFO, "message": "hi"}).encode("utf-8")
            header = struct.pack('!I', len(payload))
            client_sock.sendall(header)
            client_sock.close()

            server._handle_client(client_sock)


class TestLoggerServerRecvExact:

    def test_recv_exact_success(self):
        server = LoggerServer()
        mock_sock = MagicMock()
        mock_sock.recv.side_effect = [b"hel", b"lo"]
        result = server._recv_exact(mock_sock, 5)
        assert result == b"hello"

    def test_recv_exact_returns_none(self):
        server = LoggerServer()
        mock_sock = MagicMock()
        mock_sock.recv.return_value = b""
        assert server._recv_exact(mock_sock, 5) is None


class TestLoggerServerAppendToFile:

    def test_append_to_file(self, tmp_path):
        server = LoggerServer()
        log_file = str(tmp_path / "test.log")
        server._append_to_file(log_file, "line1")
        server._append_to_file(log_file, "line2")
        content = tmp_path.joinpath("test.log").read_text(encoding="utf-8")
        assert "line1\nline2\n" in content

    def test_append_to_file_creates_dir(self, tmp_path):
        server = LoggerServer()
        nested = str(tmp_path / "sub" / "nested.log")
        server._append_to_file(nested, "hello")
        assert tmp_path.joinpath("sub", "nested.log").exists()


class TestLoggerServerIntegration:

    @pytest.fixture
    def server(self):
        port = _find_free_port()
        srv = LoggerServer(
            port=port,
            color_print=False,
            info_max_in_mem=50,
            error_max_in_mem=50,
            print_info=False,
            print_error=False,
        )
        srv.start_server()
        time.sleep(0.2)
        yield srv
        srv.stop_server()

    def test_client_to_server_roundtrip(self, server):
        client = LoggerClient(f"localhost:{server.port}", "integration")
        client.info("info msg")
        client.error("error msg")

        time.sleep(0.3)

        # Check logs from server memory
        info_logs = server.get_logs(LOG_INFO)
        error_logs = server.get_logs(LOG_ERROR)

        assert any("info msg" in log for log in info_logs)
        assert any("error msg" in log for log in error_logs)

    def test_all_log_levels(self, server):
        client = LoggerClient(f"localhost:{server.port}", "levels")

        server.mem_logs[LOG_DEBUG] = []
        server.mem_logs[LOG_INFO] = []
        server.mem_logs[LOG_SUCCESS] = []
        server.mem_logs[LOG_WARNING] = []
        server.mem_logs[LOG_ERROR] = []

        client.debug("dbg")
        client.info("inf")
        client.success("scs")
        client.warning("wrn")
        client.error("err")

        time.sleep(0.5)

        all_logs = server.get_logs()
        # order: debug, info, success, warning, error
        assert len(all_logs) == 5
        assert any("dbg" in l for l in all_logs[0])
        assert any("inf" in l for l in all_logs[1])
        assert any("scs" in l for l in all_logs[2])
        assert any("wrn" in l for l in all_logs[3])
        assert any("err" in l for l in all_logs[4])


# ── Regression tests for reported bugs ───────────────────────────────

class TestStringLogTypeHandling:

    def _send_payload(self, sock, log_type, message):
        payload = json.dumps({"type": log_type, "message": message}).encode("utf-8")
        sock.sendall(struct.pack('!I', len(payload)) + payload)

    def _roundtrip(self, server, log_type, message):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s1, \
             socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s2:
            s1.bind(('127.0.0.1', 0))
            s1.listen(1)
            s1.settimeout(2)
            host, port = s1.getsockname()
            s2.connect((host, port))
            client_sock, _ = s1.accept()
            self._send_payload(s2, log_type, message)
            server._handle_client(client_sock)

    def test_string_log_type_is_resolved_server_side(self):
        server = LoggerServer(color_print=False, info_max_in_mem=10, print_info=False)
        self._roundtrip(server, "info", "string-typed info message")
        logs = server.get_logs(LOG_INFO)
        assert any("string-typed info message" in log for log in logs)

    def test_string_log_type_case_insensitive(self):
        server = LoggerServer(color_print=False, error_max_in_mem=10, print_error=False)
        self._roundtrip(server, "ERROR", "uppercase error message")
        logs = server.get_logs(LOG_ERROR)
        assert any("uppercase error message" in log for log in logs)


class TestFileLockLifecycle:

    def test_file_locks_pruned_after_config_change(self, tmp_path):
        server = LoggerServer()
        old = str(tmp_path / "old.log")
        new = str(tmp_path / "new.log")
        server._append_to_file(old, "line")
        assert old in server.file_locks

        server.configs[LOG_INFO]["file"] = new
        server._append_to_file(new, "line2")

        assert new in server.file_locks
        assert old not in server.file_locks

    def test_file_locks_bounded_by_active_config(self, tmp_path):
        server = LoggerServer(info_log_file=str(tmp_path / "info.log"))
        server._append_to_file(str(tmp_path / "info.log"), "x")
        for i in range(20):
            server._append_to_file(str(tmp_path / f"transient{i}.log"), "x")
            server._get_file_lock(str(tmp_path / "info.log"))
        # only the configured file's lock should survive pruning
        assert set(server.file_locks) == {str(tmp_path / "info.log")}


class TestServerStopDeadlock:

    def test_stop_server_from_worker_does_not_deadlock(self):
        port = _find_free_port()
        server = LoggerServer(port=port, color_print=False)
        server.start_server()
        time.sleep(0.2)

        result = []

        def stop_from_worker():
            server.stop_server()
            result.append(True)

        fut = server.executor.submit(stop_from_worker)
        fut.result(timeout=5)
        assert result == [True]
