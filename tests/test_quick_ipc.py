import time
import socket
import struct
import json
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from utils_tx.quick_ipc import qipc_server, qipc_client


def _find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))
        return s.getsockname()[1]


# ── Fixtures ─────────────────────────────────────────────────────────

@pytest.fixture
def server():
    port = _find_free_port()
    srv = qipc_server(host='127.0.0.1', port=port)
    srv.start_server()
    time.sleep(0.2)
    yield srv
    srv.stop_server()


@pytest.fixture
def client(server):
    return qipc_client('127.0.0.1', server.port)


# ── Server Unit Tests ────────────────────────────────────────────────

class TestServerUnit:

    def test_init(self):
        srv = qipc_server()
        assert srv.host == '127.0.0.1'
        assert srv.port == 5051
        assert srv.lists == {}
        assert srv._size_limits == {}

    def test_start_stop(self):
        srv = qipc_server(port=_find_free_port())
        srv.start_server()
        assert srv._server_thread is not None
        assert srv._server_thread.is_alive()
        srv.stop_server()
        assert srv._server_thread is None or not srv._server_thread.is_alive()

    def test_start_twice(self):
        srv = qipc_server(port=_find_free_port())
        srv.start_server()
        srv.start_server()
        srv.stop_server()

    def test_create_list(self):
        srv = qipc_server()
        srv.create_list("alpha")
        assert "alpha" in srv.lists
        assert len(srv.lists["alpha"]) == 0

    def test_create_list_with_size_limit(self):
        srv = qipc_server()
        srv.create_list("limited", size_limit=3)
        dq = srv.lists["limited"]
        dq.append(1)
        dq.append(2)
        dq.append(3)
        dq.append(4)
        assert list(dq) == [2, 3, 4]

    def test_create_list_invalid_size_limit(self):
        srv = qipc_server()
        with pytest.raises(ValueError):
            srv.create_list("bad", size_limit=0)

    def test_create_list_duplicate_is_noop(self):
        srv = qipc_server()
        srv.create_list("dup")
        srv.create_list("dup")
        assert len(srv.lists) == 1

    def test_get_lists(self):
        srv = qipc_server()
        srv.create_list("a")
        srv.create_list("b")
        result = srv.get_lists()
        assert "a" in result
        assert "b" in result

    def test_delete_list(self):
        srv = qipc_server()
        srv.create_list("x")
        srv.delete_list("x")
        assert "x" not in srv.lists

    def test_delete_list_nonexistent(self):
        srv = qipc_server()
        srv.delete_list("nonexistent")

    def test_modify_list(self):
        srv = qipc_server()
        srv.create_list("nums")
        srv.modify_list("nums", [10, 20, 30])
        assert list(srv.lists["nums"]) == [10, 20, 30]

    def test_modify_list_nonexistent_raises(self):
        srv = qipc_server()
        with pytest.raises(KeyError):
            srv.modify_list("ghost", [1])

    def test_handle_request_create_list(self):
        srv = qipc_server()
        resp = srv._handle_request({"action": "create_list", "list_name": "t"})
        assert resp["status"] == "ok"
        assert "t" in srv.lists

    def test_handle_request_get_lists(self):
        srv = qipc_server()
        srv.create_list("x")
        resp = srv._handle_request({"action": "get_lists"})
        assert resp["status"] == "ok"
        assert "x" in resp["data"]

    def test_handle_request_unknown_action(self):
        srv = qipc_server()
        resp = srv._handle_request({"action": "nonexistent"})
        assert resp["status"] == "error"


# ── Client-Server Integration Tests ──────────────────────────────────

class TestClientServerIntegration:

    def test_create_list_via_client(self, client, server):
        client.create_list("mylist")
        client.add_item("mylist", "hello")
        assert "mylist" in server.lists

    def test_add_and_get_last(self, client, server):
        client.create_list("lst")
        client.add_item("lst", "first")
        client.add_item("lst", "second")
        last = client.get_last("lst")
        assert last == "second"

    def test_add_and_get_list(self, client, server):
        client.create_list("lst")
        client.add_item("lst", "a")
        client.add_item("lst", "b")
        items = client.get_list("lst")
        assert items == ["a", "b"]

    def test_delete_last_element(self, client, server):
        client.create_list("lst")
        client.add_item("lst", "a")
        client.add_item("lst", "b")
        client.delete_last_element("lst")
        items = client.get_list("lst")
        assert items == ["a"]

    def test_delete_element_index(self, client, server):
        client.create_list("lst")
        client.add_item("lst", "a")
        client.add_item("lst", "b")
        client.add_item("lst", "c")
        client.delete_element_index("lst", 1)
        items = client.get_list("lst")
        assert items == ["a", "c"]

    def test_get_element_index_found(self, client, server):
        client.create_list("lst")
        client.add_item("lst", "x")
        client.add_item("lst", "y")
        idx = client.get_element_index("lst", "y")
        assert idx == 1

    def test_get_element_index_not_found(self, client, server):
        client.create_list("lst")
        client.add_item("lst", "x")
        idx = client.get_element_index("lst", "z")
        assert idx == -1

    def test_error_on_nonexistent_list(self, client, server):
        with pytest.raises(RuntimeError):
            client.get_list("nonexistent")

    def test_add_item_missing_list_raises(self, client, server):
        with pytest.raises(RuntimeError):
            client.add_item("ghost", "value")
        assert "ghost" not in server.lists

    def test_error_on_empty_list_get_last(self, client, server):
        client.create_list("lst")
        client.add_item("lst", 1)
        client.delete_last_element("lst")
        with pytest.raises(RuntimeError):
            client.get_last("lst")

    def test_error_on_index_out_of_range(self, client, server):
        client.create_list("lst")
        client.add_item("lst", 1)
        with pytest.raises(RuntimeError):
            client.delete_element_index("lst", 10)

    def test_size_limit_eviction(self, server):
        server.create_list("limited", size_limit=2)
        server._handle_request({"action": "add_item", "list_name": "limited", "item": 1})
        server._handle_request({"action": "add_item", "list_name": "limited", "item": 2})
        server._handle_request({"action": "add_item", "list_name": "limited", "item": 3})
        resp = server._handle_request({"action": "get_list", "list_name": "limited"})
        assert resp["data"] == [2, 3]

    def test_mixed_types(self, client, server):
        client.create_list("mix")
        client.add_item("mix", 42)
        client.add_item("mix", "hello")
        client.add_item("mix", [1, 2, 3])
        items = client.get_list("mix")
        assert items == [42, "hello", [1, 2, 3]]

    def test_concurrent_access(self, server):
        server.create_list("con")
        n = 50
        errors = []

        def writer(i):
            try:
                server._handle_request({"action": "add_item", "list_name": "con", "item": i})
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=writer, args=(i,)) for i in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        resp = server._handle_request({"action": "get_list", "list_name": "con"})
        assert len(resp["data"]) == n
        assert sorted(resp["data"]) == list(range(n))

    def test_delete_nonexistent_list_via_client(self, client, server):
        with pytest.raises(RuntimeError):
            client.delete_last_element("ghost")

    def test_modify_list_via_client(self, server):
        server.create_list("nums")
        server._handle_request({
            "action": "modify_list",
            "list_name": "nums",
            "new_list": [100, 200]
        })
        resp = server._handle_request({"action": "get_list", "list_name": "nums"})
        assert resp["data"] == [100, 200]

    def test_delete_list_via_client(self, server):
        server.create_list("temp")
        server._handle_request({"action": "delete_list", "list_name": "temp"})
        assert "temp" not in server.lists

    def test_server_cleanup_on_stop(self):
        srv = qipc_server(port=_find_free_port())
        srv.start_server()
        srv.stop_server()
        assert srv.server_sock is None

    def test_multiple_lists_independent(self, client, server):
        client.create_list("a")
        client.create_list("b")
        client.add_item("a", 1)
        client.add_item("b", "two")
        assert client.get_list("a") == [1]
        assert client.get_list("b") == ["two"]


# ── Regression tests for reported bugs ───────────────────────────────

class TestClientClassName:
    def test_client_class_is_spelled_correctly(self):
        import utils_tx.quick_ipc as q
        assert hasattr(q, "qipc_client")

    def test_legacy_alias_still_available(self):
        import utils_tx.quick_ipc as q
        assert q.qipc_client is q.qipc_clint


class TestModifyListRace:
    def test_delete_cannot_interleave_modify_list(self):
        srv = qipc_server()
        srv.create_list("x")
        srv._handle_request({"action": "add_item", "list_name": "x", "item": "old"})

        entered = threading.Event()
        release = threading.Event()
        original = srv._get_list_lock("x")

        class Gated:
            def acquire(self, blocking=True, timeout=-1):
                entered.set()
                release.wait(timeout=5)
                return original.acquire(blocking, timeout)

            def release(self):
                return original.release()

            def __enter__(self):
                self.acquire()
                return self

            def __exit__(self, *exc):
                self.release()

        srv._list_locks["x"] = Gated()
        errors = []

        def do_modify():
            try:
                srv.modify_list("x", [10, 20])
            except Exception as e:
                errors.append(e)

        t = threading.Thread(target=do_modify)
        t.start()
        assert entered.wait(timeout=5), "modify_list should have started"

        deleted = []

        def do_delete():
            srv.delete_list("x")
            deleted.append(True)

        d = threading.Thread(target=do_delete)
        d.start()
        time.sleep(0.2)
        assert d.is_alive(), "delete_list must be blocked while modify_list is in progress"

        release.set()
        t.join(timeout=5)
        d.join(timeout=5)
        assert not errors, f"modify_list raised: {errors}"
        assert deleted, "delete_list should finish after modify_list completes"
        assert "x" not in srv.lists


class TestServerStopDeadlock:
    def test_stop_server_from_worker_does_not_deadlock(self):
        port = _find_free_port()
        srv = qipc_server(host='127.0.0.1', port=port)
        srv.start_server()
        time.sleep(0.2)

        result = []

        def stop_from_worker():
            srv.stop_server()
            result.append(True)

        fut = srv.executor.submit(stop_from_worker)
        fut.result(timeout=5)
        assert result == [True]
