import socket
import threading
import json
import struct
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Optional


class qipc_server:
    """TCP server that maintains named lists for cross-process IPC.

    Clients connect, send a JSON request describing an action, and receive a
    JSON response. The server keeps all list state in memory.

    Supported actions:

    - ``create_list``  — create a new named list (optional size limit)
    - ``delete_list``  — remove a named list entirely
    - ``modify_list``  — replace a list with a new one
    - ``add_item``     — append an item to a list
    - ``get_last``     — retrieve the most recently appended item
    - ``get_list``     — retrieve the full list
    - ``delete_last_element`` — remove the last item
    - ``delete_element_index`` — remove an item by index
    - ``get_element_index`` — find the index of an item

    Thread-safe: all list mutations are protected by a per-list lock.

    Attributes:
        host: Hostname or IP to bind to.
        port: TCP port to listen on.
        lists: Dict mapping list name to ``deque`` of items.
        _size_limits: Dict mapping list name to optional max length.
    """

    def __init__(self, host: str = '127.0.0.1', port: int = 5051) -> None:
        """Initialise the IPC server.

        Args:
            host: Hostname or IP to bind the listening socket to.
                Defaults to ``'127.0.0.1'``.
            port: TCP port. Defaults to ``5051``.
        """
        self.host = host
        self.port = port
        self.lists: dict[str, deque] = {}
        self._size_limits: dict[str, Optional[int]] = {}

        self._lock = threading.Lock()
        self._list_locks: dict[str, threading.Lock] = {}
        self._locks_mutex = threading.Lock()

        self._stop_event = threading.Event()
        self.server_sock: Optional[socket.socket] = None
        self._max_workers = 20
        self.executor = ThreadPoolExecutor(max_workers=self._max_workers)
        self._executor_shutdown = False
        self._server_thread: Optional[threading.Thread] = None

    # ── list lock management ──────────────────────────────────────────

    def _get_list_lock(self, list_name: str) -> threading.Lock:
        """Retrieve or register a per-list lock for thread safety.

        Args:
            list_name: Name of the target list.

        Returns:
            A ``threading.Lock`` unique to the given list name.
        """
        with self._locks_mutex:
            if list_name not in self._list_locks:
                self._list_locks[list_name] = threading.Lock()
            return self._list_locks[list_name]

    # ── list operations (thread-safe) ─────────────────────────────────

    def get_lists(self) -> dict[str, list]:
        """Return a snapshot of all managed lists.

        Returns:
            A dict mapping list name to a plain ``list`` of its items.
            An empty dict if no lists have been created.
        """
        with self._lock:
            return {name: list(dq) for name, dq in self.lists.items()}

    def create_list(self, list_name: str, size_limit: Optional[int] = None) -> None:
        """Create a new named list.

        Args:
            list_name: Unique name for the list. If a list with this name
                already exists the call is a no-op.
            size_limit: Maximum number of items. When the limit is reached
                and a new item is appended, the oldest item is evicted.
                ``None`` (default) means unlimited growth.

        Raises:
            ValueError: If ``size_limit`` is provided and is less than 1.
        """
        if size_limit is not None and size_limit < 1:
            raise ValueError("size_limit must be >= 1 or None")

        with self._lock:
            if list_name not in self.lists:
                self.lists[list_name] = deque(maxlen=size_limit)
                self._size_limits[list_name] = size_limit

    def delete_list(self, list_name: str) -> None:
        """Delete a named list and all its items.

        Args:
            list_name: Name of the list to remove. If the list does not
                exist the call is a no-op.
        """
        with self._lock:
            self.lists.pop(list_name, None)
            self._size_limits.pop(list_name, None)

    def modify_list(self, list_name: str, new_list: list) -> None:
        """Replace the contents of an existing list with ``new_list``.

        If the list was created with a size limit, items beyond the limit
        are truncated (oldest items dropped).

        Args:
            list_name: Name of the list to modify.
            new_list: The new list of items.

        Raises:
            KeyError: If the list does not exist.
        """
        with self._lock:
            if list_name not in self.lists:
                raise KeyError(list_name)
            dq = self.lists[list_name]
            lock = self._get_list_lock(list_name)
            with lock:
                dq.clear()
                for item in new_list:
                    dq.append(item)

    # ── request router ────────────────────────────────────────────────

    def _handle_request(self, req: dict) -> dict:
        """Route an incoming request dict to the appropriate handler.

        Args:
            req: Parsed JSON dict with at least an ``"action"`` key.

        Returns:
            A response dict with ``"status": "ok"`` or ``"status": "error"``
            and any relevant data.

        Response formats by action:

        - ``get_lists``: ``{"status": "ok", "data": {...}}``
        - ``create_list``: ``{"status": "ok"}``
        - ``delete_list``: ``{"status": "ok"}``
        - ``modify_list``: ``{"status": "ok"}``
        - ``add_item``: ``{"status": "ok"}``
        - ``get_last``: ``{"status": "ok", "data": <item>}`` (or error if empty)
        - ``get_list``: ``{"status": "ok", "data": [...]}`` (or error if missing)
        - ``delete_last_element``: ``{"status": "ok"}`` (or error if empty)
        - ``delete_element_index``: ``{"status": "ok"}`` (or error if out of range)
        - ``get_element_index``: ``{"status": "ok", "data": <index>}`` (or -1 if not found)
        """
        action = req.get("action", "")
        list_name = req.get("list_name", "")

        try:
            if action == "get_lists":
                return {"status": "ok", "data": self.get_lists()}

            elif action == "create_list":
                size_limit = req.get("size_limit", None)
                self.create_list(list_name, size_limit)
                return {"status": "ok"}

            elif action == "delete_list":
                self.delete_list(list_name)
                return {"status": "ok"}

            elif action == "modify_list":
                new_list = req.get("new_list", [])
                self.modify_list(list_name, new_list)
                return {"status": "ok"}

            elif action == "add_item":
                item = req.get("item")
                with self._lock:
                    dq = self.lists.get(list_name)
                    if dq is None:
                        return {"status": "error", "error": f"List '{list_name}' not found"}
                    lock = self._get_list_lock(list_name)
                    with lock:
                        dq.append(item)
                return {"status": "ok"}

            elif action == "get_last":
                with self._lock:
                    dq = self.lists.get(list_name)
                if dq is None:
                    return {"status": "error", "error": f"List '{list_name}' not found"}
                if len(dq) == 0:
                    return {"status": "error", "error": "List is empty"}
                lock = self._get_list_lock(list_name)
                with lock:
                    item = dq[-1]
                return {"status": "ok", "data": item}

            elif action == "get_list":
                with self._lock:
                    dq = self.lists.get(list_name)
                if dq is None:
                    return {"status": "error", "error": f"List '{list_name}' not found"}
                lock = self._get_list_lock(list_name)
                with lock:
                    items = list(dq)
                return {"status": "ok", "data": items}

            elif action == "delete_last_element":
                with self._lock:
                    dq = self.lists.get(list_name)
                if dq is None:
                    return {"status": "error", "error": f"List '{list_name}' not found"}
                if len(dq) == 0:
                    return {"status": "error", "error": "List is empty"}
                lock = self._get_list_lock(list_name)
                with lock:
                    dq.pop()
                return {"status": "ok"}

            elif action == "delete_element_index":
                index = req.get("element_index", -1)
                with self._lock:
                    dq = self.lists.get(list_name)
                if dq is None:
                    return {"status": "error", "error": f"List '{list_name}' not found"}
                lock = self._get_list_lock(list_name)
                with lock:
                    try:
                        del dq[index]
                    except IndexError:
                        return {"status": "error", "error": f"Index {index} out of range"}
                return {"status": "ok"}

            elif action == "get_element_index":
                element = req.get("element")
                with self._lock:
                    dq = self.lists.get(list_name)
                if dq is None:
                    return {"status": "error", "error": f"List '{list_name}' not found"}
                lock = self._get_list_lock(list_name)
                with lock:
                    try:
                        idx = dq.index(element)
                    except ValueError:
                        idx = -1
                return {"status": "ok", "data": idx}

            else:
                return {"status": "error", "error": f"Unknown action '{action}'"}

        except KeyError as e:
            return {"status": "error", "error": f"List '{e.args[0]}' not found"}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    # ── TCP helpers ───────────────────────────────────────────────────

    def _recv_exact(self, conn: socket.socket, num_bytes: int) -> Optional[bytes]:
        """Read an exact number of bytes from a socket.

        Args:
            conn: An open TCP socket.
            num_bytes: Number of bytes to read.

        Returns:
            Exactly ``num_bytes`` of data, or ``None`` if the connection
            closed before enough data arrived.
        """
        data = bytearray()
        while len(data) < num_bytes:
            packet = conn.recv(num_bytes - len(data))
            if not packet:
                return None
            data.extend(packet)
        return bytes(data)

    def _handle_client(self, client_socket: socket.socket) -> None:
        """Read a framed JSON request, process it, and send a framed response.

        Args:
            client_socket: The accepted socket connected to a client.
        """
        try:
            client_socket.settimeout(5.0)
            header = self._recv_exact(client_socket, 4)
            if not header:
                return

            payload_len = struct.unpack('!I', header)[0]
            payload_bytes = self._recv_exact(client_socket, payload_len)
            if not payload_bytes:
                return

            req = json.loads(payload_bytes.decode('utf-8'))
            resp = self._handle_request(req)

            resp_bytes = json.dumps(resp).encode('utf-8')
            client_socket.sendall(struct.pack('!I', len(resp_bytes)) + resp_bytes)

        except Exception as e:
            error_resp = json.dumps({"status": "error", "error": str(e)}).encode('utf-8')
            try:
                client_socket.sendall(struct.pack('!I', len(error_resp)) + error_resp)
            except Exception:
                pass
        finally:
            try:
                client_socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            client_socket.close()

    def _run_server_loop(self) -> None:
        """Accept loop running inside the background server thread."""
        if not self.server_sock:
            return

        try:
            while not self._stop_event.is_set():
                try:
                    client_sock, _ = self.server_sock.accept()
                    self.executor.submit(self._handle_client, client_sock)
                except socket.timeout:
                    continue
                except OSError:
                    break
        except Exception as e:
            print(f"[qipc_server] Server loop error: {e}")
        finally:
            self._cleanup()

    def start_server(self) -> None:
        """Start the IPC server in a non-blocking background thread.

        Creates a TCP socket, binds, listens, and launches the accept loop
        in a daemon thread. Safe to call multiple times.
        """
        if self._server_thread and self._server_thread.is_alive():
            return

        if self._executor_shutdown:
            self.executor = ThreadPoolExecutor(max_workers=self._max_workers)
            self._executor_shutdown = False

        self._stop_event.clear()
        self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_sock.settimeout(1.0)

        try:
            self.server_sock.bind((self.host, self.port))
            self.server_sock.listen(128)
            self._server_thread = threading.Thread(target=self._run_server_loop, daemon=True)
            self._server_thread.start()
        except Exception as e:
            print(f"[qipc_server] Failed to start: {e}")
            self.stop_server()

    def _cleanup(self) -> None:
        """Close the listening socket and shut down the thread pool."""
        if self.server_sock:
            try:
                self.server_sock.close()
            except Exception:
                pass
            self.server_sock = None
        self.executor.shutdown(wait=False, cancel_futures=True)
        self._executor_shutdown = True

    def stop_server(self) -> None:
        """Gracefully stop the IPC server.

        Signals the background loop, closes the socket, shuts down the
        thread pool, and waits for the server thread to finish.
        """
        if self._stop_event.is_set() and self.server_sock is None:
            return

        self._stop_event.set()
        self._cleanup()
        if self._server_thread:
            self._server_thread.join(timeout=3.0)
            self._server_thread = None


# ─────────────────────────────────────────────────────────────────────
# Client
# ─────────────────────────────────────────────────────────────────────


class qipc_client:
    """TCP client for ``qipc_server`` that sends requests and receives responses.

    Each method opens a fresh TCP connection to the server, sends a
    length-prefixed JSON request, reads the length-prefixed JSON response,
    and returns the result. This keeps the client stateless and simple.

    Attributes:
        server_host: Hostname or IP of the target server.
        server_port: Port of the target server.
        timeout: Socket connection timeout in seconds.
    """

    def __init__(self, server_host: str, server_port: int, timeout: float = 5.0) -> None:
        """Initialise the IPC client.

        Args:
            server_host: Hostname or IP of the ``qipc_server``.
            server_port: Port of the ``qipc_server``.
            timeout: Socket connection timeout in seconds. Defaults to 5.0.
        """
        self.server_host = server_host
        self.server_port = server_port
        self.timeout = timeout

    def _send_request(self, request: dict) -> dict:
        """Send a JSON request and receive the JSON response.

        Args:
            request: A dict that will be serialised as JSON and sent.

        Returns:
            The response dict from the server.

        Raises:
            ConnectionRefusedError: If the server is unreachable.
            socket.timeout: If the connection or response times out.
        """
        payload_bytes = json.dumps(request).encode('utf-8')
        header = struct.pack('!I', len(payload_bytes))

        with socket.create_connection((self.server_host, self.server_port), timeout=self.timeout) as sock:
            sock.sendall(header + payload_bytes)

            resp_header = self._recv_exact(sock, 4)
            resp_len = struct.unpack('!I', resp_header)[0]
            resp_bytes = self._recv_exact(sock, resp_len)
            return json.loads(resp_bytes.decode('utf-8'))

    @staticmethod
    def _recv_exact(conn: socket.socket, num_bytes: int) -> bytes:
        """Read an exact number of bytes from a socket.

        Args:
            conn: An open TCP socket.
            num_bytes: Number of bytes to read.

        Returns:
            Exactly ``num_bytes`` of data.

        Raises:
            ConnectionError: If the connection closes before enough data arrives.
        """
        data = bytearray()
        while len(data) < num_bytes:
            packet = conn.recv(num_bytes - len(data))
            if not packet:
                raise ConnectionError("Connection closed while reading response")
            data.extend(packet)
        return bytes(data)

    # ── Public API ────────────────────────────────────────────────────

    def create_list(self, list_name: str, size_limit: int | None = None) -> None:
        """Create a new named list on the server.

        Args:
            list_name: Unique name for the list.
            size_limit: Maximum number of items before oldest is evicted.
                ``None`` (default) means unlimited.

        Raises:
            RuntimeError: If the server returns an error.
        """
        resp = self._send_request({
            "action": "create_list",
            "list_name": list_name,
            "size_limit": size_limit,
        })
        if resp.get("status") == "error":
            raise RuntimeError(resp.get("error", "Unknown error"))

    def add_item(self, list_name: str, item: Any) -> None:
        """Append an item to a named list on the server.

        Args:
            list_name: Name of the target list.
            item: The item to append (any JSON-serialisable type).

        Raises:
            RuntimeError: If the server returns an error (e.g. list not found).
        """
        resp = self._send_request({"action": "add_item", "list_name": list_name, "item": item})
        if resp.get("status") == "error":
            raise RuntimeError(resp.get("error", "Unknown error"))

    def get_last(self, list_name: str) -> Any:
        """Retrieve the most recently appended item from a named list.

        Args:
            list_name: Name of the list.

        Returns:
            The last item in the list.

        Raises:
            RuntimeError: If the list is empty or does not exist.
        """
        resp = self._send_request({"action": "get_last", "list_name": list_name})
        if resp.get("status") == "error":
            raise RuntimeError(resp.get("error", "Unknown error"))
        return resp["data"]

    def get_list(self, list_name: str) -> list:
        """Retrieve the full contents of a named list.

        Args:
            list_name: Name of the list.

        Returns:
            A list of all items currently in the named list.

        Raises:
            RuntimeError: If the list does not exist.
        """
        resp = self._send_request({"action": "get_list", "list_name": list_name})
        if resp.get("status") == "error":
            raise RuntimeError(resp.get("error", "Unknown error"))
        return resp["data"]

    def delete_last_element(self, list_name: str) -> None:
        """Delete the most recently appended item from a named list.

        Args:
            list_name: Name of the list.

        Raises:
            RuntimeError: If the list is empty or does not exist.
        """
        resp = self._send_request({"action": "delete_last_element", "list_name": list_name})
        if resp.get("status") == "error":
            raise RuntimeError(resp.get("error", "Unknown error"))

    def delete_element_index(self, list_name: str, element_index: int) -> None:
        """Delete the item at the given index from a named list.

        Args:
            list_name: Name of the list.
            element_index: Index of the item to remove.

        Raises:
            RuntimeError: If the index is out of range or the list does not exist.
        """
        resp = self._send_request({
            "action": "delete_element_index",
            "list_name": list_name,
            "element_index": element_index,
        })
        if resp.get("status") == "error":
            raise RuntimeError(resp.get("error", "Unknown error"))

    def get_element_index(self, list_name: str, element: Any) -> int:
        """Find the index of an item in a named list.

        Args:
            list_name: Name of the list.
            element: The item to search for.

        Returns:
            The index (0-based) of the first occurrence, or ``-1`` if the
            item is not found.

        Raises:
            RuntimeError: If the list does not exist.
        """
        resp = self._send_request({
            "action": "get_element_index",
            "list_name": list_name,
            "element": element,
        })
        if resp.get("status") == "error":
            raise RuntimeError(resp.get("error", "Unknown error"))
        return resp["data"]


# Backwards-compatible alias for the old misspelled name.
qipc_clint = qipc_client
