import socket
import threading
import json
import os
import struct
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from colorama import init, Fore, Style

# Initialize colorama for cross-platform colored printing
init(autoreset=True)

# Log Type Constants
LOG_DEBUG = 1
LOG_INFO = 2
LOG_SUCCESS = 3
LOG_WARNING = 4
LOG_ERROR = 5

LOG_COLORS = {
    LOG_DEBUG: Fore.CYAN,
    LOG_INFO: Fore.BLUE,
    LOG_SUCCESS: Fore.GREEN,
    LOG_WARNING: Fore.YELLOW,
    LOG_ERROR: Fore.RED
}

# Mapping for string based log type resolution
STR_TO_LOG_TYPE = {
    "debug": LOG_DEBUG,
    "info": LOG_INFO,
    "success": LOG_SUCCESS,
    "warning": LOG_WARNING,
    "error": LOG_ERROR
}


class LoggerClient:
    """TCP-based logging client that sends formatted log messages to a LoggerServer.

    Provides convenience methods for five log levels (debug, info, success, warning,
    error). Each call opens a fresh TCP connection, sends a length-prefixed JSON
    payload, and closes the connection. If the server is unreachable the error is
    silently printed to stderr rather than raised, making the client safe for use
    in production paths where logging should never crash the caller.

    Attributes:
        name: Instance name embedded in every log message for source identification.
        timeout: Socket connection timeout in seconds.
        server_host: Resolved hostname or IP of the target LoggerServer.
        server_port: Port of the target LoggerServer.
    """

    def __init__(self, logger_server_address: str, name: str, timeout: float = 5.0):
        """Initializes a LoggerClient.

        Args:
            logger_server_address: Server address in ``host:port`` format,
                e.g. ``"localhost:5050"``.
            name: Name for this client instance, embedded in every log message.
            timeout: Socket connection timeout in seconds. Defaults to 5.0.

        Raises:
            ValueError: If ``logger_server_address`` is not in ``host:port`` format.
        """
        self.name = name
        self.timeout = timeout
        try:
            host, port = logger_server_address.rsplit(':', 1)
            self.server_host = host
            self.server_port = int(port)
        except ValueError:
            raise ValueError("logger_server_address must be in the format 'host:port'")

    def _send_log(self, log_type: int, type_str: str, message: str, custom_name: str | None = None):
        """Format, frame, and send a log message to the configured server.

        Builds a timestamped message string, wraps it in a JSON payload with the
        numeric log type, prefixes the JSON with a 4-byte big-endian length header,
        and transmits everything over a temporary TCP connection.

        Args:
            log_type: Numeric log level (``LOG_DEBUG``, ``LOG_INFO``, etc.).
            type_str: Human-readable log level label (``"debug"``, ``"info"``, etc.).
            message: The log message content.
            custom_name: Optional secondary name label. When provided the formatted
                message includes ``: [custom_name] ->`` instead of ``->``.
        """
        timestamp = datetime.now().strftime("%d-%m-%y %H:%M:%S")

        if custom_name is None:
            log_string = f"[{timestamp}] [{type_str}] @ [{self.name}] -> {message}"
        else:
            log_string = f"[{timestamp}] [{type_str}] @ [{self.name}] : [{custom_name}] -> {message}"

        payload = {
            "type": log_type,
            "message": log_string
        }

        try:
            payload_bytes = json.dumps(payload).encode('utf-8')
            length_prefix = struct.pack('!I', len(payload_bytes))

            with socket.create_connection((self.server_host, self.server_port), timeout=self.timeout) as sock:
                sock.sendall(length_prefix + payload_bytes)
        except (ConnectionRefusedError, socket.timeout, OSError) as e:
            print(f"{Fore.RED}[Logger Client Error] Failed to send log to {self.server_host}:{self.server_port}. Reason: {e}")

    def debug(self, message: str, name: str | None = None):
        """Send a DEBUG level log message.

        Args:
            message: The log message content.
            name: Optional secondary name to include in the log format.
        """
        self._send_log(LOG_DEBUG, "debug", message, name)

    def info(self, message: str, name: str | None = None):
        """Send an INFO level log message.

        Args:
            message: The log message content.
            name: Optional secondary name to include in the log format.
        """
        self._send_log(LOG_INFO, "info", message, name)

    def success(self, message: str, name: str | None = None):
        """Send a SUCCESS level log message.

        Args:
            message: The log message content.
            name: Optional secondary name to include in the log format.
        """
        self._send_log(LOG_SUCCESS, "success", message, name)

    def warning(self, message: str, name: str | None = None):
        """Send a WARNING level log message.

        Args:
            message: The log message content.
            name: Optional secondary name to include in the log format.
        """
        self._send_log(LOG_WARNING, "warning", message, name)

    def error(self, message: str, name: str | None = None):
        """Send an ERROR level log message.

        Args:
            message: The log message content.
            name: Optional secondary name to include in the log format.
        """
        self._send_log(LOG_ERROR, "error", message, name)



class LoggerServer:
    """TCP logging server that receives framed JSON log messages from LoggerClients.

    Listens on a configurable host:port, accepts client connections, and dispatches
    each incoming log to configurable handlers per log level:

    - **Console output** (colourised via colorama when enabled)
    - **File output** (thread-safe, per-file locks, auto-creates directories)
    - **In-memory ring buffer** (bounded ``deque`` per level, configurable max size)

    The server runs its accept loop in a single daemon thread and delegates log
    processing to a ``ThreadPoolExecutor``, ensuring the accept loop is never
    blocked by slow handlers.

    Attributes:
        host: Hostname or IP to bind to.
        port: TCP port to listen on.
        color_print: Whether to colourise console output using colorama.
        configs: Dict mapping log level to ``{"print": bool, "file": str | None}``.
        mem_logs: Dict mapping log level to a ``deque`` of in-memory log strings.
        executor: ThreadPoolExecutor for concurrent client handling.
    """

    def __init__(
        self,
        server_host: str = 'localhost',
        port: int = 5050,
        print_debug: bool = True,
        debug_log_file: str | None = None,
        debug_max_in_mem: int = 0,
        print_info: bool = True,
        info_log_file: str | None = None,
        info_max_in_mem: int = 0,
        print_success: bool = True,
        success_log_file: str | None = None,
        success_max_in_mem: int = 0,
        print_warning: bool = True,
        warning_log_file: str | None = None,
        warning_max_in_mem: int = 0,
        print_error: bool = True,
        error_log_file: str | None = None,
        error_max_in_mem: int = 0,
        color_print: bool = True,
        max_workers: int = 20
    ):
        """Initialises the LoggerServer.

        Each of the five log levels has three independent configuration knobs:

        - ``print_<level>``: whether to echo matching logs to stdout.
        - ``<level>_log_file``: optional file path to append matching logs to.
        - ``<level>_max_in_mem``: max number of matching logs retained in memory
          (0 disables in-memory storage for that level).

        Args:
            server_host: Hostname or IP to bind the listening socket to.
                Defaults to ``"localhost"``.
            port: TCP port. Defaults to ``5050``.
            print_debug: Print DEBUG messages to stdout. Defaults to True.
            debug_log_file: File path for DEBUG messages. Defaults to None (no file).
            debug_max_in_mem: Max DEBUG messages in ring buffer. Defaults to 0 (disabled).
            print_info: Print INFO messages to stdout. Defaults to True.
            info_log_file: File path for INFO messages. Defaults to None.
            info_max_in_mem: Max INFO messages in ring buffer. Defaults to 0.
            print_success: Print SUCCESS messages to stdout. Defaults to True.
            success_log_file: File path for SUCCESS messages. Defaults to None.
            success_max_in_mem: Max SUCCESS messages in ring buffer. Defaults to 0.
            print_warning: Print WARNING messages to stdout. Defaults to True.
            warning_log_file: File path for WARNING messages. Defaults to None.
            warning_max_in_mem: Max WARNING messages in ring buffer. Defaults to 0.
            print_error: Print ERROR messages to stdout. Defaults to True.
            error_log_file: File path for ERROR messages. Defaults to None.
            error_max_in_mem: Max ERROR messages in ring buffer. Defaults to 0.
            color_print: Apply ANSI colour codes to console output. Defaults to True.
            max_workers: Max worker threads in the ``ThreadPoolExecutor`` for
                concurrent client handling. Defaults to 20.
        """
        self.host = server_host
        self.port = port
        self.color_print = color_print

        self.configs = {
            LOG_DEBUG: {"print": print_debug, "file": debug_log_file},
            LOG_INFO: {"print": print_info, "file": info_log_file},
            LOG_SUCCESS: {"print": print_success, "file": success_log_file},
            LOG_WARNING: {"print": print_warning, "file": warning_log_file},
            LOG_ERROR: {"print": print_error, "file": error_log_file},
        }

        self._mem_lock = threading.Lock()
        self.mem_logs = {}

        mem_limits = {
            LOG_DEBUG: debug_max_in_mem,
            LOG_INFO: info_max_in_mem,
            LOG_SUCCESS: success_max_in_mem,
            LOG_WARNING: warning_max_in_mem,
            LOG_ERROR: error_max_in_mem,
        }
        for log_type, limit in mem_limits.items():
            if limit > 0:
                self.mem_logs[log_type] = deque(maxlen=limit)

        self.file_locks = {}
        self._locks_mutex = threading.Lock()

        self._stop_event = threading.Event()
        self.server_sock = None
        self._max_workers = max_workers
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self._executor_shutdown = False

        self._server_thread = None

    def _active_log_files(self) -> set:
        """Return the set of file paths currently referenced by any log level."""
        return {
            config["file"]
            for config in self.configs.values()
            if isinstance(config["file"], str)
        }

    def _get_file_lock(self, file_path: str) -> threading.Lock:
        """Retrieve or register a per-file lock for thread-safe appending.

        Locks for file paths that are no longer referenced by any log level
        configuration are dropped so the registry stays bounded.

        Args:
            file_path: Absolute or relative path to the log file.

        Returns:
            A ``threading.Lock`` unique to the given file path.
        """
        with self._locks_mutex:
            lock = self.file_locks.get(file_path)
            if lock is None:
                lock = threading.Lock()
                self.file_locks[file_path] = lock
            active = self._active_log_files()
            for stale_path in [path for path in self.file_locks
                               if path not in active and path != file_path]:
                del self.file_locks[stale_path]
            return lock

    def _append_to_file(self, file_path: str, content: str):
        """Atomically append a line to a log file.

        Creates parent directories if they do not exist. Uses a per-file lock to
        prevent interleaved writes from concurrent threads.

        Args:
            file_path: Path to the target log file.
            content: The log line to append (a ``\\n`` is added automatically).
        """
        file_lock = self._get_file_lock(file_path)
        with file_lock:
            try:
                dir_name = os.path.dirname(file_path)
                if dir_name:
                    os.makedirs(dir_name, exist_ok=True)

                with open(file_path, "a", encoding="utf-8") as f:
                    f.write(content + "\n")
            except Exception as e:
                print(f"[{Fore.RED}Critical{Style.RESET_ALL}] Failed to write to {file_path}: {e}")

    def _recv_exact(self, conn: socket.socket, num_bytes: int) -> bytes | None:
        """Read an exact number of bytes from a socket, retrying until complete.

        Args:
            conn: An open TCP socket.
            num_bytes: Number of bytes to read.

        Returns:
            Exactly ``num_bytes`` of data, or ``None`` if the connection was
            closed before enough data arrived.
        """
        data = bytearray()
        while len(data) < num_bytes:
            packet = conn.recv(num_bytes - len(data))
            if not packet:
                return None
            data.extend(packet)
        return bytes(data)

    def _handle_client(self, client_socket: socket.socket):
        """Process a single framed JSON payload from a client connection.

        Reads the 4-byte big-endian length prefix, then reads the JSON payload,
        dispatches it according to the log level's configuration (console, file,
        and/or in-memory ring buffer), and closes the connection.

        Args:
            client_socket: The accepted socket connected to a LoggerClient.
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

            payload = json.loads(payload_bytes.decode('utf-8'))
            log_type = payload.get("type")
            log_message = payload.get("message")

            if isinstance(log_type, str):
                log_type = STR_TO_LOG_TYPE.get(log_type.lower())

            if log_type in self.configs:
                config = self.configs[log_type]

                if log_type in self.mem_logs:
                    with self._mem_lock:
                        self.mem_logs[log_type].append(log_message)

                if config["print"]:
                    if self.color_print and log_type in LOG_COLORS:
                        color = LOG_COLORS[log_type]
                        print(f"{color}{log_message}{Style.RESET_ALL}")
                    else:
                        print(log_message)

                if config["file"] and isinstance(config["file"], str):
                    self._append_to_file(config["file"], log_message)

        except Exception as e:
            print(f"Error handling incoming log stream: {e}")
        finally:
            try:
                client_socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            client_socket.close()

    def get_logs(self, log_type: None | int | str = None) -> list:
        """Retrieve logs currently held in the in-memory ring buffers.

        Args:
            log_type: Controls which logs are returned:

                - ``None`` (default): returns a list of five lists, one per log
                  level, in the order ``[debug, info, success, warning, error]``.
                - An ``int`` log level constant (e.g. ``LOG_INFO``): returns a
                  single list for that level.
                - A ``str`` log level name (e.g. ``"info"``): resolved via
                  ``STR_TO_LOG_TYPE`` and returns the matching list.

        Returns:
            A list of log strings. Returns an empty list if the requested level
            has no in-memory storage configured or no logs have arrived.
        """
        with self._mem_lock:
            if log_type is None:
                return [
                    list(self.mem_logs[level]) if level in self.mem_logs else []
                    for level in (LOG_DEBUG, LOG_INFO, LOG_SUCCESS, LOG_WARNING, LOG_ERROR)
                ]

            resolved_type = log_type
            if isinstance(log_type, str):
                resolved_type = STR_TO_LOG_TYPE.get(log_type.lower())

            if resolved_type in self.mem_logs:
                return list(self.mem_logs[resolved_type])

            return []

    def _run_server_loop(self):
        """Accept loop running inside the background server thread.

        Continuously accepts incoming connections and submits them to the
        thread pool for processing. The accept call has a 1-second timeout so
        that the loop can react promptly to the stop event.
        """
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
            print(f"Server loop error: {e}")
        finally:
            self._cleanup_sockets_and_pool()

    def start_server(self):
        """Start the logging server in a non-blocking background thread.

        Creates a TCP socket, binds to the configured host and port, begins
        listening, and launches ``_run_server_loop`` in a daemon thread. Safe
        to call multiple times — subsequent calls are no-ops while the server
        is already running.
        """
        if self._server_thread and self._server_thread.is_alive():
            print(f"{Fore.YELLOW}[System Server] Server is already running.")
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
            print(f"{Fore.MAGENTA}[System Server] Logging server operational on {self.host}:{self.port}\n")

            self._server_thread = threading.Thread(target=self._run_server_loop, daemon=True)
            self._server_thread.start()

        except Exception as e:
            print(f"{Fore.RED}[System Server] Failed to bind or start server: {e}")
            self.stop_server()

    def _cleanup_sockets_and_pool(self):
        """Close the listening socket and shut down the thread pool.

        Called once during server shutdown. The executor is told to wait for
        all currently running tasks to finish.
        """
        if self.server_sock:
            try:
                self.server_sock.close()
            except Exception:
                pass
            self.server_sock = None

        self.executor.shutdown(wait=False, cancel_futures=True)
        self._executor_shutdown = True

    def stop_server(self):
        """Gracefully stop the logging server.

        Signals the background loop to exit, closes the listening socket,
        shuts down the thread pool, and waits for the server thread to finish.
        Safe to call when the server is not running.
        """
        if self._stop_event.is_set() and self.server_sock is None:
            return

        print(f"\n{Fore.MAGENTA}[System Server] Stopping logging server...")
        self._stop_event.set()

        self._cleanup_sockets_and_pool()

        if self._server_thread:
            self._server_thread.join(timeout=3.0)
            self._server_thread = None
        print(f"{Fore.MAGENTA}[System Server] Server stopped cleanly.")
