import builtins
from pathlib import Path
from datetime import datetime
import threading
import os

_log_lock = threading.Lock()
_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
_log_dir = Path(os.getenv("LOG_DIR", "/app/logs"))
_log_path = _log_dir / f"server_{_ts}.log"


def log_print(*args, **kwargs):
    builtins.print(*args, **kwargs)
    with _log_lock:
        _log_path.parent.mkdir(parents=True, exist_ok=True)
        with _log_path.open("a", encoding="utf-8") as f:
            builtins.print(*args, **kwargs, file=f)
