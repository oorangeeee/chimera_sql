"""具有控制台和文件输出的日志记录工具。"""

import logging

from src.utils.constants import PROJECT_ROOT

_LOG_DIR = PROJECT_ROOT / "data" / "logs"
_LOG_FILE = _LOG_DIR / "chimera.log"

_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

_initialized = False


def _init_logging():
    """设置根记录器，包含控制台和文件处理程序（仅调用一次）。"""
    global _initialized
    if _initialized:
        return
    _initialized = True

    _LOG_DIR.mkdir(parents=True, exist_ok=True)

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    # 控制台处理程序 — INFO 级别（进度可见）
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter(_FORMAT, datefmt=_DATE_FORMAT))
    root.addHandler(console)

    # 文件处理程序 — DEBUG 级别（完整实验数据）
    file_handler = logging.FileHandler(_LOG_FILE, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(_FORMAT, datefmt=_DATE_FORMAT))
    root.addHandler(file_handler)


def get_logger(name: str) -> logging.Logger:
    """获取一个已命名的日志记录器。首次调用时初始化日志记录。"""
    _init_logging()
    return logging.getLogger(name)
