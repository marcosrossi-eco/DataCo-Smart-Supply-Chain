# DataCo Smart Supply Chain - Utils
import json
import logging
from pathlib import Path
from datetime import datetime

def get_project_root() -> Path:
    return Path(__file__).resolve().parent.parent

def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)

def setup_logger(name: str, log_dir: Path) -> logging.Logger:
    ensure_dir(log_dir)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir / (name + '_' + ts + '.log')
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    fmt = logging.Formatter('%(asctime)s | %(levelname)s | %(name)s | %(message)s')
    fh = logging.FileHandler(log_file, encoding='utf-8')
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    return logger

def save_json(obj, path: Path) -> None:
    ensure_dir(path.parent)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def now_utc_iso() -> str:
    return datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
