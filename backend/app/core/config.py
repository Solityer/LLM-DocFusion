"""Global configuration."""
import json
import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
CONFIG_PATH = PROJECT_ROOT / "config" / "config.json"
SCHEMA_REGISTRY_PATH = PROJECT_ROOT / "config" / "schema_registry.json"
UPLOAD_DIR = PROJECT_ROOT / "uploads"
OUTPUT_DIR = PROJECT_ROOT / "outputs"
LOG_DIR = PROJECT_ROOT / "logs"
TEST_DATA_DIR = PROJECT_ROOT / "测试集"

for d in [UPLOAD_DIR, OUTPUT_DIR, LOG_DIR]:
    d.mkdir(parents=True, exist_ok=True)


def load_config() -> dict:
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


_cfg = load_config()

OLLAMA_BASE = _cfg.get("llm", {}).get("ollama", {}).get("api_base", "http://localhost:11434")
OLLAMA_MODEL = _cfg.get("llm", {}).get("ollama", {}).get("model_name", "qwen2.5:14b")
OLLAMA_TEMPERATURE = _cfg.get("llm", {}).get("ollama", {}).get("temperature", 0.3)
OLLAMA_NUM_PREDICT = _cfg.get("llm", {}).get("ollama", {}).get("num_predict", 1024)
MAX_INPUT_LENGTH = _cfg.get("processing", {}).get("max_input_length", 6000)
MAX_RETRIES = _cfg.get("optimization", {}).get("max_retries", 3)
