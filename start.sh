#!/bin/bash
# DocFusion 一键启动脚本
set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

echo "================================================"
echo "  DocFusion - 文档理解与多源数据融合系统"
echo "================================================"

# 1. Setup venv
if [ ! -d "venv" ]; then
    echo "[1/3] 创建虚拟环境..."
    python3 -m venv venv
fi

source venv/bin/activate

# 2. Install dependencies
echo "[2/3] 安装依赖..."
pip install -q -r backend/requirements.txt 2>/dev/null || pip install -r backend/requirements.txt

# 3. Create dirs
mkdir -p uploads outputs logs

# 4. Start server
echo "[3/3] 启动服务器..."
echo ""

# 默认端口，可由环境变量 PORT 覆盖
DEFAULT_PORT=8000
if [ -n "${PORT:-}" ]; then
    SELECTED_PORT="$PORT"
else
    SELECTED_PORT="$DEFAULT_PORT"
fi

is_port_in_use() {
    local p="$1"
    if ss -ltn 2>/dev/null | awk '{print $4}' | grep -q ":$p$"; then
        return 0
    fi
    if command -v lsof >/dev/null 2>&1; then
        if lsof -iTCP:$p -sTCP:LISTEN -Pn 2>/dev/null | grep -q .; then
            return 0
        fi
    fi
    return 1
}

if is_port_in_use "$SELECTED_PORT"; then
    echo "端口 $SELECTED_PORT 已被占用，正在选择可用端口..."
    SELECTED_PORT=$(python3 - <<'PY'
import socket
s=socket.socket()
s.bind(('127.0.0.1',0))
port=s.getsockname()[1]
s.close()
print(port)
PY
)
    echo "使用随机可用端口: $SELECTED_PORT"
fi

echo ""
echo "  后端 API:   http://localhost:${SELECTED_PORT}/docs"
echo "  前端界面:   http://localhost:${SELECTED_PORT}/"
echo "  健康检查:   http://localhost:${SELECTED_PORT}/api/health"
echo ""
echo "按 Ctrl+C 停止服务"
echo "================================================"

cd "$PROJECT_DIR"
mkdir -p logs
echo "$SELECTED_PORT" > logs/last_port.txt
python -m uvicorn backend.app.main:app --host 0.0.0.0 --port "$SELECTED_PORT" --reload
