# DocFusion - 文档理解与多源数据融合系统

## 系统概述

DocFusion 是一个基于大语言模型（LLM）的文档理解与多源数据融合系统，支持多种格式文档的读取、理解和结构化信息抽取，并能自动填写各类模板。

### 核心能力

- **多源异构文档读取**：支持 Excel (.xlsx/.xls)、Word (.docx)、Markdown (.md)、Text (.txt)、CSV (.csv)、JSON (.json)
- **多源连接器**：支持本地文件、HTTP/API、网页 HTML 表格/正文、SQLite 查询结果统一接入
- **字段标准化与质量识别**：提供 schema registry、字段别名对齐、数值/日期/百分比/金额标准化、缺失/重复/异常/冲突检测
- **统一文档归一化**：所有格式统一转换为 DocumentBundle 中间表示
- **任意格式模板解析**：所有支持的文件格式均可作为数据源或模板
- **智能数据抽取**：基于规则检索 + LLM 增强的混合抽取策略
- **多格式模板填充**：自动将抽取数据写回到对应格式的模板中
- **结果验证**：自动验证填充结果的完整性和正确性
- **本地 LLM 部署**：基于 Ollama + qwen2.5:14b 本地推理

### 技术架构

```
┌─────────────────────────────────────────┐
│           前端展示界面 (HTML/JS)          │
├─────────────────────────────────────────┤
│           FastAPI REST API              │
├──────┬──────┬──────┬──────┬─────────────┤
│ 文档  │ 模板  │ 需求  │ 检索  │ 抽取 / 填充│
│ 解析  │ 解析  │ 解析  │ 引擎  │   引擎     │
├──────┴──────┴──────┴──────┴─────────────┤
│        Ollama + qwen2.5:14b             │
└─────────────────────────────────────────┘
```

## 快速开始

### 环境要求

- Python 3.10+
- Ollama 已安装并运行 qwen2.5:14b 模型
- Linux 系统

### 一键启动

```bash
cd /home/match/LLM-DocFusion
chmod +x start.sh
./start.sh
```

启动后：
- 前端界面: http://localhost:8000/
- API 文档: http://localhost:8000/docs
- 健康检查: http://localhost:8000/api/health

启动后：
- `start.sh` 会自动选择可用端口并在控制台中打印实际地址；也可通过环境变量 `PORT` 指定端口（例如：`PORT=8001 ./start.sh`）。
- 前端界面: http://localhost:<PORT>（以 start.sh 控制台输出为准）
- API 文档: http://localhost:<PORT>/docs
- 健康检查: http://localhost:<PORT>/api/health

### 手动启动

```bash
cd /home/match/LLM-DocFusion
source venv/bin/activate
# 使用 start.sh 自动选择可用端口并启动（或用 PORT 指定端口）
./start.sh

# 或者手动指定端口并启动 uvicorn：
PORT=8001 python -m uvicorn backend.app.main:app --host 0.0.0.0 --port $PORT
```

## 项目结构

```
project/
├── backend/
│   ├── app/
│   │   ├── main.py              # FastAPI 入口
│   │   ├── api/
│   │   │   └── routes.py        # API 路由
│   │   ├── core/
│   │   │   ├── config.py        # 全局配置
│   │   │   ├── logging.py       # 日志系统
│   │   │   └── exceptions.py    # 异常定义
│   │   ├── schemas/
│   │   │   └── models.py        # Pydantic 数据模型
│   │   ├── services/
│   │   │   ├── document_service.py   # 文档读取/归一化
│   │   │   ├── template_service.py   # 模板解析
│   │   │   ├── requirement_service.py # 需求解析
│   │   │   ├── retrieval_service.py   # 证据检索
│   │   │   ├── extraction_service.py  # 数据抽取
│   │   │   ├── fill_service.py        # 模板填充
│   │   │   ├── validation_service.py  # 结果验证
│   │   │   ├── pipeline_service.py    # 主流程编排
│   │   │   └── ollama_service.py      # LLM 服务
│   │   └── utils/
│   │       ├── json_repair.py   # JSON 修复
│   │       └── text_utils.py    # 文本工具
│   ├── tests/
│   │   └── test_basic.py        # 基础测试
│   └── requirements.txt
├── frontend/
│   ├── index.html               # 前端页面
│   ├── style.css                # 样式
│   └── app.js                   # 前端逻辑
├── config/
│   └── config.json              # 配置文件
├── 测试集/                       # 测试数据
├── uploads/                     # 上传文件临时存储
├── outputs/                     # 输出文件
├── logs/                        # 日志
├── start.sh                     # 启动脚本
└── README.md                    # 项目说明
```

## API 接口

| 方法 | 路径 | 说明 |
|------|------|------|
| GET/POST | `/api/health` | 健康检查 |
| POST | `/api/files/upload` | 上传数据源文件 |
| POST | `/api/templates/upload` | 上传模板文件 |
| POST | `/api/process` | 完整处理（上传+处理一体） |
| POST | `/api/process/local` | 处理服务器上的文件 |
| GET | `/api/sources/types` | 查看可用数据源类型 |
| POST | `/api/sources/preview` | 预览本地/API/网页/数据库数据源归一化结果 |
| POST | `/api/process/multisource` | 多源连接器 + 模板填充处理 |
| POST | `/api/document/operate` | 自然语言文档操作 |
| POST | `/api/document/extract` | 文档字段/内容提取 |
| POST | `/api/document/summarize` | 文档摘要 |
| GET | `/api/report/{task_id}` | 获取任务质量与融合报告 |
| GET | `/api/result/{task_id}` | 获取任务结果 |
| GET | `/api/download/{filename}` | 下载输出文件 |
| GET | `/api/outputs` | 列出所有输出文件 |

## 处理流程

1. **文档解析** - 读取所有源文件，归一化为统一中间表示
2. **模板解析** - 解析模板结构，识别表头、可写区域、占位符
3. **需求解析** - 解析用户要求，提取时间范围、实体关键词、过滤条件
4. **证据检索** - 基于规则和启发式匹配，从源文档中检索候选证据
5. **数据抽取** - 规则匹配 + LLM 辅助提取结构化数据
6. **模板填充** - 将抽取的数据写入模板，保留原始格式
7. **结果验证** - 验证填充率、完整性、证据链

## 配置说明

编辑 `config/config.json`：

```json
{
    "llm": {
        "ollama": {
            "api_base": "http://localhost:11434",
            "model_name": "qwen2.5:14b",
            "temperature": 0.3
        }
    }
}
```

## 运行实现流程

下面给出系统从接收到请求到最终输出的详细实现流程，便于排查与二次开发。

1. 启动与准备
     - 环境：Python 3.10+、Ollama（模型 qwen2.5:14b 已加载）、Linux。
     - 启动：推荐使用 `./start.sh`，或手动激活虚拟环境并运行：

         ```bash
         source venv/bin/activate
         python -m uvicorn backend.app.main:app --host 0.0.0.0 --port 8000
         ```

     - 启动后可访问：`http://localhost:8000/`（前端）、`http://localhost:8000/docs`（API 文档）、`http://localhost:8000/api/health`（健康检查）。

2. 请求入口（两种模式）
     - 前端/完整上传：用户在前端上传数据源和模板，前端通过表单提交到 `/api/process`（multipart/form-data），服务器会保存上传文件到 `uploads/`，然后启动处理任务并返回 task_id/结果。
     - 本地文件处理：如果服务器上已有文件，使用 `/api/process/local` 指定文件路径（或模板 ID）直接启动处理流程，常用于批量/自动化场景。

3. 处理主流程（Pipeline）——逐步说明
     - 文档解析（`document_service.py`）
         - 支持 Excel、Word、Markdown、TXT、CSV 等格式。
         - 将每个文件解析为标准中间表示 `DocumentBundle`：包含文本块（blocks）、表格、元数据、来源路径等。

     - 模板解析（`template_service.py`）
         - 解析模板文件，识别表头、占位符、可写区域和目标列名。
         - 生成填充目标结构（字段列表、行数推断、合并策略等）。

     - 需求解析（`requirement_service.py`）
         - 如果用户在前端提供了 `requirement`，则解析为结构化约束（时间范围、关键词、过滤条件）。
         - 如果为空，调用 `auto_load_requirement()`：先在模板目录查找 `用户要求.txt` 或 `requirement.txt`，如无则按文件名/上下文推断需求文本，返回 `auto_requirement` 字段并写入响应中。

     - 证据检索（`retrieval_service.py`）
         - 基于规则检索与启发式匹配，从每个 `DocumentBundle` 中挑选与需求/模板列最相关的 text blocks 和表格候选，返回 `RetrievalResult` 列表。
         - `RetrievalResult` 中包含 `source_docs`（对应 DocumentBundle），以便后续抽取访问全文上下文。

     - 数据抽取（`extraction_service.py`）
         - 混合策略：优先规则匹配（高置信度），对模糊或复杂的匹配使用 LLM 提取增强。
         - 多实体文档支持：当检测到“名单/排名/百强”等上下文时，采用分块（chunk）多次调用 LLM 的方式提取全部实体（例如每次处理 12 个文本块，调用时传入 `num_predict=2048` 以保证大输出），对每个 chunk 返回结构化行并合并、去重。
         - 多来源合并：对来自不同源（多个文件/表格）的候选结果进行合并，不再只取“最佳”候选，保留原始来源信息以便追溯。
         - 输出：每个结果包含 `rows`（提取到的数据行）、`col_confidence`（每列置信度字典）、`extraction_method`（`rule`/`llm`/`llm_multi`）、以及 `warnings`（如抽取行数异常提示）。

     - 模板填充（`fill_service.py`）
         - 读取模板解析结果，按行写入抽取数据。
         - 每个字段使用 `_get_field_confidence(field_name, value, col_confidence, extraction_method)` 计算最终置信度（规则匹配 > 精确匹配 > 包含匹配 > LLM 推断）。
         - 按模板格式（Excel/Word/CSV）保存结果文件到 `outputs/`，输出文件名包含时间戳与 task_id 以便追踪。

     - 结果验证（`validation_service.py`）
         - 检查填充率（填充字段数 / 目标字段数）、行数一致性、证据链完整性。
         - 新增异常检测：置信度均一性检测（若所有字段置信度相近且处于可疑区间如 0.40–0.85，添加警告），将 `warnings` 填入最终响应。
         - 将验证结果写入响应并记录到日志。

     - 响应与持久化
         - API 返回一个包含 `task_id`、`status`（`completed`/`failed`）、`auto_requirement`（如适用）、`results`（每个模板的填充详情）和 `warnings` 的 JSON。
         - 输出文件保存在 `outputs/`，可通过 `/api/download/{filename}` 下载；历史输出可通过 `/api/outputs` 列表。

4. LLM 细节（`ollama_service.py`）
     - 本地 Ollama 服务作为推理后端，使用配置文件 `config/config.json` 中的 `ollama.api_base` 与 `model_name`。
     - 所有 LLM 调用都支持传入 `num_predict` 参数：对大规模多实体抽取请使用更大的 `num_predict`（例如 2048），否则可能截断或漏项。
     - 建议将 `temperature` 保持在 0.0–0.3 以减小可重复性波动。

5. 日志与排错
     - 日志文件：`logs/` 目录包含服务运行日志（请求/错误/抽取详情）。
     - 常见问题：
         - Ollama 连接错误：确认 `ollama` 正在运行并且 `config/config.json` 中 `api_base` 正确。
         - 权限问题：确保 `uploads/`、`outputs/`、`logs/` 可写。
         - 置信度异常（全部 0.8/0.5 等）：检查 `fill_service` 是否被回退到默认策略，或查看 `extraction_service` 是否返回 `col_confidence`。

6. 示例命令（可复制）
     - 完整上传（multipart）：
         ```bash
         curl -X POST http://localhost:8000/api/process \\
             -F "source_files=@./测试集/包含模板文件/COVID-19数据集/COVID-19全球数据集（节选）.xlsx" \\
             -F "template_files=@./测试集/包含模板文件/COVID-19数据集/COVID-19 模板.xlsx" \\
             -F "requirement=" \\
             -F "use_llm=false"
         ```

     - 服务器本地文件处理：
         ```bash
         curl -s -X POST http://localhost:8000/api/process/local \\
             -H 'Content-Type: application/json' \\
             -d '{
                 "source_files": ["测试集/包含模板文件/COVID-19数据集/COVID-19全球数据集（节选）.xlsx"],
                 "template_files": ["测试集/包含模板文件/COVID-19数据集/COVID-19 模板.xlsx"],
                 "requirement": "",
                 "options": {"use_llm": false}
             }'
         ```

7. 开发者提示（可选调优）
     - 若处理百强/排名类长名单：提高 `extraction_service` 的 chunk_size 或 `ollama` 的 `num_predict`，并考虑分批上传以控制内存。
     - 单元测试：`backend/tests/test_basic.py` 包含基础流程回归测试，运行方法示例：
         ```bash
         cd backend
         python -m tests.test_basic
         ```

8. 目录说明（关键文件）
     - `backend/app/services/document_service.py`：文件解析与 DocumentBundle 构建
     - `backend/app/services/retrieval_service.py`：证据检索，输出包含 `source_docs`
     - `backend/app/services/extraction_service.py`：抽取逻辑（规则 + LLM，多来源合并）
     - `backend/app/services/fill_service.py`：模板填充与置信度计算
     - `backend/app/services/requirement_service.py`：需求解析与自动加载
     - `backend/app/services/ollama_service.py`：LLM 接口封装（支持 `num_predict`）

----

## 实时变更与验证日志（自动记录）

以下为最近一次交互中对系统的修改与验证记录（按时间倒序）：

- 2026-04-11 20:28:55 — 停止占用 8000 的旧进程并重启服务
    - 操作：使用 `kill <PID>` 停止原占用进程（示例：`kill 1060603`），随后在虚拟环境中重启服务：

        ```bash
        source venv/bin/activate
        python -m uvicorn backend.app.main:app --host 0.0.0.0 --port 8000
        ```

    - 启动信息（示例）：

        ```text
        INFO:     Started server process [1062309]
        INFO:     Uvicorn running on http://0.0.0.0:8000 (Press CTRL+C to quit)
        ```

    - 验证：健康检查 `/api/health` 返回：

        ```json
        {"status":"ok","ollama_status":"ok","model":"qwen2.5:14b","version":"1.0.0"}
        ```

- 2026-04-11 20:23:37–20:23:41 — 启动时遇到端口占用（address already in use）
    - 复现命令：

        ```bash
        python -m uvicorn backend.app.main:app --host 0.0.0.0 --port 8000
        python3 -m uvicorn backend.app.main:app --host 0.0.0.0 --port 8000
        ```

    - 现象：多次尝试启动时出现如下信息（示例）：

        ```text
        INFO:     Started server process [1062033]
        ERROR:    [Errno 98] error while attempting to bind on address ('0.0.0.0', 8000): address already in use
        ```

    - 排查命令（示例）：

        ```bash
        ss -ltnp | grep ':8000'
        ps -fp <PID>
        ```

    - 检查结果示例：`ss` 返回 `LISTEN ... users:(("python",pid=1060603,fd=7))`，说明 PID 1060603 的 `python` 进程占用了 8000 端口。

- API 端到端测试（已执行）
    - 测试命令（multipart 上传示例）：

        ```bash
        curl -X POST http://localhost:8000/api/process \\
            -F "source_files=@./测试集/包含模板文件/COVID-19数据集/COVID-19全球数据集（节选）.xlsx" \\
            -F "template_files=@./测试集/包含模板文件/COVID-19数据集/COVID-19 模板.xlsx" \\
            -F "requirement=" \\
            -F "use_llm=false"
        ```

    - 返回示例（从交互记录）：

        - `status: completed`
        - `auto_requirement`（截断）：

            > 请根据数据源文件（COVID-19全球数据集（节选））填写模板（COVID-19 模板 ）中的数据。自动填写所有可匹配字段，找不到的留空。

        - `fill_rate`: `96.28954577589253`
        - `rows_filled`: `15088`

- 调试命令参考
    - 查看占用端口的进程：

        ```bash
        ss -ltnp | grep ':8000'
        ```

    - 查看进程详情：

        ```bash
        ps -fp <PID>
        ```

    - 停止进程：

        ```bash
        kill <PID>
        # 或强制
        kill -9 <PID>
        ```

- 状态与备注
    - 当前服务已正常运行，可通过浏览器访问 `http://localhost:8000/` 与 `/docs`。
    - 如需将该日志写入版本控制（git commit），可由我代为提交并创建变更说明。

## 推荐演示路线（3-5 分钟）

> 以下步骤使用仓库自带测试集，全程可在浏览器完成，无需额外数据。

**前提：** 系统已启动，浏览器打开 http://localhost:8000/

### 第 1 步：确认系统状态（15秒）

- 页面左上角状态灯变绿，显示"后端可用: ollama / 模型 qwen2.5:14b"
- 或打开 http://localhost:8000/api/health 确认 `status: ok`

### 第 2 步：上传数据源（30秒）

在"📁 数据源"上传区，点击或拖拽上传：

```
测试集/包含模板文件/COVID-19数据集/COVID-19全球数据集（节选）.xlsx
```

### 第 3 步：上传模板并预览结构（30秒）

在"📋 模板文件"上传区，点击或拖拽上传：

```
测试集/包含模板文件/COVID-19数据集/COVID-19 模板.xlsx
```

点击"🔍 预览模板字段"按钮 → 界面展示模板字段列表和表格结构。

### 第 4 步：运行主填表流程（1-3分钟）

- 用户要求：留空（系统自动推断）
- 确保"启用 LLM"未勾选（用规则模式，更快）
- 点击"🚀 开始处理"
- 观察步骤进度条：Parse → Extract → Fill → Validate → Output
- 日志面板实时显示处理过程

### 第 5 步：查看结果与下载（30秒）

- 结果卡片展示：填充率（预期 >80%）、填充行数、质量报告
- 点击"⬇️ 下载"下载输出的 Excel 文件
- 底部"🏆 竞赛模式"面板：展示响应时间、是否达标

### 第 6 步：文档入库（30秒）

切换到"🗄️ 文档资产库"面板，点击上传区，选择：

```
测试集/txt/合肥市2024年国民经济和社会发展统计公报.txt
```

点击"📥 执行入库"，等待入库完成（约 5-10 秒）。

### 第 7 步：搜索关键词（15秒）

在搜索框输入"合肥"，点击"🔍 搜索" → 显示匹配的文档、文本块、实体。

### 第 8 步：文档摘要（15秒）

切换到"🤖 文档智能操作"面板：
- 上传同一 TXT 文件
- 操作类型选"summarize（摘要）"
- 点击"▶ 执行操作" → 展示摘要和要点

### 第 9 步：查看质量看板（15秒）

切换到"📊 数据质量看板"面板，点击"🔄 刷新看板" → 展示：
- 入库文档数、识别实体数、平均填充率
- 质量问题分布（缺失/重复/异常/冲突）
- 近期任务列表（填充率、响应时间）

---

## v2.0 新增功能（2026-04-24）

DocFusion v2.0 在原有填表引擎基础上新增以下模块：

### Module A — SQLite 文档资产库

所有处理过的文档（含实体、字段、质量问题）可入库至 `data/docfusion_store.sqlite`，支持 SHA256 去重和全文搜索。

新增 API：

| 端点 | 说明 |
|------|------|
| `POST /api/store/import/upload` | 上传文档并入库 |
| `GET /api/store/documents` | 文档列表 |
| `GET /api/store/search?q=` | 全文检索 |
| `GET /api/store/stats` | 数据库统计 |

### Module B — 文档智能操作前端

在"🤖 文档智能操作"面板中上传文档，使用自然语言指令执行摘要/提取/查找/替换/格式化操作。

### Module C — 多源数据接入前端

在"🌐 多源数据接入"面板中配置 HTTP/API、网页抓取、SQLite 等外部数据源，支持预览后加入处理。

### Module D — 模板字段增强

- `POST /api/templates/inspect/upload`：上传后预览模板字段
- `options.field_aliases`：配置字段别名映射
- `options.time_budget_seconds`：任务时间预算
- `options.store_extracted_data`：处理后自动入库

### Module E — 数据质量分析看板

`GET /api/analytics/dashboard` 返回全局统计、数据源类型分布、质量问题分布、近期任务列表。

### Module F — 竞赛评估模式

`POST /api/evaluate/compare` 将填写输出文件与标准答案对比，返回单元格准确率和行准确率（≥80% 为合格）。

---

## 运行测试

```bash
cd /home/match/LLM-DocFusion/backend
python3 -m pytest tests/ -v
```

测试覆盖（74 个测试，全部通过）：
- `tests/test_basic.py` — 核心逻辑（JSON修复、文档读取、模板解析、抽取、填充、验证等）
- `tests/test_store_service.py` — SQLite 文档资产库（入库、去重、搜索、质量问题）
- `tests/test_evaluation_service.py` — 评估服务（CSV/Excel 准确率对比）
- `tests/test_template_inspect.py` — 模板字段检视（CSV/Excel/TXT/MD）
- `tests/test_multisource_preview.py` — 多源预览（本地文件、错误处理、规则路径）
