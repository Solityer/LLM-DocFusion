"""Analytics and reporting API routes."""
from __future__ import annotations

import json
from collections import Counter
from typing import Any

from fastapi import APIRouter, HTTPException

from ..core.config import OUTPUT_DIR
from ..core.logging import logger
from ..services.task_service import get_task
from ..services.report_service import build_task_report

router = APIRouter(prefix="/api")


@router.get("/analytics/dashboard")
async def analytics_dashboard():
    """Return an aggregated analytics snapshot from the store and recent tasks."""
    from ..services.document_store_service import get_stats, get_quality_issues

    # Store stats
    try:
        store_stats = get_stats()
        store_data = store_stats.model_dump()
    except Exception as exc:
        logger.warning(f"analytics: store stats failed: {exc}")
        store_data = {}

    # Recent task metrics from task_metrics table
    try:
        from ..services.document_store_service import _get_connection, _DB_LOCK
        import threading
        with _DB_LOCK:
            conn = _get_connection()
            try:
                rows = conn.execute(
                    "SELECT * FROM task_metrics ORDER BY created_at DESC LIMIT 20"
                ).fetchall()
                task_metrics = [
                    {
                        "task_id": r["task_id"],
                        "fill_rate": r["fill_rate"],
                        "response_time": r["response_time"],
                        "quality_issue_count": r["quality_issue_count"],
                        "template_count": r["template_count"],
                        "source_count": r["source_count"],
                        "created_at": r["created_at"],
                    }
                    for r in rows
                ]
            finally:
                conn.close()
    except Exception as exc:
        logger.warning(f"analytics: task metrics failed: {exc}")
        task_metrics = []

    avg_fill_rate = (
        round(sum(t["fill_rate"] for t in task_metrics) / len(task_metrics), 2)
        if task_metrics else 0.0
    )
    avg_response_time = (
        round(sum(t["response_time"] for t in task_metrics) / len(task_metrics), 2)
        if task_metrics else 0.0
    )

    # Quality issues summary
    try:
        q_data = get_quality_issues(limit=1000)
        quality_type_dist = q_data.get("type_distribution", {})
        quality_sev_dist = q_data.get("severity_distribution", {})
    except Exception:
        quality_type_dist = {}
        quality_sev_dist = {}

    return {
        "status": "ok",
        "store": store_data,
        "quality_type_distribution": quality_type_dist,
        "quality_severity_distribution": quality_sev_dist,
        "recent_tasks": task_metrics,
        "avg_fill_rate": avg_fill_rate,
        "avg_response_time": avg_response_time,
        "meets_accuracy_threshold": avg_fill_rate >= 80.0,
        "meets_time_threshold": avg_response_time <= 90.0 or avg_response_time == 0.0,
    }


@router.get("/report/{task_id}/markdown")
async def get_report_markdown(task_id: str):
    """Generate a Markdown report for a task (for competition documentation reuse)."""
    task = get_task(task_id)
    if task is None:
        raise HTTPException(404, f"Task not found: {task_id}")

    report = build_task_report(task)
    md = _build_markdown_report(task, report)

    # Save to outputs
    md_path = OUTPUT_DIR / f"report_{task_id}.md"
    try:
        md_path.write_text(md, encoding="utf-8")
    except Exception:
        pass

    return {"task_id": task_id, "markdown": md, "report_file": str(md_path)}


def _build_markdown_report(task, report: dict) -> str:
    lines = [
        f"# DocFusion 任务报告 — {task.task_id}",
        "",
        "## 项目场景",
        "本系统为多源数据整合与质量识别应用系统，支持从文件、网页、API、数据库等多个来源",
        "获取非结构化/半结构化数据，通过字段统一、格式标准化、质量识别，自动填写 Word/Excel 模板。",
        "",
        "## 数据源列表",
    ]

    source_stats = [stat for result in (task.results or []) for stat in (result.source_stats or [])]
    if source_stats:
        lines.append("")
        lines.append("| 来源文件 | 类型 | 文本块 | 表格 | 抽取记录 | 贡献字段 |")
        lines.append("|---------|------|--------|------|---------|---------|")
        for stat in source_stats[:20]:
            name = (stat.source_file or "").split("/")[-1]
            lines.append(f"| {name} | {stat.file_type} | {stat.text_blocks} | {stat.tables} | {stat.extracted_records} | {stat.contributed_fields} |")
    else:
        lines.append("（无来源统计数据）")

    lines += [
        "",
        "## 数据整合流程",
        "1. **清理** — 清除旧输出文件",
        "2. **解析** — 读取所有来源文档，归一化为 DocumentBundle",
        "3. **模板解析** — 解析模板结构、字段、占位符",
        "4. **需求解析** — 解析用户要求或自动推断",
        "5. **证据检索** — 规则+LLM 对源表/文本段进行列对齐",
        "6. **数据抽取** — 混合策略：规则优先，LLM 兜底",
        "7. **模板填充** — 写回模板格式，保留原始格式",
        "8. **结果验证** — 检查填充率、证据链、实体合法性",
        "9. **输出** — 生成结果文件和质量报告",
        "",
        "## 字段统一与标准化逻辑",
        "- 字段别名通过 schema_registry.json 的 canonical_fields 映射",
        "- 数值：识别万、亿等中文单位，自动换算",
        "- 日期：支持多种格式（年月日、斜杠、中文），统一为 YYYY-MM-DD",
        "- 百分比：自动识别 % 符号并标准化为小数",
        "- 空值：识别 空、nan、—、NULL 等标记",
        "",
        "## 质量识别结果",
    ]

    q_issues = report.get("quality_issue_count", 0)
    q_dist = report.get("quality_issue_distribution", {})
    lines.append(f"- 质量问题总数：{q_issues}")
    for issue_type, count in q_dist.items():
        lines.append(f"  - {issue_type}：{count}")

    lines += [
        "",
        "## 填表结果",
        "",
        "| 模板 | 输出文件 | 填充率 | 填充行数 | 是否通过 |",
        "|------|---------|--------|---------|---------|",
    ]
    for result in (task.results or []):
        template_name = (result.template_file or "").split("/")[-1]
        output_name = (result.output_file or "").split("/")[-1]
        passed = "✅" if result.meets_minimum else "❌"
        lines.append(f"| {template_name} | {output_name} | {result.fill_rate:.1f}% | {result.rows_filled} | {passed} |")

    response_time = report.get("response_time")
    lines += [
        "",
        "## 响应时间",
        f"- 总耗时：{f'{response_time:.1f}s' if response_time else '未记录'}",
        f"- 竞赛要求：≤ 90 秒/模板",
        "",
        "## 模型调用情况",
    ]

    if task.model_usage:
        mu = task.model_usage
        lines += [
            f"- Provider：{mu.provider}",
            f"- 模型：{mu.model}",
            f"- 总调用数：{mu.total_calls}",
            f"- 成功调用：{mu.successful_calls}",
        ]
        if mu.per_stage:
            lines.append("- 各阶段调用：" + "、".join(f"{k}({v})" for k, v in mu.per_stage.items()))
    else:
        lines.append("- 未使用 LLM 或无调用记录")

    lines += [
        "",
        "## 应用价值",
        "",
        "### 经济效益",
        "- 自动化填表替代人工录入，单模板节省约 15-30 分钟人工时间",
        "- 多源数据整合避免重复查找和复制粘贴错误",
        "",
        "### 社会效益",
        "- 政府、企业数据报告标准化，提升数据质量",
        "- 减少因格式不规范导致的数据错误风险",
        "",
        "### 效率提升",
    ]

    total_docs = len({s.source_file for s in source_stats})
    total_rows = sum(r.rows_filled for r in (task.results or []))
    manual_time = total_docs * 5 + total_rows * 10 / 60  # minutes
    system_time = (response_time or 0) / 60

    lines += [
        f"- 文档数量：{total_docs}",
        f"- 总填充行数：{total_rows}",
        f"- 手工预估耗时：约 {manual_time:.0f} 分钟",
        f"- 系统实际耗时：约 {system_time:.1f} 分钟",
        f"- 节省时间：{max(0, manual_time - system_time):.0f} 分钟",
    ]

    return "\n".join(lines)
