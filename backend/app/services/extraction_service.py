"""Extraction service - rule-first extraction with optional LLM fallback."""
from __future__ import annotations

import hashlib
import re
from collections import Counter
from pathlib import Path
from typing import Any

from ..core.logging import logger
from ..schemas.models import CandidateEvidence, DocumentBundle, NormalizedTable, RequirementSpec, TemplateSchema
from ..utils.entity_utils import describe_entity_reason, evaluate_entity_compatibility, validate_entity_value
from ..utils.text_utils import best_column_match, clean_cell_value, similarity, truncate_text
from .ollama_service import get_ollama_service
from .retrieval_service import RetrievalResult


LIST_LIKE_TITLE_PATTERNS = (
    r'(?:top|TOP|前)\s*\d{1,4}',
    r'\d{1,4}\s*(?:强|名|项|个|位|条)',
    r'(?:排名|排行|榜单|名录|名单|清单)',
)
LIST_ITEM_PATTERN = r'^\s*(?:\d+[.)、]|[一二三四五六七八九十]+、|[-*•])\s+'
NUMBER_PATTERN = r'[-+]?\d[\d,]*(?:\.\d+)?'
DATE_PATTERN = (
    r'(?:19\d{2}|20\d{2}|2100)'
    r'(?:[-/.年](?:1[0-2]|0?[1-9]))'
    r'(?:[-/.月](?:3[01]|[12]\d|0?[1-9]))?'
    r'(?:[日号])?'
    r'(?:\s+\d{1,2}:\d{2}(?::\d{2}(?:\.\d+)?)?)?'
)
ENTITY_SUFFIXES = ("市", "省", "自治区", "特别行政区", "自治州", "地区", "盟", "县", "区")
GENERIC_SEGMENT_PREFIXES = ("一、", "二、", "三、", "四、", "五、", "六、", "七、", "八、", "九、", "十、", "结语")
GENERIC_TOPIC_TOKENS = {
    "数据", "信息", "报告", "模板", "情况", "要求", "说明", "结果", "来源",
    "统计", "表格", "字段", "指标", "文件", "任务", "内容", "分析",
    "中国", "全国", "全省", "全市", "城市", "地区", "国家", "省份",
}
GENERIC_FILTER_KEYS = {"时间", "日期", "年份", "月份", "城市", "地区", "国家", "名称", "类型", "类别"}
GENERIC_HEADER_TERMS = {
    "城市", "城市名", "城市名称", "国家", "地区", "国家地区", "名称", "站点名称", "区", "省", "省份",
    "日期", "时间", "监测时间", "单位", "单位名称",
}
NORMALIZED_GENERIC_HEADER_TERMS = {
    re.sub(r'[\s_\-:：,，、/\\()（）\[\]【】]+', '', item.lower())
    for item in GENERIC_HEADER_TERMS
}
GENERIC_TOPIC_SUBSTRINGS = {
    "模板", "报告", "数据", "信息", "统计", "公报", "情况", "说明", "结果", "来源",
    "sheet", "table", "doc", "xlsx", "docx", "txt", "md",
}
GENERIC_ENTITY_BLACKLIST_PARTS = {
    "概览", "报告", "公报", "模板", "数据", "信息", "情况", "统计", "全景", "分析", "汇总", "监测",
}
GENERIC_HEADING_LINE_PARTS = GENERIC_ENTITY_BLACKLIST_PARTS | {"简报", "综述", "概况", "导语"}
TOPIC_DOMAIN_KEYWORDS = {
    "economy": {"gdp", "生产总值", "经济", "财政", "预算", "收入", "支出", "人口", "人均", "工业", "投资"},
    "air_quality": {"aqi", "空气", "环境", "质量", "pm2", "pm10", "so2", "no2", "co", "o3", "污染"},
    "epidemic": {"covid", "疫情", "确诊", "死亡", "康复", "病例", "感染", "检测", "新冠"},
}
NUMERIC_HEADER_CONCEPTS = {
    "metric_gdp_total",
    "metric_population",
    "metric_gdp_per_capita",
    "metric_budget_revenue",
    "metric_budget_expenditure",
    "metric_cases",
    "metric_tests",
    "metric_aqi",
    "metric_pm10",
    "metric_pm2_5",
    "generic_numeric",
}
KNOWN_CONTINENT_VALUES = {
    "asia", "亚洲",
    "europe", "欧洲",
    "africa", "非洲",
    "north america", "北美洲",
    "south america", "南美洲",
    "oceania", "大洋洲",
    "antarctica", "南极洲",
}


def extract_data(
    retrieval: RetrievalResult,
    template: TemplateSchema,
    requirement: RequirementSpec,
    use_llm: bool = True,
    usage_context: dict[str, Any] | None = None,
) -> list[dict]:
    """Extract structured records for each template table."""
    results = []
    for tmpl_table in template.tables:
        template_context = _build_template_table_context(template, tmpl_table, requirement)
        results.append(
            _extract_for_table(
                tmpl_table,
                retrieval,
                requirement,
                use_llm,
                usage_context=usage_context,
                template_context=template_context,
            )
        )
    return results


def _extract_for_table(
    tmpl_table,
    retrieval: RetrievalResult,
    requirement: RequirementSpec,
    use_llm: bool,
    usage_context: dict[str, Any] | None = None,
    template_context: dict[str, Any] | None = None,
) -> dict:
    """Extract data for a single template table."""
    ollama = get_ollama_service()
    headers = [header.strip() for header in tmpl_table.headers if header and header.strip()]
    matching = [
        candidate for candidate in retrieval.table_candidates
        if candidate["template_table"].table_index == tmpl_table.table_index
    ]

    records: list[dict] = []
    warnings: list[str] = []
    table_evidence: list[CandidateEvidence] = []
    raw_source_counts = Counter({doc.source_file: 0 for doc in retrieval.source_docs})
    filtered_source_counts = Counter({doc.source_file: 0 for doc in retrieval.source_docs})
    entity_block_counts = Counter({doc.source_file: 0 for doc in retrieval.source_docs})
    narrative_stage_audit = _empty_narrative_stage_audit(retrieval.source_docs)
    narrative_record_registry = _empty_narrative_record_registry(retrieval.source_docs)
    filter_diagnostics = _empty_filter_diagnostics()
    candidate_row_estimates: list[int] = []
    extraction_method = "none"
    relevant_narrative_docs = [
        document
        for document in retrieval.source_docs
        if _is_narrative_source(document)
        and _source_matches_template_context(
            document,
            headers,
            requirement,
            template_context=template_context,
        )
    ]
    relevant_source_files = {
        candidate["evidence"].source_file
        for candidate in matching
        if candidate.get("evidence") and candidate["evidence"].source_file
    }
    relevant_source_files.update(document.source_file for document in relevant_narrative_docs)
    invalidated_sources: set[str] = set()

    for document in retrieval.source_docs:
        if not _is_narrative_source(document):
            continue
        raw_segments = _raw_text_segments(document)
        blocks = _group_segments_into_entity_blocks(raw_segments)
        if blocks:
            entity_block_counts[document.source_file] = sum(
                1
                for block in blocks
                if _segment_is_relevant(
                    block["segment"],
                    headers,
                    requirement,
                    template_context=template_context,
                )
            )
        else:
            entity_block_counts[document.source_file] = sum(
                1
                for segment, _location, _heading_level in raw_segments
                if _segment_is_relevant(
                    segment,
                    headers,
                    requirement,
                    template_context=template_context,
                )
            )
        if document.source_file in narrative_stage_audit:
            narrative_stage_audit[document.source_file]["relevant_segments"] = entity_block_counts[document.source_file]

    for candidate in matching:
        src_table: NormalizedTable = candidate["source_table"]
        src_file = candidate["evidence"].source_file
        src_name = Path(src_file).name if src_file else "unknown"
        filtered_rows = _apply_table_spec_filter(
            src_table,
            candidate["filtered_rows"],
            tmpl_table.table_index,
            requirement,
        )
        logger.info("    source '%s': %s rows matched", src_name, len(filtered_rows))
        candidate_row_estimates.append(len(filtered_rows))

        if not filtered_rows:
            warn = f"source {src_name} 对模板表 {tmpl_table.table_index} 未产出记录"
            warnings.append(warn)
            logger.warning("    [WARN] %s", warn)
            continue

        table_evidence.append(candidate["evidence"])
        for row_index, src_row in enumerate(filtered_rows):
            record = _build_rule_record(headers, candidate, src_row, row_index)
            if any(record["values"].values()):
                records.append(record)
                raw_source_counts[src_file] += 1

    if records:
        extraction_method = "rule"

    if relevant_narrative_docs:
        if use_llm and not ollama.is_available:
            ollama.note_skip(
                "相关 Word/docx/叙事 source 命中模板，但 Ollama/qwen 不可用",
                {
                    **(usage_context or {}),
                    "stage": "extract",
                    "source_files": [document.source_file for document in relevant_narrative_docs],
                },
            )
            raise RuntimeError("检测到必须走 qwen 的叙事/非结构化抽取，但本地 Ollama qwen2.5:14b 不可用")

        text_records, text_warnings, text_source_counts, text_evidence = _extract_text_records(
            headers=headers,
            retrieval=retrieval,
            requirement=requirement,
            candidate_documents=relevant_narrative_docs,
            template_context=template_context,
        )
        _register_narrative_records(narrative_record_registry, text_records, origin="rule")
        _update_narrative_stage_audit(
            narrative_stage_audit,
            _raw_record_source_counts(text_records, retrieval),
            "rule_records",
        )
        warnings.extend(text_warnings)
        table_evidence.extend(text_evidence)
        raw_source_counts.update(text_source_counts)
        stable_text_records, suspicious_text_records, quality_warnings = _split_narrative_records_by_quality(
            headers,
            text_records,
        )
        _mark_narrative_record_quality(narrative_record_registry, stable_text_records, quality="stable")
        _mark_narrative_record_quality(narrative_record_registry, suspicious_text_records, quality="suspicious")
        _update_narrative_stage_audit(
            narrative_stage_audit,
            _raw_record_source_counts(stable_text_records, retrieval),
            "stable_records",
        )
        _update_narrative_stage_audit(
            narrative_stage_audit,
            _raw_record_source_counts(suspicious_text_records, retrieval),
            "suspicious_records",
        )
        warnings.extend(quality_warnings)
        if stable_text_records:
            records.extend(stable_text_records)
            if extraction_method == "none":
                extraction_method = "text_rule"
            elif "text_rule" not in extraction_method:
                extraction_method = f"{extraction_method}+text_rule"
        if use_llm and relevant_narrative_docs:
            llm_segment_limit = 8 if suspicious_text_records or not stable_text_records else 4
            llm_max_chunks = 2 if suspicious_text_records or not stable_text_records else 1
            if suspicious_text_records or not stable_text_records:
                llm_text_records, llm_warnings, llm_source_counts, llm_evidence = _extract_llm_text_records(
                    headers=headers,
                    retrieval=retrieval,
                    requirement=requirement,
                    use_llm=use_llm,
                    usage_context=usage_context,
                    candidate_documents=relevant_narrative_docs,
                    require_llm=True,
                    template_context=template_context,
                    segment_limit=llm_segment_limit,
                    max_chunks=llm_max_chunks,
                )
                warnings.extend(llm_warnings)
                table_evidence.extend(llm_evidence)
                if llm_text_records:
                    _register_narrative_records(narrative_record_registry, llm_text_records, origin="qwen", quality="qwen")
                    records.extend(llm_text_records)
                    raw_source_counts.update(llm_source_counts)
                    _update_narrative_stage_audit(
                        narrative_stage_audit,
                        llm_source_counts,
                        "llm_records",
                    )
                    extraction_method = "qwen_text" if extraction_method == "none" else f"{extraction_method}+qwen_text"
            else:
                _probe_source_coverage(
                    headers=headers,
                    retrieval=retrieval,
                    requirement=requirement,
                    usage_context=usage_context,
                    template_context=template_context,
                    require_llm=True,
                )
            if usage_context is not None:
                usage_context["_source_probe_done"] = True

    if records and use_llm and not bool((usage_context or {}).get("_source_probe_done")):
        _probe_source_coverage(
            headers=headers,
            retrieval=retrieval,
            requirement=requirement,
            usage_context=usage_context,
            template_context=template_context,
        )
        if usage_context is not None:
            usage_context["_source_probe_done"] = True

    if not records:
        llm_text_records, llm_warnings, llm_source_counts, llm_evidence = _extract_llm_text_records(
            headers=headers,
            retrieval=retrieval,
            requirement=requirement,
            use_llm=use_llm,
            usage_context=usage_context,
            template_context=template_context,
        )
        warnings.extend(llm_warnings)
        table_evidence.extend(llm_evidence)
        if llm_text_records:
            _register_narrative_records(narrative_record_registry, llm_text_records, origin="qwen", quality="qwen")
            records.extend(llm_text_records)
            raw_source_counts.update(llm_source_counts)
            _update_narrative_stage_audit(
                narrative_stage_audit,
                llm_source_counts,
                "llm_records",
            )
            extraction_method = "qwen_text"

        text_records, text_warnings, text_source_counts, text_evidence = _extract_text_records(
            headers=headers,
            retrieval=retrieval,
            requirement=requirement,
            template_context=template_context,
        )
        _update_narrative_stage_audit(
            narrative_stage_audit,
            _raw_record_source_counts(text_records, retrieval),
            "rule_records",
        )
        warnings.extend(text_warnings)
        table_evidence.extend(text_evidence)
        if text_records:
            records.extend(text_records)
            raw_source_counts.update(text_source_counts)
            if extraction_method == "qwen_text":
                extraction_method = "qwen_text+text_rule"
            else:
                extraction_method = "hybrid_rule" if extraction_method == "rule" else "text_rule"

    records = _deduplicate_records(records, headers)
    records = _merge_records_by_semantic_key(
        records,
        headers,
        requirement,
        use_llm=use_llm,
        usage_context=usage_context,
    )
    records, context_filter_diagnostics = _filter_records_by_template_context(records, template_context, warnings)
    filter_diagnostics = _merge_filter_diagnostics(filter_diagnostics, context_filter_diagnostics)
    records = _normalize_records(headers, records)
    records = _annotate_records_with_entity_semantics(headers, records, template_context)
    records, invalidated_sources, filter_diagnostics = _filter_records_by_entity_legality(
        headers,
        records,
        warnings,
        template_context=template_context,
    )
    _update_narrative_stage_audit(
        narrative_stage_audit,
        _record_source_counts(records, retrieval),
        "post_entity_records",
    )
    # Dedup again after normalization - but only for records that are not anchored
    # to a concrete table row. Narrative/LLM records may normalize into duplicates,
    # while structured rows must remain distinct even when the template hides dates.
    records = _dedup_narrative_only(records, headers)
    filtered_source_counts = _record_source_counts(records, retrieval)
    entity_estimate = _estimate_entity_count(
        retrieval,
        candidate_row_estimates,
        records,
        relevant_source_files=relevant_source_files,
    )

    if _needs_llm_backfill(records, entity_estimate, retrieval, headers, use_llm):
        if _is_multi_entity_context(retrieval, headers):
            llm_rows = _llm_extract_multi_entity(
                headers,
                retrieval,
                requirement,
                usage_context=usage_context,
                template_context=template_context,
            )
            llm_method = "qwen_multi"
        else:
            llm_rows = _llm_extract_single(
                headers,
                retrieval,
                requirement,
                usage_context=usage_context,
                template_context=template_context,
            )
            llm_method = "qwen"
        llm_records = _rows_to_llm_records(headers, llm_rows, retrieval, llm_method)
        before_count = len(records)
        records = _deduplicate_records(records + llm_records, headers)
        records, context_filter_diagnostics = _filter_records_by_template_context(records, template_context, warnings)
        filter_diagnostics = _merge_filter_diagnostics(filter_diagnostics, context_filter_diagnostics)
        records = _normalize_records(headers, records)
        records = _annotate_records_with_entity_semantics(headers, records, template_context)
        records, extra_invalidated_sources, extra_filter_diagnostics = _filter_records_by_entity_legality(
            headers,
            records,
            warnings,
            template_context=template_context,
        )
        invalidated_sources.update(extra_invalidated_sources)
        filter_diagnostics = _merge_filter_diagnostics(filter_diagnostics, extra_filter_diagnostics)
        raw_source_counts.update(
            Counter(record.get("source_file", "") for record in llm_records if record.get("source_file"))
        )
        if len(records) > before_count:
            extraction_method = f"{extraction_method}+{llm_method}" if extraction_method not in {"none", llm_method} else llm_method
        records = _deduplicate_records(records, headers)
        filtered_source_counts = _record_source_counts(records, retrieval)

    records = _apply_ranking_limit(records, retrieval, requirement, warnings, template_context=template_context)
    filtered_source_counts = _record_source_counts(records, retrieval)
    _update_narrative_stage_audit(
        narrative_stage_audit,
        filtered_source_counts,
        "final_records",
    )
    rows = [[record["values"].get(header, "") for header in headers] for record in records]
    col_confidence = _aggregate_column_confidence(records, headers)

    if entity_estimate > 1 and len(records) <= 1:
        warn = (
            f"多实体文档信号明显，但模板表 {tmpl_table.table_index} 仅抽取出 {len(records)} 条记录，"
            "可能存在实体切块不足或字段映射不完整"
        )
        warnings.append(warn)
        logger.warning("    [WARN] %s", warn)

    if rows and not table_evidence:
        warn = f"模板表 {tmpl_table.table_index} 有值但缺少表级 evidence"
        warnings.append(warn)
        logger.warning("    [WARN] %s", warn)

    confidence_values = [value for value in col_confidence.values() if value is not None]
    if len(confidence_values) >= 4:
        rounded = {round(v, 2) for v in confidence_values}
        spread = max(confidence_values) - min(confidence_values)
        if len(rounded) <= 2 or spread < 0.04:
            warn = (
                f"模板表 {tmpl_table.table_index} 的列级 confidence 过于集中，"
                f"范围 {min(confidence_values):.2f}-{max(confidence_values):.2f}"
            )
            warnings.append(warn)
            logger.warning("    [WARN] %s", warn)

    evidence = _collect_table_evidence(records, table_evidence)

    return {
        "table_index": tmpl_table.table_index,
        "headers": headers,
        "rows": rows,
        "records": records,
        "col_mapping": matching[0]["col_mapping"] if matching else {},
        "col_confidence": col_confidence,
        "extraction_method": extraction_method,
        "evidence": evidence,
        "warnings": warnings,
        "source_counts": dict(raw_source_counts),
        "filtered_source_counts": dict(filtered_source_counts),
        "entity_block_counts": dict(entity_block_counts),
        "narrative_stage_audit": narrative_stage_audit,
        "entity_estimate": entity_estimate,
        "relevant_source_files": sorted(relevant_source_files),
        "invalidated_source_files": sorted(invalidated_sources),
        "filter_diagnostics": filter_diagnostics,
        "narrative_record_registry": _serialize_narrative_record_registry(narrative_record_registry),
        "template_context": template_context or {},
    }


def _extract_text_records(
    headers: list[str],
    retrieval: RetrievalResult,
    requirement: RequirementSpec,
    candidate_documents: list[DocumentBundle] | None = None,
    template_context: dict[str, Any] | None = None,
) -> tuple[list[dict], list[str], Counter, list[CandidateEvidence]]:
    """Extract record-by-record data from narrative text blocks."""
    records: list[dict] = []
    warnings: list[str] = []
    source_counts = Counter({doc.source_file: 0 for doc in retrieval.source_docs})
    evidence: list[CandidateEvidence] = []

    documents = candidate_documents or retrieval.source_docs
    for document in documents:
        if document.tables and len(document.text_blocks) <= 1:
            continue
        source_profile = _build_narrative_source_profile(document, headers)
        for segment, location in _candidate_text_segments(document):
            if not _segment_is_relevant(segment, headers, requirement, template_context=template_context):
                continue
            record = _build_text_record(
                headers,
                segment,
                document.source_file,
                location,
                source_profile=source_profile,
            )
            if record is None:
                continue
            records.append(record)
            source_counts[document.source_file] += 1
            first_evidence = next(
                (
                    ev
                    for evs in record["field_evidence"].values()
                    for ev in evs[:1]
                ),
                None,
            )
            if first_evidence:
                evidence.append(first_evidence)

    if _is_multi_entity_context(retrieval, headers) and len(records) <= 1:
        warnings.append("文本规则抽取记录偏少，已准备交由 LLM 做补充判别")

    return records, warnings, source_counts, evidence


def _split_narrative_records_by_quality(
    headers: list[str],
    records: list[dict],
) -> tuple[list[dict], list[dict], list[str]]:
    """Keep strong narrative rule records and send suspicious ones to qwen refinement."""
    stable: list[dict] = []
    suspicious: list[dict] = []
    warnings: list[str] = []

    for record in records:
        reasons = _narrative_record_quality_reasons(headers, record)
        if reasons:
            record["quality_flags"] = reasons
            suspicious.append(record)
            continue
        stable.append(record)

    if suspicious:
        warnings.append(
            f"叙事规则抽取中有 {len(suspicious)} 条候选记录质量可疑，已交由 qwen 做定向语义复核"
        )
    return stable, suspicious, warnings


def _narrative_record_quality_reasons(headers: list[str], record: dict) -> list[str]:
    """Flag likely-cross-contaminated or semantically invalid narrative rule records."""
    reasons: list[str] = []
    values = record.get("values", {})
    filled_count = 0
    for header in headers:
        value = clean_cell_value(values.get(header, ""))
        if not value:
            continue
        filled_count += 1
        concept = _header_concept(header)
        if concept.startswith("entity"):
            legal, reason = validate_entity_value(value, header, peer_headers=headers)
            if not legal:
                # region_field_country_only is a granularity mismatch, not an entity error;
                # preserve sub-national records as stable so they are not collapsed by LLM
                if reason != "region_field_country_only":
                    reasons.append(f"entity:{reason or 'illegal'}")
                continue
        elif concept == "metric_continent":
            if not _continent_value_is_valid(value):
                reasons.append("invalid_continent")
                continue
        elif concept == "date":
            _normalized, status = _normalize_date_value(value)
            if status == "as_is":
                reasons.append("invalid_date")
                continue
        elif concept in NUMERIC_HEADER_CONCEPTS:
            normalized, _status = _normalize_numeric_value(header, value)
            if not re.fullmatch(rf"{NUMBER_PATTERN}", normalized):
                reasons.append("invalid_numeric")
                continue
            try:
                numeric_value = float(normalized.replace(",", ""))
            except ValueError:
                reasons.append("invalid_numeric")
                continue
            if concept == "metric_population" and numeric_value < 10:
                reasons.append("population_too_small")
            elif concept == "metric_tests" and numeric_value < 1:
                reasons.append("tests_too_small")

    completeness = filled_count / max(len(headers), 1)
    if completeness < 0.45:
        reasons.append("low_completeness")
    return reasons


def _continent_value_is_valid(value: str) -> bool:
    """Validate continent-like values after lightweight normalization."""
    normalized = clean_cell_value(value).lower()
    normalized = normalized.replace("（", "(").replace("）", ")")
    normalized = re.sub(r"[^a-z\u4e00-\u9fa5 ]+", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    if not normalized:
        return False
    if normalized in KNOWN_CONTINENT_VALUES:
        return True
    if "asia" in normalized or "亚洲" in normalized:
        return True
    if "europe" in normalized or "欧洲" in normalized:
        return True
    if "africa" in normalized or "非洲" in normalized:
        return True
    if "america" in normalized or "美洲" in normalized:
        return True
    if "oceania" in normalized or "大洋洲" in normalized:
        return True
    return False


def _extract_llm_text_records(
    headers: list[str],
    retrieval: RetrievalResult,
    requirement: RequirementSpec,
    use_llm: bool,
    usage_context: dict[str, Any] | None = None,
    candidate_documents: list[DocumentBundle] | None = None,
    require_llm: bool = False,
    template_context: dict[str, Any] | None = None,
    segment_limit: int | None = None,
    max_chunks: int | None = None,
) -> tuple[list[dict], list[str], Counter, list[CandidateEvidence]]:
    """Use qwen on segmented narrative sources before falling back to pure rules."""
    warnings: list[str] = []
    records: list[dict] = []
    evidence: list[CandidateEvidence] = []
    source_counts = Counter({doc.source_file: 0 for doc in retrieval.source_docs})
    ollama = get_ollama_service()
    llm_context = {
        **(usage_context or {}),
        "stage": "extract",
        "source_files": [doc.source_file for doc in (candidate_documents or retrieval.source_docs)],
    }

    if not use_llm:
        ollama.note_skip("LLM 增强关闭，叙事文本改用规则抽取", llm_context)
        return records, warnings, source_counts, evidence
    if not ollama.is_available:
        ollama.note_skip("Ollama/qwen 不可用，叙事文本改用规则抽取", llm_context)
        if require_llm:
            raise RuntimeError("必须走 qwen 的叙事/非结构化抽取阶段不可用")
        warnings.append("qwen 不可用，叙事文本已回退到规则抽取")
        return records, warnings, source_counts, evidence

    narrative_docs = [
        document for document in (candidate_documents or retrieval.source_docs)
        if _is_narrative_source(document)
    ]
    scored_narrative_docs = [
        (document, _source_context_score(document, headers, requirement, template_context=template_context))
        for document in narrative_docs
    ]
    relevant_narrative_docs = [
        (document, score)
        for document, score in scored_narrative_docs
        if score >= 3
    ]
    relevant_narrative_docs.sort(key=lambda item: item[1], reverse=True)
    if relevant_narrative_docs:
        narrative_docs = [document for document, _ in relevant_narrative_docs[: min(4, len(relevant_narrative_docs))]]
    if not narrative_docs:
        ollama.note_skip("当前模板未命中文本型 source，未触发 qwen 叙事抽取", llm_context)
        return records, warnings, source_counts, evidence

    qwen_called = False
    for document in narrative_docs:
        if require_llm:
            ollama.mark_required_call(
                "叙事/Word/docx 语义抽取必须真正经过 qwen",
                {
                    **(usage_context or {}),
                    "stage": "extract",
                    "source_file": document.source_file,
                },
            )
        multi_entity = _is_multi_entity_context(retrieval, headers)
        ranked_segments = _top_relevant_source_segments(
            document,
            headers,
            requirement,
            limit=segment_limit or (12 if multi_entity else 8),
            template_context=template_context,
        )
        if not ranked_segments:
            continue
        group_size = 4 if multi_entity else 4
        chunk_budget = max_chunks or (3 if multi_entity else 2)
        for chunk_index in range(0, min(len(ranked_segments), group_size * chunk_budget), group_size):
            chunk = ranked_segments[chunk_index: chunk_index + group_size]
            if not chunk:
                continue
            qwen_called = True
            rows = _llm_extract_from_segments(
                headers=headers,
                requirement=requirement,
                source_file=document.source_file,
                chunk=chunk,
                usage_context={
                    **(usage_context or {}),
                    "stage": "extract",
                    "source_file": document.source_file,
                },
            )
            location = f"{chunk[0][1]}..{chunk[-1][1]}"
            snippet = "\n".join(segment for segment, _, _ in chunk)[:260]
            chunk_records = _rows_to_llm_records_from_source(
                headers=headers,
                rows=rows,
                source_file=document.source_file,
                location=location,
                raw_snippet=snippet,
                method="qwen_narrative",
            )
            if chunk_records:
                records.extend(chunk_records)
                source_counts[document.source_file] += len(chunk_records)
                evidence.append(CandidateEvidence(
                    source_file=document.source_file,
                    location=location,
                    raw_snippet=snippet,
                    match_reason="qwen 叙事分块抽取",
                    confidence=0.62,
                ))

    if not qwen_called:
        ollama.note_skip("没有可用于 qwen 的高相关叙事分段，改用规则抽取", llm_context)
    if narrative_docs and not records:
        warnings.append("qwen 未从叙事文本中抽出足够记录，已继续使用规则抽取")
    return records, warnings, source_counts, evidence


def _candidate_text_segments(document: DocumentBundle) -> list[tuple[str, str]]:
    """Split a document into entity-aware narrative blocks."""
    raw_segments = _raw_text_segments(document)
    blocks = _group_segments_into_entity_blocks(raw_segments)
    if not blocks:
        return [(segment, location) for segment, location, _ in raw_segments]
    return [(block["segment"], block["location"]) for block in blocks]


def _build_narrative_source_profile(document: DocumentBundle, headers: list[str]) -> dict[str, Any]:
    """Infer one document-level scope so supplemental narrative segments can merge conservatively."""
    segments = _candidate_text_segments(document)[:18]
    if not segments:
        return {}

    profiled_headers = [
        header for header in headers
        if _header_concept(header).startswith("entity")
        or _header_concept(header) in {"date", "metric_continent"}
    ]
    scope_values: dict[str, str] = {}
    scope_confidence: dict[str, float] = {}
    scope_evidence: dict[str, list[CandidateEvidence]] = {}

    for header in profiled_headers:
        extracted = _extract_profiled_scope_value(header, segments, document.source_file)
        if not extracted:
            continue
        value, location, snippet, reason, confidence = extracted
        scope_values[header] = value
        scope_confidence[header] = confidence
        scope_evidence[header] = [CandidateEvidence(
            source_file=document.source_file,
            location=location,
            raw_snippet=snippet,
            match_reason=reason,
            confidence=confidence,
        )]

    entity_headers = [header for header in profiled_headers if _header_concept(header).startswith("entity")]
    primary_entity_header = next((header for header in entity_headers if scope_values.get(header)), "")
    primary_entity_value = clean_cell_value(scope_values.get(primary_entity_header, ""))
    primary_entity_key = _normalize_topic_text(primary_entity_value)
    subordinate_entities = sorted({
        entity
        for segment, _location in segments[1:]
        for entity in [_extract_leading_entity(segment)]
        if entity and _normalize_topic_text(entity) and _normalize_topic_text(entity) != primary_entity_key
    })
    scope_date = next(
        (scope_values.get(header, "") for header in profiled_headers if _header_concept(header) == "date"),
        "",
    )
    scope_signature_parts = [
        primary_entity_header,
        primary_entity_value,
        clean_cell_value(scope_date),
    ]
    return {
        "scope_values": scope_values,
        "scope_confidence": scope_confidence,
        "scope_evidence": scope_evidence,
        "primary_entity_header": primary_entity_header,
        "primary_entity_value": primary_entity_value,
        "scope_date": clean_cell_value(scope_date),
        "subordinate_entities": subordinate_entities,
        "supports_entity_remap": bool(primary_entity_value and subordinate_entities),
        "scope_signature": "|".join(part for part in scope_signature_parts if part),
    }


def _extract_profiled_scope_value(
    header: str,
    segments: list[tuple[str, str]],
    source_file: str,
) -> tuple[str, str, str, str, float] | None:
    """Find the strongest early-scope value for one narrative header."""
    del source_file
    best: tuple[float, str, str, str, str] | None = None
    for rank, (segment, location) in enumerate(segments[:6]):
        value, match_method, reason = _extract_value_for_header(header, segment)
        if not value:
            continue
        confidence = 0.66 - min(rank, 4) * 0.04
        if match_method in {"text_context", "text_exact"}:
            confidence += 0.08
        elif match_method == "entity_lead":
            confidence += 0.03
        score = confidence + (0.06 if rank == 0 else 0.0)
        candidate = (score, value, location, segment[:220], reason)
        if best is None or candidate[0] > best[0]:
            best = candidate
        if rank <= 1 and match_method in {"text_context", "text_exact"}:
            break
    if not best:
        return None
    score, value, location, snippet, reason = best
    return value, location, snippet, reason, round(max(0.52, min(0.82, score)), 4)


def _clone_evidence_list(evidence_list: list[CandidateEvidence]) -> list[CandidateEvidence]:
    """Clone evidence objects before reusing them across merged/remapped records."""
    return [CandidateEvidence(
        source_file=item.source_file,
        location=item.location,
        raw_snippet=item.raw_snippet,
        match_reason=item.match_reason,
        confidence=item.confidence,
    ) for item in evidence_list]


def _apply_narrative_scope_defaults(
    headers: list[str],
    values: dict[str, str],
    field_confidence: dict[str, float | None],
    field_evidence: dict[str, list[CandidateEvidence]],
    match_methods: dict[str, str],
    source_profile: dict[str, Any] | None,
):
    """Fill blank scope fields from document-level narrative context without fabricating facts."""
    if not source_profile:
        return
    scope_values = source_profile.get("scope_values", {})
    scope_confidence = source_profile.get("scope_confidence", {})
    scope_evidence = source_profile.get("scope_evidence", {})
    entity_headers = [header for header in headers if _header_concept(header).startswith("entity")]
    has_entity_value = any(values.get(header) for header in entity_headers)

    for header in headers:
        if values.get(header):
            continue
        scope_value = clean_cell_value(scope_values.get(header, ""))
        if not scope_value:
            continue
        concept = _header_concept(header)
        if concept.startswith("entity") and has_entity_value:
            continue
        if concept not in {"date", "metric_continent"} and not concept.startswith("entity"):
            continue
        values[header] = scope_value
        field_confidence[header] = scope_confidence.get(header)
        field_evidence[header] = _clone_evidence_list(scope_evidence.get(header, []))
        match_methods[header] = "doc_scope"


def _narrative_temporal_scope(segment: str, source_profile: dict[str, Any] | None) -> str:
    """Attach a conservative time scope for same-source narrative merge checks."""
    segment_scope = _extract_date_value(segment, ["日期", "时间", "监测时间"])
    if segment_scope:
        return segment_scope
    if source_profile:
        return clean_cell_value(str(source_profile.get("scope_date", "")))
    return ""


def _narrative_record_role(
    headers: list[str],
    values: dict[str, str],
    source_profile: dict[str, Any] | None,
) -> str:
    """Label narrative records as summary rows or scope supplements for diagnostics/merge."""
    if not source_profile:
        return "narrative_row"
    scope_header = str(source_profile.get("primary_entity_header", ""))
    scope_value = clean_cell_value(str(source_profile.get("primary_entity_value", "")))
    if not scope_header or not scope_value:
        return "narrative_row"
    current_value = clean_cell_value(values.get(scope_header, ""))
    if not current_value:
        return "scope_summary"
    if _semantic_key_value(scope_header, current_value) == _semantic_key_value(scope_header, scope_value):
        return "scope_summary"
    if any(values.get(header) for header in headers if header != scope_header and values.get(header)):
        return "scope_supplement"
    return "narrative_row"


def _raw_text_segments(document: DocumentBundle) -> list[tuple[str, str, int]]:
    """Explode long text blocks into smaller raw units before block segmentation."""
    segments: list[tuple[str, str, int]] = []
    for block in document.text_blocks:
        content = block.content.strip()
        if not content:
            continue
        pieces = [content]
        if "\n" in content and len(content) > 500:
            pieces = [piece.strip() for piece in re.split(r'\n+', content) if piece.strip()]
        elif len(content) > 900:
            pieces = [piece.strip() for piece in re.split(r'(?<=[。！？；;])\s+', content) if piece.strip()]
        for piece_index, piece in enumerate(pieces):
            location = f"text_block{block.block_index}"
            if len(pieces) > 1:
                location += f".seg{piece_index}"
            segments.append((piece, location, block.heading_level))
    return segments


def _group_segments_into_entity_blocks(segments: list[tuple[str, str, int]]) -> list[dict[str, Any]]:
    """Group adjacent raw segments into one-entity narrative blocks."""
    blocks: list[dict[str, Any]] = []
    current_parts: list[str] = []
    current_locations: list[str] = []
    current_entity = ""

    for segment, location, heading_level in segments:
        entity_hint = _extract_leading_entity(segment)
        if _starts_new_entity_block(segment, heading_level, entity_hint, current_entity, current_parts):
            blocks.append(_finalize_entity_block(current_parts, current_locations, current_entity))
            current_parts = []
            current_locations = []
            current_entity = ""

        current_parts.append(segment)
        current_locations.append(location)
        if entity_hint and not current_entity:
            current_entity = entity_hint

        if _should_flush_entity_block(current_parts, current_entity):
            blocks.append(_finalize_entity_block(current_parts, current_locations, current_entity))
            current_parts = []
            current_locations = []
            current_entity = ""

    if current_parts:
        blocks.append(_finalize_entity_block(current_parts, current_locations, current_entity))

    return _merge_continuation_blocks(blocks)


def _starts_new_entity_block(
    segment: str,
    heading_level: int,
    entity_hint: str,
    current_entity: str,
    current_parts: list[str],
) -> bool:
    """Detect boundaries between repeated-entity narrative blocks."""
    if not current_parts:
        return False
    if heading_level > 0:
        return True
    if re.match(LIST_ITEM_PATTERN, segment) and _segment_has_structure(segment):
        return True
    if entity_hint and current_entity and entity_hint != current_entity:
        return True
    if entity_hint and not current_entity and len(current_parts) >= 2:
        return True
    if sum(len(part) for part in current_parts) >= 900 and _segment_has_structure(segment):
        return True
    return False


def _should_flush_entity_block(parts: list[str], entity_hint: str) -> bool:
    """Prevent one entity block from greedily swallowing an entire long report."""
    total_length = sum(len(part) for part in parts)
    if total_length >= 1400:
        return True
    if entity_hint and len(parts) >= 4 and total_length >= 700:
        return True
    return False


def _finalize_entity_block(parts: list[str], locations: list[str], entity_hint: str) -> dict[str, Any]:
    """Materialize one entity-aware block for downstream rule/LLM extraction."""
    segment = "\n".join(part for part in parts if part).strip()
    if not segment:
        return {"segment": "", "location": "", "entity_hint": ""}
    location = locations[0] if len(locations) == 1 else f"{locations[0]}..{locations[-1]}"
    return {
        "segment": segment,
        "location": location,
        "entity_hint": entity_hint,
        "part_count": len(parts),
    }


def _merge_continuation_blocks(blocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Merge tiny continuation paragraphs into the previous entity block."""
    merged: list[dict[str, Any]] = []
    for block in blocks:
        segment = block.get("segment", "")
        if not segment:
            continue
        if merged and _should_merge_into_previous(block, merged[-1]):
            merged[-1]["segment"] = f"{merged[-1]['segment']}\n{segment}".strip()
            merged[-1]["location"] = f"{merged[-1]['location']}..{block.get('location', '')}".rstrip('.')
            merged[-1]["part_count"] = int(merged[-1].get("part_count", 1)) + int(block.get("part_count", 1))
            if not merged[-1].get("entity_hint") and block.get("entity_hint"):
                merged[-1]["entity_hint"] = block["entity_hint"]
            continue
        merged.append(block)
    return merged


def _should_merge_into_previous(block: dict[str, Any], previous: dict[str, Any]) -> bool:
    """Keep short continuation sentences attached to the previous entity block."""
    segment = block.get("segment", "")
    if not segment or block.get("entity_hint"):
        return False
    if re.match(LIST_ITEM_PATTERN, segment):
        return False
    if _segment_has_structure(segment):
        return False
    return len(segment) <= 180 and bool(previous.get("segment"))


def _segment_has_structure(segment: str) -> bool:
    """Return True when a segment carries enough structure to stand alone."""
    return bool(
        re.search(NUMBER_PATTERN, segment)
        or re.search(DATE_PATTERN, segment)
        or _extract_leading_entity(segment)
        or re.search(r'[^：:\s]{1,16}[：:][^：:]{1,32}', segment)
    )


def _is_narrative_source(document: DocumentBundle) -> bool:
    """Detect sources that should go through segmented narrative extraction."""
    return (
        document.file_type in {"word", "markdown", "text"}
        or (not document.tables and len(document.text_blocks) >= 2)
        or (len(document.text_blocks) > 4 and len(document.tables) <= 1)
    )


def _build_template_table_context(
    template: TemplateSchema,
    tmpl_table,
    requirement: RequirementSpec,
) -> dict[str, Any]:
    """Build topic cues for one template table to prevent cross-topic contamination."""
    description_text = tmpl_table.description or ""
    template_stem = Path(template.source_file).stem
    section_headings = [section.heading for section in template.sections[:2] if section.heading]

    anchor_parts = [template_stem]
    if description_text:
        anchor_parts.append(description_text)
    anchor_parts.extend(section_headings)

    topic_parts = list(anchor_parts)
    if tmpl_table.headers:
        topic_parts.append(" ".join(header for header in tmpl_table.headers if header))
    if requirement.raw_text and requirement.raw_text.strip() not in {"", "帮我智能填表"}:
        topic_parts.append(requirement.raw_text)
    anchor_text = " ".join(part for part in anchor_parts if part)
    topic_text = " ".join(part for part in topic_parts if part)
    return {
        "template_file": template.source_file,
        "anchor_text": anchor_text,
        "anchor_tokens": _meaningful_topic_tokens(anchor_text),
        "anchor_domains": _topic_domain_concepts(anchor_text),
        "topic_text": topic_text,
        "topic_tokens": _meaningful_topic_tokens(topic_text),
        "topic_domains": _topic_domain_concepts(topic_text),
        "filter_hints": _extract_context_filters(description_text),
    }


def _extract_context_filters(text: str) -> dict[str, str]:
    """Extract generic key-value hints from table descriptions such as '城市：南京'."""
    filters: dict[str, str] = {}
    if not text:
        return filters
    for line in re.split(r'[\n；;]+', text):
        normalized = clean_cell_value(line)
        if not normalized:
            continue
        match = re.match(r'^([^：:\s]{1,20})[：:]\s*([^：:]{1,40})$', normalized)
        if not match:
            continue
        key, value = match.groups()
        key = clean_cell_value(key)
        value = clean_cell_value(value)
        if not key or not value:
            continue
        if key in GENERIC_FILTER_KEYS or len(value) <= 32:
            filters[key] = value
    return filters


def _meaningful_topic_tokens(text: str) -> set[str]:
    """Extract stable topic tokens while skipping generic report words."""
    tokens: set[str] = set()
    for token in re.findall(r'[A-Za-z]{2,20}|[\u4e00-\u9fa5]{2,24}', text or ""):
        normalized = token.lower().strip()
        if len(normalized) < 2:
            continue
        if normalized in GENERIC_TOPIC_TOKENS:
            continue
        tokens.add(normalized)
        if re.fullmatch(r'[\u4e00-\u9fa5]{8,24}', normalized):
            for chunk in re.findall(r'[\u4e00-\u9fa5]{2,8}', normalized):
                if chunk in GENERIC_TOPIC_TOKENS:
                    continue
                if len(chunk) <= 4 and any(part in chunk for part in GENERIC_TOPIC_SUBSTRINGS):
                    continue
                tokens.add(chunk)
    return tokens


def _topic_domain_concepts(text: str) -> set[str]:
    """Infer coarse topic domains from topic text."""
    normalized = " ".join(sorted(_meaningful_topic_tokens(text)))
    concepts: set[str] = set()
    for concept, keywords in TOPIC_DOMAIN_KEYWORDS.items():
        if any(keyword.lower() in normalized for keyword in keywords):
            concepts.add(concept)
    return concepts


def _source_anchor_text(document: DocumentBundle) -> str:
    """Build a stable source-topic summary from filename/title metadata only."""
    parts = [Path(document.source_file).stem]
    if document.metadata:
        parts.append(str(document.metadata.get("title", "")))
        parts.append(str(document.metadata.get("inferred_topic", "")))
    return " ".join(part for part in parts if part)


def _source_matches_template_context(
    document: DocumentBundle,
    headers: list[str],
    requirement: RequirementSpec,
    template_context: dict[str, Any] | None = None,
) -> bool:
    """Cheap source-level filter so qwen only sees narrative docs that match the template intent."""
    template_tokens = set((template_context or {}).get("topic_tokens", set()))
    template_anchor_tokens = set((template_context or {}).get("anchor_tokens", set()))
    template_anchor_domains = set((template_context or {}).get("anchor_domains", set()))
    score = _source_context_score(document, headers, requirement, template_context=template_context)
    if not _document_matches_entity_headers(document, headers):
        return False

    source_anchor_text = _source_anchor_text(document)
    source_anchor_tokens = _meaningful_topic_tokens(source_anchor_text)
    source_anchor_domains = _topic_domain_concepts(source_anchor_text)
    anchor_overlap = template_anchor_tokens & source_anchor_tokens

    if template_anchor_domains and source_anchor_domains and not (template_anchor_domains & source_anchor_domains) and not anchor_overlap:
        return False
    if template_anchor_tokens and anchor_overlap:
        score += 4 + min(3, len(anchor_overlap))
    elif template_anchor_tokens and template_anchor_domains and not source_anchor_domains:
        score -= 1

    if template_tokens:
        source_tokens = _meaningful_topic_tokens(
            " ".join([
                source_anchor_text,
                " ".join(
                    block.content[:120]
                    for block in document.text_blocks[:4]
                    if block.content
                ),
            ])
        )
        if not (template_tokens & source_tokens):
            return score >= (6 if template_anchor_tokens else 5)
    if template_anchor_tokens and not anchor_overlap:
        return score >= 6
    return score >= 3


def _source_context_score(
    document: DocumentBundle,
    headers: list[str],
    requirement: RequirementSpec,
    template_context: dict[str, Any] | None = None,
) -> int:
    """Score how well a narrative source matches the current template context."""
    if document.file_type not in {"word", "markdown", "text"} and len(document.text_blocks) < 4:
        return 0

    title_parts = [Path(document.source_file).stem]
    if document.metadata:
        title_parts.append(str(document.metadata.get("title", "")))
    title_text = _normalize_topic_text(" ".join(title_parts))
    body_text = _normalize_topic_text(" ".join(
        block.content[:180]
        for block in document.text_blocks[:10]
        if block.content
    ))

    indicator_terms = _topic_terms_from_headers(headers) + requirement.indicator_keywords[:8]
    entity_terms = requirement.entity_keywords[:6]
    score = 0
    matched_indicator = 0
    matched_entity = 0
    template_tokens = set((template_context or {}).get("topic_tokens", set()))
    source_tokens = _meaningful_topic_tokens(" ".join(title_parts))
    body_tokens = _meaningful_topic_tokens(" ".join(
        block.content[:220]
        for block in document.text_blocks[:12]
        if block.content
    ))

    title_overlap = template_tokens & source_tokens
    body_overlap = template_tokens & body_tokens
    if title_overlap:
        score += 5 + min(4, len(title_overlap))
    elif body_overlap:
        score += 3 + min(3, len(body_overlap))
    elif template_tokens:
        score -= 1

    for term in _dedupe_topic_terms(indicator_terms):
        normalized = _normalize_topic_text(term)
        if len(normalized) < 2 or normalized in GENERIC_TOPIC_TOKENS:
            continue
        if normalized in title_text:
            score += 3
            matched_indicator += 1
        elif normalized in body_text:
            score += 2
            matched_indicator += 1

    for term in _dedupe_topic_terms(entity_terms):
        normalized = _normalize_topic_text(term)
        if len(normalized) < 2 or normalized in GENERIC_TOPIC_TOKENS:
            continue
        if normalized in title_text:
            score += 3
            matched_entity += 1
        elif normalized in body_text:
            score += 1
            matched_entity += 1

    if matched_indicator >= 2:
        score += 2
    if matched_entity and matched_indicator:
        score += 1
    if score >= 3:
        return score

    fallback_signal = len([
        block for block in document.text_blocks[:8]
        if re.search(NUMBER_PATTERN, block.content) or _extract_leading_entity(block.content)
    ])
    if fallback_signal >= 3 and matched_indicator >= 1:
        return score + 2
    if fallback_signal >= 4:
        return score + 1
    return score


def _top_relevant_source_segments(
    document: DocumentBundle,
    headers: list[str],
    requirement: RequirementSpec,
    limit: int,
    template_context: dict[str, Any] | None = None,
) -> list[tuple[str, str, int]]:
    """Rank a source's narrative segments before sending them to qwen."""
    scored: list[tuple[str, str, int]] = []
    for segment, location in _candidate_text_segments(document):
        if not _segment_is_relevant(segment, headers, requirement, template_context=template_context):
            continue
        score = 0
        lowered = segment.lower()
        for header in headers:
            for alias in _header_aliases(header):
                if alias.lower() in lowered:
                    score += 3
                    break
        if requirement.entity_keywords:
            score += sum(2 for keyword in requirement.entity_keywords[:6] if keyword in segment)
        if re.search(NUMBER_PATTERN, segment):
            score += 1
        if re.search(DATE_PATTERN, segment):
            score += 1
        if _extract_leading_entity(segment):
            score += 1
        if score > 0:
            scored.append((segment, location, score))
    scored.sort(key=lambda item: item[2], reverse=True)
    return scored[:limit]


def _segment_is_relevant(
    segment: str,
    headers: list[str],
    requirement: RequirementSpec,
    template_context: dict[str, Any] | None = None,
) -> bool:
    """Filter text blocks before expensive field extraction."""
    segment = clean_cell_value(segment)
    if len(segment) < 18:
        return False
    if any(segment.startswith(prefix) for prefix in GENERIC_SEGMENT_PREFIXES) and not re.search(NUMBER_PATTERN, segment):
        return False

    lowered = segment.lower()
    segment_tokens = _meaningful_topic_tokens(segment)
    template_tokens = set((template_context or {}).get("topic_tokens", set()))
    alias_hits = 0
    specific_alias_hits = 0
    for header in headers:
        for alias in _header_aliases(header):
            if alias.lower() in lowered:
                alias_hits += 1
                if _normalize_topic_text(alias) not in NORMALIZED_GENERIC_HEADER_TERMS:
                    specific_alias_hits += 1
                break

    if requirement.entity_keywords:
        if any(keyword in segment for keyword in requirement.entity_keywords):
            alias_hits += 1
    has_number = bool(re.search(NUMBER_PATTERN, segment))
    has_date = bool(re.search(DATE_PATTERN, segment))
    has_entity = bool(_extract_leading_entity(segment))
    has_structure = has_number or has_date or has_entity or bool(re.search(r'[^：:\s]{1,16}[：:][^：:]{1,32}', segment))
    topic_overlap = len(template_tokens & segment_tokens) if template_tokens else 0
    if topic_overlap:
        alias_hits += 1
    if template_tokens and topic_overlap == 0 and alias_hits == 0:
        return False
    return (
        specific_alias_hits >= 1 and has_structure
        or alias_hits >= 2 and has_structure
        or topic_overlap > 0 and has_structure
        or alias_hits >= 1 and has_number and has_entity
    )


def _topic_terms_from_headers(headers: list[str]) -> list[str]:
    terms: list[str] = []
    for header in headers:
        if not header:
            continue
        if _normalize_topic_text(header) not in NORMALIZED_GENERIC_HEADER_TERMS:
            terms.append(header)
        terms.extend([
            alias
            for alias in _header_aliases(header)[:4]
            if _normalize_topic_text(alias) not in NORMALIZED_GENERIC_HEADER_TERMS
        ])
    return terms


def _dedupe_topic_terms(terms: list[str]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for term in terms:
        normalized = _normalize_topic_text(term)
        if len(normalized) < 2 or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(term)
    return ordered


def _normalize_topic_text(text: str) -> str:
    return re.sub(r'[\s_\-:：,，、/\\()（）\[\]【】]+', '', (text or "").lower())


def _record_provenance_id(
    headers: list[str],
    *,
    source_file: str,
    source_location: str,
    row_index: int,
    values: dict[str, str],
    origin_tag: str,
) -> str:
    """Build a stable raw-record id for provenance/audit tracing."""
    seed = "|".join(
        [
            origin_tag,
            clean_cell_value(source_file),
            clean_cell_value(source_location),
            str(row_index),
            *(f"{header}={clean_cell_value(values.get(header, ''))}" for header in headers),
        ]
    )
    return hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]


def _with_record_provenance(headers: list[str], record: dict, *, origin_tag: str) -> dict:
    """Attach field-level value provenance so later stats can distinguish evidence vs. value."""
    values = record.get("values", {})
    source_file = clean_cell_value(str(record.get("source_file", "")))
    source_location = clean_cell_value(str(record.get("source_location", "")))
    row_index = int(record.get("row_index", 0) or 0)
    record_id = record.get("record_id") or _record_provenance_id(
        headers,
        source_file=source_file,
        source_location=source_location,
        row_index=row_index,
        values=values,
        origin_tag=origin_tag,
    )
    record["record_id"] = record_id
    record.setdefault("record_origin", origin_tag)
    record["origin_record_ids"] = _ordered_unique_strings([*record.get("origin_record_ids", []), record_id])
    field_value_sources: dict[str, list[str]] = record.setdefault("field_value_sources", {})
    field_value_record_ids: dict[str, list[str]] = record.setdefault("field_value_record_ids", {})
    for header in headers:
        value = clean_cell_value(values.get(header, ""))
        if value:
            field_value_sources.setdefault(header, [source_file] if source_file else [])
            field_value_record_ids.setdefault(header, [record_id])
        else:
            field_value_sources.setdefault(header, [])
            field_value_record_ids.setdefault(header, [])
    return record


def _ordered_unique_strings(values: list[str]) -> list[str]:
    """Deduplicate short string lists while preserving their original order."""
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        cleaned = clean_cell_value(value)
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        ordered.append(cleaned)
    return ordered


def _build_rule_record(headers: list[str], candidate: dict, src_row: list[str], row_index: int) -> dict:
    """Build a record with field-level confidence and evidence."""
    src_table: NormalizedTable = candidate["source_table"]
    col_mapping: dict[str, str] = candidate["col_mapping"]
    src_h_idx = {header: index for index, header in enumerate(src_table.headers)}
    row_text = " | ".join(clean_cell_value(cell) for cell in src_row[: max(len(src_table.headers), 12)])
    source_evidence: CandidateEvidence = candidate["evidence"]
    source_quality = 0.68 + min(0.26, (source_evidence.confidence or 0.0) * 0.26)

    values: dict[str, str] = {}
    field_confidence: dict[str, float | None] = {}
    field_evidence: dict[str, list[CandidateEvidence]] = {}
    match_methods: dict[str, str] = {}

    for header in headers:
        src_col = col_mapping.get(header, "")
        value = ""
        evidence: list[CandidateEvidence] = []
        match_method = _mapping_quality(header, src_col)

        if src_col in src_h_idx:
            cell_index = src_h_idx[src_col]
            if cell_index < len(src_row):
                value = clean_cell_value(src_row[cell_index])
                if value:
                    evidence.append(CandidateEvidence(
                        source_file=source_evidence.source_file,
                        location=f"table{src_table.table_index}({src_table.sheet_name}).row{row_index + 1}.col{cell_index + 1}",
                        raw_snippet=row_text[:220],
                        match_reason=f"{header} <- {src_col}",
                    ))

        confidence = _compute_rule_confidence(
            value,
            match_method,
            source_quality,
            bool(evidence),
            header=header,
        )
        if evidence:
            evidence[0].confidence = confidence

        values[header] = value
        field_confidence[header] = confidence
        field_evidence[header] = evidence
        match_methods[header] = match_method

    return _with_record_provenance(headers, {
        "values": values,
        "field_confidence": field_confidence,
        "field_evidence": field_evidence,
        "source_file": source_evidence.source_file,
        "source_location": source_evidence.location,
        "row_index": row_index,
        "match_methods": match_methods,
    }, origin_tag="rule")


def _build_text_record(
    headers: list[str],
    segment: str,
    source_file: str,
    location: str,
    source_profile: dict[str, Any] | None = None,
) -> dict | None:
    """Build a record from a narrative text segment."""
    values: dict[str, str] = {}
    field_confidence: dict[str, float | None] = {}
    field_evidence: dict[str, list[CandidateEvidence]] = {}
    match_methods: dict[str, str] = {}

    for header in headers:
        value, match_method, reason = _extract_value_for_header(header, segment)
        evidence: list[CandidateEvidence] = []
        confidence = None
        if value:
            confidence = _compute_text_rule_confidence(value, match_method)
            evidence = [CandidateEvidence(
                source_file=source_file,
                location=location,
                raw_snippet=segment[:220],
                match_reason=reason,
                confidence=confidence,
            )]

        values[header] = value
        field_confidence[header] = confidence
        field_evidence[header] = evidence
        match_methods[header] = match_method

    _apply_narrative_scope_defaults(
        headers,
        values,
        field_confidence,
        field_evidence,
        match_methods,
        source_profile,
    )

    if not _passes_text_record_threshold(values, headers):
        return None

    return _with_record_provenance(headers, {
        "values": values,
        "field_confidence": field_confidence,
        "field_evidence": field_evidence,
        "source_file": source_file,
        "source_location": location,
        "row_index": 0,
        "match_methods": match_methods,
        "record_role": _narrative_record_role(headers, values, source_profile),
        "narrative_scope": source_profile or {},
        "temporal_scope": _narrative_temporal_scope(segment, source_profile),
    }, origin_tag="text_rule")


def _extract_value_for_header(header: str, segment: str) -> tuple[str, str, str]:
    """Extract a field value from a text segment."""
    concept = _header_concept(header)
    aliases = _header_aliases(header)

    if concept.startswith("entity"):
        if concept == "entity_region":
            value = _extract_region_value(segment, aliases)
            if value:
                return value, "text_context", f"{header} 标签命中"
            value = _extract_leading_entity(segment)
            if value:
                return value, "entity_lead", f"{header} 段首实体命中"
            return "", "none", ""
        value = _extract_leading_entity(segment)
        if value and _entity_matches_concept(value, concept):
            return value, "entity_lead", f"{header} 段首实体命中"
        value = _extract_labeled_text(segment, aliases)
        if value:
            return value, "text_context", f"{header} 标签命中"
        return "", "none", ""

    if concept == "metric_continent":
        value = _extract_continent_value(segment, aliases)
        if value:
            return value, "text_context", f"{header} 洲别命中"
        return "", "none", ""

    if concept in {
        "metric_gdp_total",
        "metric_population",
        "metric_gdp_per_capita",
        "metric_budget_revenue",
        "metric_budget_expenditure",
        "metric_cases",
        "metric_tests",
        "metric_aqi",
        "metric_pm10",
        "metric_pm2_5",
        "generic_numeric",
    }:
        value = _extract_number_by_concept(segment, concept, aliases)
        if value:
            return value, "text_exact", f"{header} 语义规则命中"
        value, exact = _extract_near_alias_number(segment, aliases)
        if value:
            if concept == "metric_population" and not _population_candidate_is_valid(segment, value):
                return "", "none", ""
            return value, "text_exact" if exact else "text_approx", f"{header} 数值邻近命中"
        return "", "none", ""

    if concept == "date":
        value = _extract_date_value(segment, aliases)
        if value:
            return value, "text_exact", f"{header} 日期命中"
        return "", "none", ""

    value = _extract_labeled_text(segment, aliases)
    if value:
        return value, "text_context", f"{header} 文本邻近命中"

    return "", "none", ""


def _header_concept(header: str) -> str:
    """Classify a header into a broad semantic concept."""
    normalized = re.sub(r'[\s_/（）()\-]+', '', header).lower()
    if "pm2.5" in header.lower().replace(" ", "") or "pm25" in normalized:
        return "metric_pm2_5"
    if "pm10" in normalized:
        return "metric_pm10"
    if "空气质量指数" in header or normalized == "aqi":
        return "metric_aqi"
    if "首要污染物" in header:
        return "metric_pollutant"
    if "污染类型" in header:
        return "metric_pollution_type"
    if "一般公共预算收入" in header:
        return "metric_budget_revenue"
    if "一般公共预算支出" in header:
        return "metric_budget_expenditure"
    if "人均" in header and "gdp" in normalized:
        return "metric_gdp_per_capita"
    if "gdp" in normalized or "生产总值" in header:
        return "metric_gdp_total"
    if "人口" in header:
        return "metric_population"
    if "病例" in header or "确诊" in header:
        return "metric_cases"
    if "检测" in header:
        return "metric_tests"
    if "日期" in header or "时间" in header:
        return "date"
    if "大洲" in header or header.strip() == "洲":
        return "metric_continent"
    if "国家" in header or "地区" in header:
        return "entity_region"
    if "省" in header or "自治区" in header:
        return "entity_province"
    if "城市" in header or "city" in normalized:
        return "entity_city"
    if header.strip() == "区" or "区县" in header:
        return "entity_district"
    if "站点" in header or "名称" in header or "单位" in header:
        return "entity_name"
    if re.search(r'(数|率|值|金额|指数|占比|规模)', header):
        return "generic_numeric"
    return "generic_text"


def _header_aliases(header: str) -> list[str]:
    """Return header aliases used in text-rule extraction."""
    concept = _header_concept(header)
    alias_map = {
        "entity_city": ["城市名", "城市名称", "城市"],
        "entity_region": ["国家/地区", "国家地区", "国家", "地区"],
        "entity_province": ["省份", "省", "自治区", "直辖市"],
        "entity_district": ["区", "区县", "城区"],
        "entity_name": ["站点名称", "名称", "单位名称", "机构名称", "企业名称"],
        "metric_gdp_total": ["GDP总量", "GDP 总量", "地区生产总值", "国内生产总值"],
        "metric_population": ["常住人口", "人口"],
        "metric_gdp_per_capita": ["人均GDP", "人均 GDP", "人均地区生产总值", "人均国内生产总值"],
        "metric_budget_revenue": ["一般公共预算收入", "公共预算收入", "预算收入"],
        "metric_budget_expenditure": ["一般公共预算支出", "公共预算支出", "预算支出"],
        "metric_cases": ["病例数", "病例", "确诊病例", "新增确诊病例", "确诊数"],
        "metric_tests": ["每日检测数", "检测量", "核酸检测量", "单日检测量", "当日检测量"],
        "metric_aqi": ["空气质量指数", "AQI"],
        "metric_pm10": ["PM10监测值", "PM10"],
        "metric_pm2_5": ["PM2.5监测值", "PM2.5", "PM25"],
        "metric_pollutant": ["首要污染物"],
        "metric_pollution_type": ["污染类型"],
        "metric_continent": ["大洲", "洲"],
        "date": ["日期", "时间", "监测时间"],
    }
    aliases = alias_map.get(concept, []).copy()
    header_clean = header.strip()
    variants = {
        header_clean,
        re.sub(r'[\s_]+', '', header_clean),
        re.sub(r'[（(].*?[)）]', '', header_clean).strip(),
    }
    for item in variants:
        if item:
            aliases.append(item)
    return _deduplicate_aliases([alias for alias in aliases if alias])


def _deduplicate_aliases(aliases: list[str]) -> list[str]:
    """Deduplicate aliases while preserving order."""
    seen: set[str] = set()
    ordered: list[str] = []
    for alias in aliases:
        key = alias.lower()
        if key in seen:
            continue
        seen.add(key)
        ordered.append(alias)
    return ordered


def _entity_matches_concept(value: str, concept: str) -> bool:
    """Check whether a leading entity is compatible with the expected header type."""
    if concept == "entity_city":
        if re.fullmatch(r'[A-Za-z][A-Za-z .-]{2,30}', value):
            return False
        if value.endswith(("省", "自治区", "特别行政区")):
            return False
        if value.endswith(("市", "州", "盟", "区", "县")):
            return True
        return bool(re.fullmatch(r'[\u4e00-\u9fa5·]{2,6}', value))
    if concept == "entity_province":
        return value.endswith(("省", "自治区", "特别行政区"))
    if concept == "entity_district":
        return value.endswith(("区", "县"))
    if concept == "entity_region":
        return len(value) <= 20
    return True


def _document_matches_entity_headers(document: DocumentBundle, headers: list[str]) -> bool:
    """Require at least one plausible entity type match for narrative sources."""
    entity_headers = [
        (header, _header_concept(header), _header_aliases(header))
        for header in headers
        if _header_concept(header).startswith("entity")
    ]
    if not entity_headers:
        return True
    segments = _candidate_text_segments(document)[:30]
    if not segments:
        return True
    for header, concept, aliases in entity_headers:
        for segment, _location in segments:
            entity = _extract_leading_entity(segment)
            if entity and _entity_matches_concept(entity, concept):
                return True
            if concept == "entity_region":
                entity = _extract_region_value(segment, aliases)
                if entity:
                    return True
                continue
            labeled = _extract_labeled_text(segment, aliases)
            if labeled and (_entity_matches_concept(labeled, concept) or concept == "entity_name"):
                return True
    # Fallback: if the document has many structured text blocks with numbers, accept it
    # even if entity type matching is ambiguous
    structured_blocks = sum(
        1 for block in document.text_blocks[:20]
        if re.search(NUMBER_PATTERN, block.content) and len(block.content) > 30
    )
    if structured_blocks >= 3:
        return True
    return False


def _extract_leading_entity(segment: str) -> str:
    """Extract a leading entity from a narrative paragraph."""
    candidate_lines = [line.strip() for line in re.split(r'\n+', segment) if line.strip()]
    if not candidate_lines:
        return ""

    skipped_generic_heading = False
    for line in candidate_lines[:4]:
        if _is_generic_heading_line(line):
            skipped_generic_heading = True
            continue
        candidate = _match_leading_entity_candidate(line)
        if candidate:
            return candidate

    if skipped_generic_heading:
        return ""

    return _match_leading_entity_candidate(segment)


def _is_generic_heading_line(line: str) -> bool:
    """Skip short generic title lines so the next narrative line can provide the entity."""
    line = clean_cell_value(line)
    if not line or any(line.startswith(prefix) for prefix in GENERIC_SEGMENT_PREFIXES):
        return True
    if len(line) > 24 or re.search(NUMBER_PATTERN, line) or re.search(r'[，,。；;:：]', line):
        return False
    normalized = _normalize_topic_text(line)
    if normalized in GENERIC_TOPIC_TOKENS:
        return True
    return any(part in line for part in GENERIC_HEADING_LINE_PARTS)


def _match_leading_entity_candidate(text: str) -> str:
    """Extract and normalize one candidate entity from the start of a text fragment."""
    if any(text.startswith(prefix) for prefix in GENERIC_SEGMENT_PREFIXES):
        return ""
    match = re.match(
        r'^\s*('
        r'[A-Za-z][A-Za-z .-]{2,30}'
        r'|'
        # Limit to 2-8 Chinese chars so we don't greedily absorb predicate phrases.
        r'[\u4e00-\u9fa5·]{2,8}(?:市|省|自治区|特别行政区|自治州|地区|盟|县|区)?'
        r')'
        r'(?=以|凭借|作为|在|GDP|常住人口|人口|位|紧随其后|，|,|\s|$)',
        text,
    )
    if not match:
        return ""
    candidate = match.group(1).strip("：:，,。 ")
    candidate = re.sub(r'(紧随其后|以|凭借|作为|在)$', '', candidate).strip()
    candidate = re.sub(
        r'(常住人口.*|人口.*|人均.*|GDP.*|一般公共预算.*|病例.*|检测.*|新增.*)$',
        '',
        candidate,
    ).strip("：:，,。 ")
    if candidate in {"数据来源", "结语"} or re.fullmatch(r'[一二三四五六七八九十]+', candidate):
        return ""
    if any(part in candidate for part in GENERIC_ENTITY_BLACKLIST_PARTS):
        return ""
    return candidate


def _extract_labeled_text(segment: str, aliases: list[str]) -> str:
    """Extract text values around a header alias."""
    for alias in aliases:
        separator_pattern = r'[：:]' if len(alias) <= 2 or alias in {"地区", "国家", "区域", "名称", "单位"} else r'[：:\s]{0,3}'
        patterns = [
            rf'{re.escape(alias)}(?:{separator_pattern}|\s{{1,3}})([A-Za-z\u4e00-\u9fa5·0-9/%\-（）()]+)',
            rf'([A-Za-z\u4e00-\u9fa5·0-9/%\-（）()]+)[，,。；; ]{{0,2}}{re.escape(alias)}',
        ]
        for pattern in patterns:
            match = re.search(pattern, segment)
            if not match:
                continue
            value = clean_cell_value(match.group(1))
            value = re.sub(r'[，,。；;]+$', '', value).strip()
            if not value or value == alias:
                continue
            if _is_generic_heading_line(value) or any(part in value for part in GENERIC_HEADING_LINE_PARTS):
                continue
            if value.startswith(alias) and len(value) <= len(alias) + 6:
                continue
            if value:
                return value
    return ""


def _extract_region_value(segment: str, aliases: list[str]) -> str:
    """Extract a country/region only when the text gives explicit evidence."""
    leading = _extract_leading_entity(segment)
    if leading:
        return leading
    value = _extract_labeled_text(segment, aliases)
    if value:
        return _trim_trailing_entity_verb(value)
    match = re.search(
        r'地处[^\u4e00-\u9fa5A-Za-z0-9]{0,4}[A-Za-z\u4e00-\u9fa5（）()]{2,20}的'
        r'([A-Za-z\u4e00-\u9fa5]{2,20}?)(?=报告|通报|发布|新增|实现|达到|保持|位于|在|，|,|\s|$)',
        segment,
    )
    return _trim_trailing_entity_verb(match.group(1)) if match else ""


def _trim_trailing_entity_verb(value: str) -> str:
    """Trim obvious narrative verbs accidentally absorbed into an entity span."""
    cleaned = clean_cell_value(value)
    cleaned = re.sub(r'(报告|通报|发布|新增|实现|达到|保持|位于|作为)$', '', cleaned).strip()
    return cleaned


def _extract_continent_value(segment: str, aliases: list[str]) -> str:
    """Extract continent cues from narrative text such as '地处Asia（亚洲）'."""
    match = re.search(r'地处([A-Za-z]+)[（(]([\u4e00-\u9fa5]+)[)）]', segment)
    if match:
        chinese = clean_cell_value(match.group(2))
        english = clean_cell_value(match.group(1))
        return chinese or english
    match = re.search(r'地处([A-Za-z]+)', segment)
    if match:
        return clean_cell_value(match.group(1))
    match = re.search(r'(亚洲|欧洲|非洲|北美洲|南美洲|大洋洲|南极洲)', segment)
    if match:
        return clean_cell_value(match.group(1))
    value = _extract_labeled_text(segment, [alias for alias in aliases if len(alias) > 1])
    if value and _continent_value_is_valid(value):
        return value
    return ""


def _extract_near_alias_number(segment: str, aliases: list[str]) -> tuple[str, bool]:
    """Extract the nearest number around a metric alias."""
    for alias in sorted(aliases, key=len, reverse=True):
        best_value = ""
        best_distance = 10**9
        best_exact = False
        for match in re.finditer(re.escape(alias), segment, flags=re.IGNORECASE):
            start, end = match.span()
            before = segment[max(0, start - 32):start]
            after = segment[end:end + 32]
            candidates = []
            for number_match in re.finditer(NUMBER_PATTERN, before):
                value = number_match.group(0)
                distance = len(before) - number_match.end()
                candidates.append((value, distance, False))
            for number_match in re.finditer(NUMBER_PATTERN, after):
                value = number_match.group(0)
                distance = number_match.start()
                candidates.append((value, distance, True))
            for value, distance, is_after in candidates:
                if distance < best_distance:
                    best_value = _preserve_numeric_unit(segment, value)
                    best_distance = distance
                    best_exact = distance <= 10 or is_after
        if best_value:
            return clean_cell_value(best_value), best_exact
    return "", False


def _extract_date_value(segment: str, aliases: list[str]) -> str:
    """Extract a date near a date alias, or fallback to the first date in the segment."""
    for alias in aliases:
        pattern = rf'{re.escape(alias)}[：:\s]{{0,3}}({DATE_PATTERN})'
        match = re.search(pattern, segment)
        if match:
            return clean_cell_value(match.group(1))
    match = re.search(DATE_PATTERN, segment)
    return clean_cell_value(match.group(0)) if match else ""


def _preserve_numeric_unit(text_scope: str, value: str) -> str:
    """Keep nearby numeric units so downstream normalization can recover true magnitude."""
    cleaned = clean_cell_value(value)
    if not cleaned:
        return ""
    match = re.search(
        rf'{re.escape(cleaned)}\s*(万亿元|亿元|万元|万份|万例|万人|万|亿|元|份|例)',
        text_scope,
    )
    if match:
        return f"{cleaned} {match.group(1)}"
    return cleaned


def _extract_number_by_concept(segment: str, concept: str, aliases: list[str]) -> str:
    """Use concept-aware regex before falling back to nearest-number matching."""
    alias_group = "|".join(re.escape(alias) for alias in aliases[:4] if alias)
    if not alias_group:
        return ""

    patterns: list[str] = []
    if concept == "metric_population":
        patterns = [
            # Most specific: "X万...人口" handles "1,779.05万常住人口", "3,191.43万庞大人口基数"
            rf'({NUMBER_PATTERN})\s*万[^。；，,]{{0,10}}(?:常住|户籍|总)?人口',
            # Fallback: alias + strict connector, e.g. "常住人口达2487.45"
            rf'(?:{alias_group})\s*(?:达|为|至|约|控制在|严控至|增至)\s*({NUMBER_PATTERN})',
        ]
    elif concept == "metric_gdp_total":
        patterns = [
            rf'({NUMBER_PATTERN})\s*亿元[^。；，,]{{0,12}}(?:的)?(?:GDP\s*总量|地区生产总值|国内生产总值)',
            rf'(?:GDP\s*总量|地区生产总值|国内生产总值)[^0-9]{{0,10}}(?:达|达到|为|位列|跃升至|突破)?\s*({NUMBER_PATTERN})',
        ]
    elif concept == "metric_gdp_per_capita":
        patterns = [
            # Most specific: "X元...人均GDP" handles "217,710元的高位人均GDP"
            rf'({NUMBER_PATTERN})\s*元[^。；，,\d]{{0,14}}(?:人均\s*GDP|人均地区生产总值|人均国内生产总值)',
            # Fallback: alias-led with optional connector, e.g. "人均GDP为115,710元"
            rf'(?:{alias_group})\s*(?:高达|为|达|实现|约为|均为|位居)?\s*({NUMBER_PATTERN})',
        ]
    elif concept == "metric_budget_revenue":
        patterns = [
            # Most specific: number + 亿元 + 预算收入 (keyword follows number+unit)
            rf'({NUMBER_PATTERN})\s*亿元[^\u3002\uff1b\uff0c,\d]{{0,10}}(?:一般公共预算斖入|一般公共预算收入|公共预算收入|预算收入)',
            # Fallback: keyword + optional connector + number (handles "预算收入 8,500.91亿元")
            rf'(?:一般公共预算斖入|一般公共预算收入|公共预算收入|预算收入)[^0-9]{{0,8}}(?:突破|跃升至|为|达|达到|\s)*({NUMBER_PATTERN})',
        ]
    elif concept == "metric_budget_expenditure":
        patterns = [
            # Most specific: number + 亿元 + 预算支出 (keyword follows number+unit)
            rf'({NUMBER_PATTERN})\s*亿元[^\u3002\uff1b\uff0c,\d]{{0,10}}(?:一般公共预算支出|公共预算支出|预算支出)',
            # Fallback: keyword + optional connector + number (handles "预算支出 10,506.3亿元")
            rf'(?:一般公共预算支出|公共预算支出|预算支出)[^0-9]{{0,8}}(?:高达|为|达|达到|更是高达|\s)*({NUMBER_PATTERN})',
        ]
    elif concept == "metric_cases":
        patterns = [
            rf'(?:新增确诊病例|确诊病例|病例数|确诊数)[^0-9]{{0,10}}(?:为|达|共)?\s*({NUMBER_PATTERN})',
            rf'({NUMBER_PATTERN})\s*例[^。；，,]{{0,10}}(?:新增确诊病例|确诊病例|病例)',
        ]
    elif concept == "metric_tests":
        patterns = [
            rf'(?:检测量|核酸检测量|单日检测量|当日检测量|每日检测数)[^0-9]{{0,10}}(?:约|为|达)?\s*({NUMBER_PATTERN})',
            rf'({NUMBER_PATTERN})\s*(?:万)?份[^。；，,]{{0,10}}(?:检测量|核酸检测量|单日检测量|每日检测数)',
        ]

    for pattern in patterns:
        match = re.search(pattern, segment, flags=re.IGNORECASE)
        if match:
            return clean_cell_value(_preserve_numeric_unit(segment, match.group(1)))
    if concept == "metric_cases" and re.search(r'(无新增病例|无新增确诊|零新增确诊|零新增病例|全零报告)', segment):
        return "0"
    return ""


def _population_candidate_is_valid(segment: str, value: str) -> bool:
    """Reject incidental numbers near '人口' that are clearly not population values."""
    normalized_value = re.escape(clean_cell_value(value))
    if re.search(rf'{normalized_value}\s*(万亿|亿|万|人)', segment):
        return True
    if re.search(rf'(常住人口|人口)[^。；，,\d]{{0,8}}(?:约|达|为|超|近|共)?\s*{normalized_value}', segment):
        return True
    if re.search(rf'{normalized_value}[^。；，,\d]{{0,8}}(?:常住人口|人口)', segment):
        return True
    return False


def _passes_text_record_threshold(values: dict[str, str], headers: list[str]) -> bool:
    """Reject narrative segments that do not contain enough structured content."""
    entity_headers = [header for header in headers if _header_concept(header).startswith("entity")]
    non_empty = [header for header, value in values.items() if value]
    metric_count = len([header for header in non_empty if header not in entity_headers])
    metric_domains = {
        _metric_domain(_header_concept(header))
        for header in non_empty
        if header not in entity_headers and _metric_domain(_header_concept(header))
    }

    if len(headers) >= 6:
        if entity_headers and not any(values.get(header) for header in entity_headers):
            return False
        # Allow partial same-domain records so complementary sources can merge later.
        if len(non_empty) >= 3 and metric_count >= 2 and len(metric_domains) <= 1:
            return True
        return len(non_empty) >= 4 and metric_count >= 3

    if len(headers) >= 4:
        if entity_headers and not any(values.get(header) for header in entity_headers):
            return False
        # Allow partial same-domain records so complementary sources can merge later.
        if len(non_empty) >= 3 and metric_count >= 2 and len(metric_domains) <= 1:
            return True
        if len(non_empty) >= 3 and metric_count >= 2:
            return True
        return len(non_empty) >= 4 and metric_count >= 3

    if entity_headers and not any(values.get(header) for header in entity_headers):
        return False
    return len(non_empty) >= 2 and metric_count >= 1


def _metric_domain(concept: str) -> str:
    """Group metric concepts into coarse domains for partial-record validation."""
    if concept in {
        "metric_gdp_total",
        "metric_population",
        "metric_gdp_per_capita",
        "metric_budget_revenue",
        "metric_budget_expenditure",
    }:
        return "economy"
    if concept in {"metric_cases", "metric_tests"}:
        return "epidemic"
    if concept in {"metric_aqi", "metric_pm10", "metric_pm2_5", "metric_pollutant", "metric_pollution_type"}:
        return "air_quality"
    return ""


def _mapping_quality(template_header: str, source_header: str) -> str:
    """Classify the quality of a header mapping."""
    if not template_header or not source_header:
        return "none"
    if template_header == source_header:
        return "exact"
    if template_header in source_header or source_header in template_header:
        return "containment"
    score = similarity(template_header, source_header)
    if score >= 0.78:
        return "approx"
    if score >= 0.58:
        return "weak"
    return "none"


def _compute_rule_confidence(
    value: str,
    match_method: str,
    source_quality: float,
    has_evidence: bool,
    header: str = "",
) -> float | None:
    """Compute confidence for structured rule-based extraction."""
    if not value or not has_evidence:
        return None

    base_scores = {
        "exact": 0.91,
        "containment": 0.84,
        "approx": 0.76,
        "weak": 0.70,
        "none": 0.58,
    }
    max_scores = {
        "exact": 0.98,
        "containment": 0.89,
        "approx": 0.85,
        "weak": 0.79,
        "none": 0.69,
    }
    confidence = base_scores.get(match_method, 0.58)
    confidence += max(-0.03, min(0.03, (source_quality - 0.76) * 0.18))
    confidence += min(len(value.strip()) / 160.0, 0.02)
    concept = _header_concept(header)
    concept_offsets = {
        "date": 0.05,
        "entity_region": 0.02,
        "entity_city": 0.018,
        "entity_name": 0.015,
        "metric_gdp_total": 0.012,
        "metric_population": 0.006,
        "metric_gdp_per_capita": 0.0,
        "metric_budget_revenue": 0.01,
        "metric_budget_expenditure": 0.008,
        "metric_tests": -0.02,
        "metric_cases": -0.04,
        "metric_continent": -0.05,
        "metric_pm10": -0.012,
        "metric_pm2_5": -0.016,
        "metric_aqi": -0.02,
    }
    confidence += concept_offsets.get(concept, 0.0)
    if re.search(r'\d{4}[-/.年]\d{1,2}', value) or re.search(r'\d+(?:\.\d+)?', value):
        confidence += 0.01
    if len(value.strip()) <= 1:
        confidence -= 0.05
    upper_bound = max_scores.get(match_method, 0.69)
    lower_bound = 0.90 if match_method == "exact" else 0.70 if match_method in {"approx", "weak"} else 0.58
    return round(max(lower_bound, min(upper_bound, confidence)), 4)


def _compute_text_rule_confidence(value: str, match_method: str) -> float | None:
    """Confidence for text-rule extraction stays within contest-required bands."""
    if not value:
        return None
    base_scores = {
        "text_exact": 0.92,
        "text_context": 0.84,
        "entity_lead": 0.82,
        "text_approx": 0.75,
        "none": 0.60,
    }
    max_scores = {
        "text_exact": 0.98,
        "text_context": 0.89,
        "entity_lead": 0.88,
        "text_approx": 0.84,
        "none": 0.69,
    }
    confidence = base_scores.get(match_method, 0.60)
    confidence += min(len(value.strip()) / 180.0, 0.018)
    if re.search(r'\d+(?:\.\d+)?', value):
        confidence += 0.012
    if "," in value:
        confidence -= 0.004
    if "." in value:
        confidence += 0.006
    if len(value.strip()) <= 1:
        confidence -= 0.05
    lower_bound = {
        "text_exact": 0.90,
        "text_context": 0.80,
        "entity_lead": 0.78,
        "text_approx": 0.70,
    }.get(match_method, 0.58)
    return round(max(lower_bound, min(max_scores.get(match_method, 0.69), confidence)), 4)


def _aggregate_column_confidence(records: list[dict], headers: list[str]) -> dict[str, float | None]:
    """Aggregate confidence across records by column."""
    aggregated: dict[str, float | None] = {}
    for header in headers:
        values = [
            record["field_confidence"].get(header)
            for record in records
            if record["values"].get(header)
            and record["field_confidence"].get(header) is not None
        ]
        aggregated[header] = round(sum(values) / len(values), 4) if values else None
    return aggregated


def _collect_table_evidence(records: list[dict], table_evidence: list[CandidateEvidence]) -> list[CandidateEvidence]:
    """Keep a compact evidence list per table."""
    collected: list[CandidateEvidence] = []
    seen: set[tuple[str, str, str]] = set()
    for evidence in table_evidence:
        key = (evidence.source_file, evidence.location, evidence.match_reason)
        if key not in seen:
            seen.add(key)
            collected.append(evidence)

    for record in records[:12]:
        for evidence_list in record["field_evidence"].values():
            for evidence in evidence_list[:1]:
                key = (evidence.source_file, evidence.location, evidence.match_reason)
                if key not in seen:
                    seen.add(key)
                    collected.append(evidence)
    return collected


def _rows_to_llm_records(headers: list[str], rows: list[list[str]], retrieval: RetrievalResult, method: str) -> list[dict]:
    """Convert LLM rows to record objects used by fill service."""
    if not rows:
        return []
    evidence_source = _pick_llm_evidence_source(retrieval)
    records: list[dict] = []
    for row_index, row in enumerate(rows):
        values: dict[str, str] = {}
        field_confidence: dict[str, float | None] = {}
        field_evidence: dict[str, list[CandidateEvidence]] = {}
        match_methods: dict[str, str] = {}
        for col_index, header in enumerate(headers):
            value = clean_cell_value(row[col_index]) if col_index < len(row) else ""
            confidence = _compute_llm_confidence(value)
            evidence = []
            if value and evidence_source:
                evidence = [CandidateEvidence(
                    source_file=evidence_source.source_file,
                    location=evidence_source.location,
                    raw_snippet=evidence_source.raw_snippet,
                    match_reason=f"LLM 辅助抽取: {header}",
                    confidence=confidence,
                )]
            values[header] = value
            field_confidence[header] = confidence
            field_evidence[header] = evidence
            match_methods[header] = method
        records.append(_with_record_provenance(headers, {
            "values": values,
            "field_confidence": field_confidence,
            "field_evidence": field_evidence,
            "source_file": evidence_source.source_file if evidence_source else "",
            "source_location": evidence_source.location if evidence_source else "",
            "row_index": row_index,
            "match_methods": match_methods,
        }, origin_tag=method or "llm"))
    return _deduplicate_records(records, headers)


def _rows_to_llm_records_from_source(
    headers: list[str],
    rows: list[list[str]],
    source_file: str,
    location: str,
    raw_snippet: str,
    method: str,
) -> list[dict]:
    """Convert qwen rows from one narrative source into record objects."""
    records: list[dict] = []
    for row_index, row in enumerate(rows):
        values: dict[str, str] = {}
        field_confidence: dict[str, float | None] = {}
        field_evidence: dict[str, list[CandidateEvidence]] = {}
        match_methods: dict[str, str] = {}
        for col_index, header in enumerate(headers):
            value = clean_cell_value(row[col_index]) if col_index < len(row) else ""
            confidence = _compute_llm_confidence(value)
            evidence = []
            if value:
                evidence = [CandidateEvidence(
                    source_file=source_file,
                    location=location,
                    raw_snippet=raw_snippet,
                    match_reason=f"qwen 叙事抽取: {header}",
                    confidence=confidence,
                )]
            values[header] = value
            field_confidence[header] = confidence
            field_evidence[header] = evidence
            match_methods[header] = method
        if not _passes_text_record_threshold(values, headers):
            continue
        records.append(_with_record_provenance(headers, {
            "values": values,
            "field_confidence": field_confidence,
            "field_evidence": field_evidence,
            "source_file": source_file,
            "source_location": location,
            "row_index": row_index,
            "match_methods": match_methods,
        }, origin_tag=method or "qwen_narrative"))
    return _deduplicate_records(records, headers)


def _pick_llm_evidence_source(retrieval: RetrievalResult) -> CandidateEvidence | None:
    """Pick a stable evidence source for LLM-assisted extraction."""
    if retrieval.text_candidates:
        return retrieval.text_candidates[0]
    if retrieval.source_docs:
        first_doc = retrieval.source_docs[0]
        snippet = first_doc.text_blocks[0].content[:220] if first_doc.text_blocks else ""
        return CandidateEvidence(
            source_file=first_doc.source_file,
            location="text_block0",
            raw_snippet=snippet,
            match_reason="LLM 辅助抽取",
            confidence=0.55,
        )
    return None


def _compute_llm_confidence(value: str) -> float | None:
    """Confidence for LLM-assisted extraction stays in the lower band and varies by value quality."""
    if not value:
        return None
    confidence = 0.51
    if re.search(r'\d{4}[-/.年]\d{1,2}', value) or re.search(r'\d+(?:\.\d+)?', value):
        confidence += 0.07
    if len(value.strip()) >= 6:
        confidence += 0.04
    elif len(value.strip()) <= 1:
        confidence -= 0.05
    return round(max(0.45, min(0.75, confidence)), 4)


def _deduplicate_records(records: list[dict], headers: list[str]) -> list[dict]:
    """Deduplicate records while preserving structured table row identity."""
    deduped: list[dict] = []
    seen: dict[tuple[str, ...], int] = {}
    for record in records:
        visible_signature = _record_visible_value_signature(record, headers)
        if not any(visible_signature):
            continue
        signature = _record_dedup_signature(record, headers, visible_signature)
        if signature in seen:
            _merge_record_data(deduped[seen[signature]], record, headers)
            continue
        seen[signature] = len(deduped)
        deduped.append(record)
    return deduped


def _record_visible_value_signature(record: dict, headers: list[str]) -> tuple[str, ...]:
    """Build the template-visible value signature shared by narrative-style dedup."""
    return tuple(clean_cell_value(record["values"].get(header, "")) for header in headers)


def _dedup_narrative_only(records: list[dict], headers: list[str]) -> list[dict]:
    """Dedup only narrative records after normalization while preserving table rows."""
    deduped: list[dict] = []
    seen_narrative: dict[tuple[str, ...], int] = {}
    for record in records:
        if _record_is_table_backed(record):
            deduped.append(record)
            continue

        visible_signature = _record_visible_value_signature(record, headers)
        if not any(visible_signature):
            continue

        signature = _dedup_signature_from_visible_values(visible_signature)
        if signature in seen_narrative:
            _merge_record_data(deduped[seen_narrative[signature]], record, headers)
            continue
        seen_narrative[signature] = len(deduped)
        deduped.append(record)
    return deduped


def _dedup_signature_from_visible_values(visible_signature: tuple[str, ...]) -> tuple[str, ...]:
    """Namespace value-based dedup signatures so they never collide with row identities."""
    return ("__values__", *visible_signature)


def _dedup_signature_from_table_row(record: dict) -> tuple[str, ...]:
    """Use stable table row identity so date-hidden rows are not collapsed early."""
    row_identity = _record_table_row_identity(record)
    if row_identity:
        return ("__table_row__", *row_identity)
    return ()


def _dedup_signature_from_fallback_row_info(record: dict) -> tuple[str, ...]:
    """Fallback for structured records that only expose table-level source metadata."""
    source_file = clean_cell_value(str(record.get("source_file", "")))
    source_location = clean_cell_value(str(record.get("source_location", "")))
    row_index = record.get("row_index")
    row_marker = ""
    if isinstance(row_index, int) and row_index >= 0:
        row_marker = f"row{row_index + 1}"
    if source_location.startswith("table"):
        return tuple(part for part in (source_file, source_location, row_marker) if part)
    return ()


def _record_dedup_signature(
    record: dict,
    headers: list[str],
    visible_signature: tuple[str, ...] | None = None,
) -> tuple[str, ...]:
    """Choose row-identity dedup for table-backed records and value dedup otherwise."""
    visible_signature = visible_signature or _record_visible_value_signature(record, headers)
    if _record_is_table_backed(record):
        signature = _dedup_signature_from_table_row(record)
        if signature:
            return signature
        fallback_signature = _dedup_signature_from_fallback_row_info(record)
        if fallback_signature:
            return ("__table_row__", *fallback_signature)
    return _dedup_signature_from_visible_values(visible_signature)


def _record_is_table_backed(record: dict) -> bool:
    """Return True when a record is anchored to a concrete structured table row."""
    if _record_table_row_identity(record):
        return True
    source_location = clean_cell_value(str(record.get("source_location", "")))
    if source_location.startswith("table") and not _record_is_narrative(record):
        return True
    return False


def _record_table_row_identity(record: dict) -> tuple[str, ...]:
    """Extract the most stable structured-row identity available from field evidence."""
    source_file = clean_cell_value(str(record.get("source_file", "")))
    source_location = clean_cell_value(str(record.get("source_location", "")))
    row_locations = sorted({
        row_location
        for evidence_list in record.get("field_evidence", {}).values()
        for evidence in evidence_list
        for row_location in [_table_row_location(evidence.location)]
        if row_location
    })
    row_index = record.get("row_index")
    row_marker = ""
    if isinstance(row_index, int) and row_index >= 0:
        row_marker = f"row{row_index + 1}"
    if row_locations:
        return tuple(part for part in (source_file, source_location, row_locations[0], row_marker) if part)
    return ()


def _table_row_location(location: str) -> str:
    """Normalize field evidence like table0(Sheet1).row12.col3 to a row-level key."""
    normalized = clean_cell_value(location)
    match = re.match(r'^(table\d+(?:\([^)]*\))?)\.row(\d+)(?:\.col\d+)?$', normalized)
    if not match:
        return ""
    return f"{match.group(1)}.row{match.group(2)}"



def _merge_record_data(existing: dict, incoming: dict, headers: list[str]):
    """Merge evidence and confidence from duplicate records instead of dropping later evidence."""
    existing.setdefault("field_value_sources", {})
    existing.setdefault("field_value_record_ids", {})
    existing["origin_record_ids"] = _ordered_unique_strings([
        *existing.get("origin_record_ids", []),
        *incoming.get("origin_record_ids", []),
    ])
    for header in headers:
        existing_value = clean_cell_value(existing["values"].get(header, ""))
        incoming_value = clean_cell_value(incoming["values"].get(header, ""))
        if not existing_value and incoming_value:
            existing["values"][header] = incoming["values"][header]
            existing["field_value_sources"][header] = _record_field_value_sources(incoming, header)
            existing["field_value_record_ids"][header] = _record_field_value_record_ids(incoming, header)
            existing_value = incoming_value
        if existing_value and incoming_value and existing_value == incoming_value:
            _merge_field_value_provenance(existing, incoming, header)
        existing_conf = existing.get("field_confidence", {}).get(header)
        incoming_conf = incoming.get("field_confidence", {}).get(header)
        if incoming_conf is not None and (existing_conf is None or incoming_conf > existing_conf):
            existing.setdefault("field_confidence", {})[header] = incoming_conf
        existing.setdefault("field_evidence", {})
        existing.setdefault("match_methods", {})
        current_evidence = existing["field_evidence"].get(header, [])
        incoming_evidence = incoming.get("field_evidence", {}).get(header, [])
        seen_keys = {(item.source_file, item.location, item.match_reason) for item in current_evidence}
        for item in incoming_evidence:
            key = (item.source_file, item.location, item.match_reason)
            if key not in seen_keys:
                current_evidence.append(item)
                seen_keys.add(key)
        existing["field_evidence"][header] = current_evidence
        if incoming.get("match_methods", {}).get(header):
            current_method = existing["match_methods"].get(header, "")
            if not current_method or current_method.startswith("qwen"):
                existing["match_methods"][header] = incoming["match_methods"][header]
    if not existing.get("source_file") and incoming.get("source_file"):
        existing["source_file"] = incoming["source_file"]
    if not existing.get("source_location") and incoming.get("source_location"):
        existing["source_location"] = incoming["source_location"]


def _raw_record_source_counts(records: list[dict], retrieval: RetrievalResult) -> Counter:
    """Count records by their primary source_file before cross-source evidence support is merged."""
    counts = Counter({doc.source_file: 0 for doc in retrieval.source_docs})
    for record in records:
        source_file = clean_cell_value(str(record.get("source_file", "")))
        if source_file:
            counts[source_file] += 1
    return counts


def _record_source_counts(records: list[dict], retrieval: RetrievalResult) -> Counter:
    """Count final deduplicated records per source."""
    counts = Counter({doc.source_file: 0 for doc in retrieval.source_docs})
    for record in records:
        for source_file in _record_sources(record):
            counts[source_file] += 1
    return counts


def _empty_narrative_stage_audit(documents: list[DocumentBundle]) -> dict[str, dict[str, int]]:
    """Create default per-source counters for narrative stage auditing."""
    return {
        document.source_file: {
            "relevant_segments": 0,
            "rule_records": 0,
            "stable_records": 0,
            "suspicious_records": 0,
            "llm_records": 0,
            "post_entity_records": 0,
            "final_records": 0,
        }
        for document in documents
        if _is_narrative_source(document)
    }


def _empty_narrative_record_registry(documents: list[DocumentBundle]) -> dict[str, dict[str, dict[str, Any]]]:
    """Create a per-source raw-record registry for later narrative merge/effect audits."""
    return {
        document.source_file: {}
        for document in documents
        if _is_narrative_source(document)
    }


def _register_narrative_records(
    registry: dict[str, dict[str, dict[str, Any]]],
    records: list[dict],
    *,
    origin: str,
    quality: str | None = None,
):
    """Register raw narrative records before later merge/filter stages can hide them."""
    for record in records:
        source_file = clean_cell_value(str(record.get("source_file", "")))
        record_id = clean_cell_value(str(record.get("record_id", "")))
        if not source_file or not record_id or source_file not in registry:
            continue
        registry[source_file][record_id] = {
            "record_id": record_id,
            "source_file": source_file,
            "source_location": clean_cell_value(str(record.get("source_location", ""))),
            "record_origin": origin,
            "quality_bucket": quality or origin,
            "record_role": clean_cell_value(str(record.get("record_role", ""))),
            "non_empty_headers": [
                header
                for header, value in (record.get("values", {}) or {}).items()
                if clean_cell_value(value)
            ],
        }


def _mark_narrative_record_quality(
    registry: dict[str, dict[str, dict[str, Any]]],
    records: list[dict],
    *,
    quality: str,
):
    """Annotate already-registered narrative raw records with their quality bucket."""
    for record in records:
        source_file = clean_cell_value(str(record.get("source_file", "")))
        record_id = clean_cell_value(str(record.get("record_id", "")))
        if not source_file or not record_id:
            continue
        entry = registry.get(source_file, {}).get(record_id)
        if entry is not None:
            entry["quality_bucket"] = quality


def _serialize_narrative_record_registry(
    registry: dict[str, dict[str, dict[str, Any]]],
) -> dict[str, list[dict[str, Any]]]:
    """Convert the internal raw-record registry into a stable JSON-friendly structure."""
    return {
        source_file: list(entries.values())
        for source_file, entries in registry.items()
        if entries
    }


def _update_narrative_stage_audit(
    stage_audit: dict[str, dict[str, int]],
    source_counts: Counter | dict[str, int],
    field_name: str,
):
    """Accumulate one stage counter into the narrative audit structure."""
    for source_file, count in source_counts.items():
        if source_file not in stage_audit:
            continue
        stage_audit[source_file][field_name] = stage_audit[source_file].get(field_name, 0) + int(count)


def _record_sources(record: dict) -> set[str]:
    """Collect all sources that support a record."""
    sources = {
        evidence.source_file
        for evidence_list in record.get("field_evidence", {}).values()
        for evidence in evidence_list
        if evidence.source_file
    }
    if not sources and record.get("source_file"):
        sources.add(record["source_file"])
    return sources


def _record_is_narrative(record: dict) -> bool:
    """Return True when the record mainly comes from narrative text rather than a table row."""
    for evidence_list in record.get("field_evidence", {}).values():
        for evidence in evidence_list:
            if "text_block" in evidence.location or evidence.location.startswith("text_"):
                return True
    return any(
        method.startswith("qwen") or method.startswith("text_") or method == "entity_lead"
        for method in record.get("match_methods", {}).values()
        if method
    )


def _record_matches_template_context(record: dict, template_context: dict[str, Any] | None) -> bool:
    """Reject narrative records whose source/topic does not match the current template."""
    if not template_context:
        return True
    template_tokens = set(template_context.get("topic_tokens", set()))
    template_anchor_tokens = set(template_context.get("anchor_tokens", set()))
    template_anchor_domains = set(template_context.get("anchor_domains", set()))
    if not template_tokens or not _record_is_narrative(record):
        return True

    source_anchor_parts = [Path(record.get("source_file", "")).stem]
    source_parts = list(source_anchor_parts)
    for evidence_list in record.get("field_evidence", {}).values():
        for evidence in evidence_list[:1]:
            if evidence.source_file:
                source_anchor_parts.append(Path(evidence.source_file).stem)
            source_parts.append(evidence.raw_snippet[:180])
    source_text = " ".join(part for part in source_parts if part)
    source_tokens = _meaningful_topic_tokens(source_text)
    source_anchor_text = " ".join(part for part in source_anchor_parts if part)
    source_anchor_tokens = _meaningful_topic_tokens(source_anchor_text)
    source_anchor_domains = _topic_domain_concepts(source_anchor_text)
    anchor_overlap = template_anchor_tokens & source_anchor_tokens
    if not anchor_overlap and template_anchor_tokens:
        anchor_overlap = template_anchor_tokens & source_tokens
    if anchor_overlap:
        return True
    if template_anchor_domains and source_anchor_domains and not (template_anchor_domains & source_anchor_domains):
        return False

    if template_tokens & source_tokens:
        return True

    template_domains = _topic_domain_concepts(" ".join(sorted(template_tokens)))
    source_domains = _topic_domain_concepts(source_text)
    if template_domains and source_domains and not (template_domains & source_domains):
        return False
    if template_domains and not source_domains:
        return True

    evidence_hits = 0
    for header, evidence_list in record.get("field_evidence", {}).items():
        if record.get("values", {}).get(header) in (None, "", "N/A"):
            continue
        if evidence_list:
            evidence_hits += 1
    return evidence_hits >= max(2, min(3, len(record.get("values", {}))))


def _filter_records_by_template_context(
    records: list[dict],
    template_context: dict[str, Any] | None,
    warnings: list[str],
) -> tuple[list[dict], dict[str, Any]]:
    """Drop obviously wrong-topic narrative records before merge/write-back."""
    diagnostics = _empty_filter_diagnostics()
    if not template_context:
        diagnostics["stage_remaining_counts"]["template_context"] = len(records)
        return records, diagnostics
    filtered: list[dict] = []
    dropped: list[str] = []
    reason_counts: Counter[str] = Counter()
    per_source_counts: dict[str, Counter[str]] = {}
    for record in records:
        reason = ""
        if not _record_matches_template_context(record, template_context):
            reason = "template_topic_mismatch"
        elif not _record_matches_context_filters(record, template_context):
            reason = "template_filter_mismatch"
        if not reason:
            filtered.append(record)
            continue
        source_file = clean_cell_value(str(record.get("source_file", "")))
        dropped.append(Path(source_file).name or "unknown")
        reason_counts[reason] += 1
        if source_file:
            per_source_counts.setdefault(source_file, Counter())
            per_source_counts[source_file][reason] += 1
            diagnostics["per_source_stage"].setdefault(source_file, {})
            diagnostics["per_source_stage"][source_file]["template_context"] = {
                "dropped": int(diagnostics["per_source_stage"][source_file].get("template_context", {}).get("dropped", 0)) + 1,
                "remaining": 0,
                "reason_counts": dict(per_source_counts[source_file]),
            }
        diagnostics["filtered_records"].append({
            "record_id": record.get("record_id", ""),
            "source_file": source_file,
            "field_name": "",
            "filter_reason": reason,
            "filter_stage": "template_context",
            "filter_level": "hard_filter",
        })
        diagnostics["hard_block_examples"].append({
            "record_id": record.get("record_id", ""),
            "source_file": source_file,
            "field_name": "",
            "filter_reason": reason,
            "filter_stage": "template_context",
            "filter_level": "hard_filter",
        })
    if dropped:
        warnings.append(
            "已丢弃与当前模板主题不一致的叙事记录: "
            + ", ".join(sorted(dict.fromkeys(dropped))[:6])
        )
    per_source_remaining = Counter(
        source_file
        for record in filtered
        for source_file in [clean_cell_value(str(record.get("source_file", "")))]
        if source_file
    )
    diagnostics["filter_reason_counts"] = dict(reason_counts)
    diagnostics["per_source"] = {
        source_file: dict(counts)
        for source_file, counts in per_source_counts.items()
    }
    diagnostics["stage_loss_counts"]["template_context"] = len(records) - len(filtered)
    diagnostics["stage_remaining_counts"]["template_context"] = len(filtered)
    diagnostics["stage_reason_counts"]["template_context"] = dict(reason_counts)
    for source_file, counts in per_source_counts.items():
        diagnostics["per_source_stage"].setdefault(source_file, {})
        diagnostics["per_source_stage"][source_file]["template_context"] = {
            "dropped": sum(counts.values()),
            "remaining": int(per_source_remaining.get(source_file, 0)),
            "reason_counts": dict(counts),
        }
    for source_file, remaining in per_source_remaining.items():
        diagnostics["per_source_stage"].setdefault(source_file, {})
        bucket = diagnostics["per_source_stage"][source_file].setdefault("template_context", {
            "dropped": 0,
            "remaining": 0,
            "reason_counts": {},
        })
        bucket["remaining"] = int(remaining)
    return filtered, diagnostics


def _record_matches_context_filters(record: dict, template_context: dict[str, Any] | None) -> bool:
    """Validate record values against explicit filters inferred from the template region."""
    filter_hints = dict((template_context or {}).get("filter_hints", {}))
    if not filter_hints:
        return True
    values = record.get("values", {})
    headers = list(values.keys())
    for key, expected_value in filter_hints.items():
        matched_header = best_column_match(key, headers)
        if not matched_header:
            continue
        actual_value = clean_cell_value(values.get(matched_header, ""))
        if not actual_value:
            continue
        if _context_value_matches(actual_value, expected_value):
            continue
        return False
    return True


def _context_value_matches(actual_value: str, expected_value: str) -> bool:
    """Loose equality used for table-region validation."""
    if not actual_value or not expected_value:
        return True
    if _fuzzy_date_match(actual_value, expected_value):
        return True
    normalized_actual = _normalize_topic_text(actual_value)
    normalized_expected = _normalize_topic_text(expected_value)
    return (
        normalized_actual == normalized_expected
        or normalized_expected in normalized_actual
        or normalized_actual in normalized_expected
    )


def _normalize_records(headers: list[str], records: list[dict]) -> list[dict]:
    """Normalize extracted values before multi-source merge and template write-back."""
    for record in records:
        raw_values = record.setdefault("raw_values", {})
        normalized_flags = record.setdefault("normalization_status", {})
        for header in headers:
            raw_value = clean_cell_value(record.get("values", {}).get(header, ""))
            if header not in raw_values:
                raw_values[header] = raw_value
            normalized_value, status = _normalize_field_value(header, raw_value)
            record.setdefault("values", {})[header] = normalized_value
            normalized_flags[header] = status
    return records


def _annotate_records_with_entity_semantics(
    headers: list[str],
    records: list[dict],
    template_context: dict[str, Any] | None,
) -> list[dict]:
    """Attach normalized entity/granularity metadata to each record before filtering."""
    context_text = _template_context_blob(template_context)
    for index, record in enumerate(records):
        record.setdefault("record_id", _record_identifier(record, headers, index))
        values = record.get("values", {})
        entity_metadata = record.setdefault("entity_metadata", {})
        entity_compatibility = record.setdefault("entity_compatibility", {})
        primary_entity = None

        for header in headers:
            concept = _header_concept(header)
            if not concept.startswith("entity"):
                continue
            value = clean_cell_value(values.get(header, ""))
            if not value:
                continue
            assessment = evaluate_entity_compatibility(
                value,
                header,
                peer_headers=headers,
                context_text=context_text,
                record_values=values,
            )
            entity_metadata[header] = {
                "normalized_entity_text": assessment.get("normalized_entity_text", ""),
                "normalized_entity_type": assessment.get("normalized_entity_type", ""),
                "normalized_granularity": assessment.get("normalized_granularity", ""),
                "parent_scope": assessment.get("parent_scope", ""),
                "semantic_role": assessment.get("semantic_role", ""),
            }
            entity_compatibility[header] = dict(assessment)
            if primary_entity is None:
                primary_entity = assessment

        if primary_entity:
            record["normalized_entity_text"] = primary_entity.get("normalized_entity_text", "")
            record["normalized_entity_type"] = primary_entity.get("normalized_entity_type", "")
            record["normalized_granularity"] = primary_entity.get("normalized_granularity", "")
            record["parent_scope"] = primary_entity.get("parent_scope", "")
            record["semantic_role"] = primary_entity.get("semantic_role", "")
    return records


def _template_context_blob(template_context: dict[str, Any] | None) -> str:
    """Flatten template context into one semantic text blob for compatibility checks."""
    if not template_context:
        return ""
    parts = [
        str(template_context.get("template_file", "")),
        str(template_context.get("anchor_text", "")),
        str(template_context.get("topic_text", "")),
    ]
    return " ".join(part for part in parts if part)


def _record_identifier(record: dict, headers: list[str], index: int) -> str:
    """Build a stable record id for filter diagnostics and traceability."""
    values = record.get("values", {})
    seed = "|".join(
        [
            str(record.get("source_file", "")),
            str(record.get("source_location", "")),
            str(record.get("row_index", index)),
            *(f"{header}={clean_cell_value(values.get(header, ''))}" for header in headers),
        ]
    )
    return hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]


def _empty_filter_diagnostics() -> dict[str, Any]:
    """Create the default per-table filter diagnostics structure."""
    return {
        "filtered_records": [],
        "filter_reason_counts": {},
        "per_source": {},
        "stage_loss_counts": {},
        "stage_remaining_counts": {},
        "stage_reason_counts": {},
        "per_source_stage": {},
        "recovered_examples": [],
        "hard_block_examples": [],
        "soft_block_examples": [],
        "remap_examples": [],
    }


def _merge_filter_diagnostics(base: dict[str, Any], extra: dict[str, Any]) -> dict[str, Any]:
    """Merge filter diagnostics from multiple filter passes."""
    merged = _empty_filter_diagnostics()
    for name in ("filtered_records", "recovered_examples", "hard_block_examples", "soft_block_examples", "remap_examples"):
        seen: set[tuple[str, str, str]] = set()
        for item in list(base.get(name, [])) + list(extra.get(name, [])):
            fingerprint = (
                str(item.get("record_id", "")),
                str(item.get("field_name", "")),
                str(item.get("filter_reason", "")),
            )
            if fingerprint in seen:
                continue
            seen.add(fingerprint)
            merged[name].append(item)
    counts = Counter(base.get("filter_reason_counts", {}))
    counts.update(extra.get("filter_reason_counts", {}))
    merged["filter_reason_counts"] = dict(counts)
    stage_loss_counts = Counter(base.get("stage_loss_counts", {}))
    stage_loss_counts.update(extra.get("stage_loss_counts", {}))
    merged["stage_loss_counts"] = dict(stage_loss_counts)
    stage_remaining_counts = Counter(base.get("stage_remaining_counts", {}))
    stage_remaining_counts.update(extra.get("stage_remaining_counts", {}))
    merged["stage_remaining_counts"] = dict(stage_remaining_counts)
    stage_reason_counts: dict[str, Counter[str]] = {}
    for diagnostics in (base.get("stage_reason_counts", {}), extra.get("stage_reason_counts", {})):
        for stage_name, reason_counts in diagnostics.items():
            stage_reason_counts.setdefault(stage_name, Counter())
            stage_reason_counts[stage_name].update(reason_counts)
    merged["stage_reason_counts"] = {
        stage_name: dict(reason_counts)
        for stage_name, reason_counts in stage_reason_counts.items()
    }
    per_source: dict[str, Counter[str]] = {}
    for diagnostics in (base.get("per_source", {}), extra.get("per_source", {})):
        for source_file, reason_counts in diagnostics.items():
            per_source.setdefault(source_file, Counter())
            per_source[source_file].update(reason_counts)
    merged["per_source"] = {source_file: dict(reason_counts) for source_file, reason_counts in per_source.items()}
    per_source_stage: dict[str, dict[str, dict[str, Any]]] = {}
    for diagnostics in (base.get("per_source_stage", {}), extra.get("per_source_stage", {})):
        for source_file, stage_info in diagnostics.items():
            source_bucket = per_source_stage.setdefault(source_file, {})
            for stage_name, payload in stage_info.items():
                bucket = source_bucket.setdefault(stage_name, {
                    "dropped": 0,
                    "remaining": 0,
                    "reason_counts": {},
                })
                bucket["dropped"] = int(bucket.get("dropped", 0)) + int(payload.get("dropped", 0))
                bucket["remaining"] = max(int(bucket.get("remaining", 0)), int(payload.get("remaining", 0)))
                reason_counter = Counter(bucket.get("reason_counts", {}))
                reason_counter.update(payload.get("reason_counts", {}))
                bucket["reason_counts"] = dict(reason_counter)
    merged["per_source_stage"] = per_source_stage
    return merged


def _filter_records_by_entity_legality(
    headers: list[str],
    records: list[dict],
    warnings: list[str],
    template_context: dict[str, Any] | None = None,
) -> tuple[list[dict], set[str], dict[str, Any]]:
    """Filter records by entity legality and granularity-aware compatibility."""
    filtered: list[dict] = []
    invalidated_sources: set[str] = set()
    diagnostics = _empty_filter_diagnostics()
    reason_counts: Counter[str] = Counter()
    per_source_counts: dict[str, Counter[str]] = {}
    blocked_examples: list[str] = []
    context_text = _template_context_blob(template_context)

    for record in records:
        values = record.get("values", {})
        has_entity_value = False
        has_accepted_entity = False
        invalid_examples: list[dict[str, Any]] = []

        for header in headers:
            concept = _header_concept(header)
            if not concept.startswith("entity"):
                continue
            value = clean_cell_value(values.get(header, ""))
            if not value:
                continue
            has_entity_value = True
            assessment = record.get("entity_compatibility", {}).get(header) or evaluate_entity_compatibility(
                value,
                header,
                peer_headers=headers,
                context_text=context_text,
                record_values=values,
            )
            if assessment.get("accepted"):
                has_accepted_entity = True
                if assessment.get("recoverable_mismatch_reason") or assessment.get("compatibility_score", 1.0) < 0.95:
                    diagnostics["recovered_examples"].append({
                        "record_id": record.get("record_id", ""),
                        "source_file": record.get("source_file", ""),
                        "field_name": header,
                        "entity_text": value,
                        "normalized_entity_type": assessment.get("normalized_entity_type", ""),
                        "normalized_granularity": assessment.get("normalized_granularity", ""),
                        "compatibility_score": assessment.get("compatibility_score", 0.0),
                        "recoverable_mismatch_reason": assessment.get("recoverable_mismatch_reason", ""),
                    })
                continue

            invalid_examples.append({
                "field_name": header,
                "value": value,
                "normalized_entity_type": assessment.get("normalized_entity_type", ""),
                "normalized_granularity": assessment.get("normalized_granularity", ""),
                "filter_reason": assessment.get("filter_reason", "entity_incompatible"),
                "filter_level": assessment.get("filter_level", "soft_filter"),
                "filter_stage": assessment.get("filter_stage", "entity_compatibility"),
                "whether_recoverable": bool(assessment.get("whether_recoverable", False)),
            })

        if has_entity_value and not has_accepted_entity:
            remap_examples = _maybe_remap_record_to_narrative_scope(
                record,
                headers,
                template_context=template_context,
                context_text=context_text,
            )
            if remap_examples:
                diagnostics["remap_examples"].extend(remap_examples)
                values = record.get("values", {})
                has_entity_value = False
                has_accepted_entity = False
                invalid_examples = []
                for header in headers:
                    concept = _header_concept(header)
                    if not concept.startswith("entity"):
                        continue
                    value = clean_cell_value(values.get(header, ""))
                    if not value:
                        continue
                    has_entity_value = True
                    assessment = record.get("entity_compatibility", {}).get(header) or evaluate_entity_compatibility(
                        value,
                        header,
                        peer_headers=headers,
                        context_text=context_text,
                        record_values=values,
                    )
                    if assessment.get("accepted"):
                        has_accepted_entity = True
                if has_entity_value and has_accepted_entity:
                    filtered.append(record)
                    continue

            source_file = record.get("source_file", "")
            dominant = invalid_examples[0] if invalid_examples else {
                "field_name": "",
                "value": "",
                "normalized_entity_type": "",
                "normalized_granularity": "",
                "filter_reason": "entity_incompatible",
                "filter_level": "soft_filter",
                "filter_stage": "entity_compatibility",
                "whether_recoverable": True,
            }
            reason = str(dominant.get("filter_reason", "entity_incompatible") or "entity_incompatible")
            if source_file:
                per_source_counts.setdefault(source_file, Counter())
                per_source_counts[source_file][reason] += 1
            reason_counts[reason] += 1
            if dominant.get("filter_level") == "hard_filter" and source_file:
                invalidated_sources.add(source_file)
            example = {
                "record_id": record.get("record_id", ""),
                "source_file": source_file,
                "entity_text": dominant.get("value", ""),
                "normalized_entity_type": dominant.get("normalized_entity_type", ""),
                "normalized_granularity": dominant.get("normalized_granularity", ""),
                "field_name": dominant.get("field_name", ""),
                "filter_reason": reason,
                "filter_stage": dominant.get("filter_stage", "entity_compatibility"),
                "whether_recoverable": bool(dominant.get("whether_recoverable", False)),
                "filter_level": dominant.get("filter_level", "soft_filter"),
            }
            diagnostics["filtered_records"].append(example)
            if dominant.get("filter_level") == "hard_filter":
                diagnostics["hard_block_examples"].append(example)
            elif dominant.get("filter_level") == "remap_candidate":
                diagnostics["remap_examples"].append(example)
            else:
                diagnostics["soft_block_examples"].append(example)
            if invalid_examples and len(blocked_examples) < 3:
                blocked_examples.append(
                    f"{dominant.get('field_name')}={dominant.get('value')}({describe_entity_reason(reason)})"
                )
            continue

        filtered.append(record)

    dropped_count = len(records) - len(filtered)
    if dropped_count:
        reason_text = "；".join(
            f"{describe_entity_reason(reason)} x{count}"
            for reason, count in reason_counts.most_common(3)
        )
        message = f"已按实体合法性/粒度兼容性过滤 {dropped_count} 条候选记录"
        if reason_text:
            message += f"：{reason_text}"
        if blocked_examples:
            message += f"，示例: {', '.join(blocked_examples)}"
        warnings.append(message)
        if reason_counts and len(reason_counts) == 1 and dropped_count >= 2:
            warnings.append(
                f"同一过滤原因批量清空 narrative records: {describe_entity_reason(next(iter(reason_counts)))}"
            )

    per_source_remaining = Counter(
        source_file
        for record in filtered
        for source_file in [clean_cell_value(str(record.get("source_file", "")))]
        if source_file
    )
    diagnostics["filter_reason_counts"] = dict(reason_counts)
    diagnostics["per_source"] = {source_file: dict(counts) for source_file, counts in per_source_counts.items()}
    diagnostics["stage_loss_counts"]["entity_legality"] = dropped_count
    diagnostics["stage_remaining_counts"]["entity_legality"] = len(filtered)
    diagnostics["stage_reason_counts"]["entity_legality"] = dict(reason_counts)
    for source_file, counts in per_source_counts.items():
        diagnostics["per_source_stage"].setdefault(source_file, {})
        diagnostics["per_source_stage"][source_file]["entity_legality"] = {
            "dropped": sum(counts.values()),
            "remaining": int(per_source_remaining.get(source_file, 0)),
            "reason_counts": dict(counts),
        }
    for source_file, remaining in per_source_remaining.items():
        diagnostics["per_source_stage"].setdefault(source_file, {})
        bucket = diagnostics["per_source_stage"][source_file].setdefault("entity_legality", {
            "dropped": 0,
            "remaining": 0,
            "reason_counts": {},
        })
        bucket["remaining"] = int(remaining)

    return filtered, invalidated_sources, diagnostics


def _maybe_remap_record_to_narrative_scope(
    record: dict,
    headers: list[str],
    *,
    template_context: dict[str, Any] | None,
    context_text: str,
) -> list[dict[str, Any]]:
    """Remap recoverable narrative entity mismatches to document scope when evidence supports it."""
    if _record_is_table_backed(record):
        return []
    scope = record.get("narrative_scope") or {}
    if not scope or not scope.get("supports_entity_remap"):
        return []

    values = record.setdefault("values", {})
    field_confidence = record.setdefault("field_confidence", {})
    field_evidence = record.setdefault("field_evidence", {})
    match_methods = record.setdefault("match_methods", {})
    remapped: list[dict[str, Any]] = []

    for header in headers:
        concept = _header_concept(header)
        if not concept.startswith("entity"):
            continue
        scope_value = clean_cell_value((scope.get("scope_values") or {}).get(header, ""))
        if not scope_value:
            continue

        current_value = clean_cell_value(values.get(header, ""))
        current_assessment = None
        if current_value:
            current_assessment = record.get("entity_compatibility", {}).get(header) or evaluate_entity_compatibility(
                current_value,
                header,
                peer_headers=headers,
                context_text=context_text,
                record_values=values,
            )
            if current_assessment.get("accepted"):
                continue
            if not current_assessment.get("whether_recoverable"):
                continue
            # If the entity fails because the field is country_only (e.g. a province in the
            # \u56fd\u5bb6/\u5730\u533a column), only remap when the source document has exactly ONE
            # sub-national entity listed — that case indicates supplementary context for the parent
            # country.  When the document contains multiple co-equal sub-national entities
            # (e.g. 28 Chinese provinces each with their own data rows), remapping every one of
            # them to the parent scope produces semantically wrong duplicate country rows.
            if current_assessment.get("filter_reason") == "region_field_country_only":
                if len(scope.get("subordinate_entities") or []) >= 2:
                    continue

        candidate_values = dict(values)
        candidate_values[header] = scope_value
        scope_assessment = evaluate_entity_compatibility(
            scope_value,
            header,
            peer_headers=headers,
            context_text=context_text,
            record_values=candidate_values,
        )
        if not scope_assessment.get("accepted"):
            continue

        values[header] = scope_value
        scope_confidence = (scope.get("scope_confidence") or {}).get(header)
        existing_confidence = field_confidence.get(header)
        if scope_confidence is not None and (existing_confidence is None or scope_confidence > existing_confidence):
            field_confidence[header] = scope_confidence

        current_items = field_evidence.get(header, [])
        seen_keys = {(item.source_file, item.location, item.match_reason) for item in current_items}
        for item in _clone_evidence_list((scope.get("scope_evidence") or {}).get(header, [])):
            key = (item.source_file, item.location, item.match_reason)
            if key not in seen_keys:
                current_items.append(item)
                seen_keys.add(key)
        field_evidence[header] = current_items
        match_methods[header] = "doc_scope"
        remapped.append({
            "record_id": record.get("record_id", ""),
            "source_file": record.get("source_file", ""),
            "field_name": header,
            "from_value": current_value,
            "to_value": scope_value,
            "filter_reason": (current_assessment or {}).get("filter_reason", "") if current_assessment else "",
            "remap_reason": "narrative_scope_entity",
            "scope_signature": scope.get("scope_signature", ""),
        })

    if remapped:
        _annotate_records_with_entity_semantics(headers, [record], template_context)
    return remapped


def _normalize_field_value(header: str, value: str) -> tuple[str, str]:
    """Normalize one field value according to its header semantics."""
    if not value:
        return "", "empty"
    concept = _header_concept(header)
    cleaned = clean_cell_value(value).replace("\u3000", " ")
    if concept == "date":
        return _normalize_date_value(cleaned)
    if concept in NUMERIC_HEADER_CONCEPTS:
        return _normalize_numeric_value(header, cleaned)
    return cleaned, "as_is"


def _normalize_date_value(value: str) -> tuple[str, str]:
    """Normalize date-like strings into ISO-ish output."""
    match = re.search(
        r'((?:19\d{2}|20\d{2}|2100))[-/.年](1[0-2]|0?[1-9])(?:[-/.月](3[01]|[12]\d|0?[1-9]))?'
        r'(?:[日号])?(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?',
        value,
    )
    if not match:
        return value, "as_is"
    year, month, day, hour, minute, second = match.groups()
    day = day or "01"
    normalized = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
    if hour and minute:
        normalized += f" {hour.zfill(2)}:{minute.zfill(2)}:{(second or '00').zfill(2)}"
    return normalized, "normalized"


def _normalize_numeric_value(header: str, value: str) -> tuple[str, str]:
    """Normalize numeric strings and convert units when the header is explicit."""
    match = re.search(rf'({NUMBER_PATTERN})\s*(万亿元|亿元|万元|万份|万例|万人|万|亿|元|份|例|%|％)?', value)
    if not match:
        return value, "as_is"
    number_text, unit = match.groups()
    try:
        numeric = float(number_text.replace(",", ""))
    except ValueError:
        return value, "as_is"

    header_text = header.replace(" ", "")
    concept = _header_concept(header)
    if unit == "万亿元" and ("亿元" in header_text or concept in {"metric_gdp_total", "metric_budget_revenue", "metric_budget_expenditure"}):
        numeric *= 10000.0
    elif unit == "亿元" and "万元" in header_text:
        numeric *= 10000.0
    elif unit == "亿" and "万" in header_text:
        numeric *= 10000.0
    elif unit == "亿" and concept == "metric_population" and "亿" not in header_text:
        numeric *= 100000000.0
    elif unit == "万" and "亿" in header_text and "万亿" not in header_text:
        numeric /= 10000.0
    elif unit in {"万", "万人"} and concept == "metric_population" and "万" not in header_text:
        numeric *= 10000.0
    elif unit in {"万", "万份"} and concept == "metric_tests" and "万" not in header_text:
        numeric *= 10000.0
    elif unit in {"万", "万例"} and concept == "metric_cases" and "万" not in header_text:
        numeric *= 10000.0
    elif unit in {"万", "万元"} and concept == "metric_gdp_per_capita" and ("元" in header_text or "人均" in header_text):
        numeric *= 10000.0

    normalized = f"{numeric:.4f}".rstrip("0").rstrip(".")
    return normalized, "normalized" if normalized != clean_cell_value(value) else "as_is"


def _merge_records_by_semantic_key(
    records: list[dict],
    headers: list[str],
    requirement: RequirementSpec,
    use_llm: bool,
    usage_context: dict[str, Any] | None = None,
) -> list[dict]:
    """Merge partial records that refer to the same entity/date tuple across sources."""
    key_headers = [
        header for header in headers
        if _header_concept(header).startswith("entity") or _header_concept(header) == "date"
    ]
    if not key_headers and headers:
        key_headers = [headers[0]]

    merged: list[dict] = []
    key_to_index: dict[tuple[str, ...], int] = {}
    for record in records:
        key = tuple(_semantic_key_value(header, record["values"].get(header, "")) for header in key_headers)
        if not any(key):
            merged.append(record)
            continue
        existing_index = key_to_index.get(key)
        if existing_index is None:
            key_to_index[key] = len(merged)
            merged.append(record)
            continue
        if (
            _record_sources(merged[existing_index]) == _record_sources(record)
            and not _should_merge_same_source_records(merged[existing_index], record, headers, key_headers)
        ):
            merged.append(record)
            continue
        _merge_semantic_record_data(
            merged[existing_index],
            record,
            headers=headers,
            key_headers=key_headers,
            requirement=requirement,
            use_llm=use_llm,
            usage_context=usage_context,
        )
    return merged


def _merge_semantic_record_data(
    existing: dict,
    incoming: dict,
    *,
    headers: list[str],
    key_headers: list[str],
    requirement: RequirementSpec,
    use_llm: bool,
    usage_context: dict[str, Any] | None = None,
):
    """Merge record fragments and resolve true value conflicts conservatively."""
    existing.setdefault("field_value_sources", {})
    existing.setdefault("field_value_record_ids", {})
    existing["origin_record_ids"] = _ordered_unique_strings([
        *existing.get("origin_record_ids", []),
        *incoming.get("origin_record_ids", []),
    ])
    for header in headers:
        existing_value = clean_cell_value(existing["values"].get(header, ""))
        incoming_value = clean_cell_value(incoming["values"].get(header, ""))
        if not incoming_value:
            continue
        if not existing_value or existing_value == incoming_value:
            if not existing_value:
                existing["values"][header] = incoming_value
                existing["field_value_sources"][header] = _record_field_value_sources(incoming, header)
                existing["field_value_record_ids"][header] = _record_field_value_record_ids(incoming, header)
            _merge_field_value_provenance(existing, incoming, header)
            existing_conf = existing.get("field_confidence", {}).get(header)
            incoming_conf = incoming.get("field_confidence", {}).get(header)
            if incoming_conf is not None and (existing_conf is None or incoming_conf > existing_conf):
                existing.setdefault("field_confidence", {})[header] = incoming_conf
            existing.setdefault("field_evidence", {})
            existing.setdefault("match_methods", {})
            current_evidence = existing["field_evidence"].get(header, [])
            seen_keys = {(item.source_file, item.location, item.match_reason) for item in current_evidence}
            for item in incoming.get("field_evidence", {}).get(header, []):
                key = (item.source_file, item.location, item.match_reason)
                if key not in seen_keys:
                    current_evidence.append(item)
                    seen_keys.add(key)
            existing["field_evidence"][header] = current_evidence
            if incoming.get("match_methods", {}).get(header) and (
                not existing["match_methods"].get(header)
                or existing["match_methods"].get(header, "").startswith("qwen")
            ):
                existing["match_methods"][header] = incoming["match_methods"][header]
            continue
        if header in key_headers:
            existing.setdefault("field_evidence", {})
            existing.setdefault("field_confidence", {})
            existing.setdefault("match_methods", {})
            current_evidence = existing["field_evidence"].get(header, [])
            seen_keys = {(item.source_file, item.location, item.match_reason) for item in current_evidence}
            for item in incoming.get("field_evidence", {}).get(header, []):
                key = (item.source_file, item.location, item.match_reason)
                if key not in seen_keys:
                    current_evidence.append(item)
                    seen_keys.add(key)
            existing["field_evidence"][header] = current_evidence
            incoming_conf = incoming.get("field_confidence", {}).get(header)
            existing_conf = existing.get("field_confidence", {}).get(header)
            if incoming_conf is not None and (existing_conf is None or incoming_conf > existing_conf):
                existing["field_confidence"][header] = incoming_conf
            if incoming.get("match_methods", {}).get(header) and (
                not existing["match_methods"].get(header)
                or existing["match_methods"].get(header, "").startswith("qwen")
            ):
                existing["match_methods"][header] = incoming["match_methods"][header]
            continue

        resolved = _resolve_conflicting_field_value(
            header=header,
            existing=existing,
            incoming=incoming,
            requirement=requirement,
            use_llm=use_llm,
            usage_context=usage_context,
        )
        if resolved == "incoming":
            existing["values"][header] = incoming_value
            existing.setdefault("field_confidence", {})[header] = incoming.get("field_confidence", {}).get(header)
            existing.setdefault("field_evidence", {})[header] = incoming.get("field_evidence", {}).get(header, [])
            existing.setdefault("match_methods", {})[header] = incoming.get("match_methods", {}).get(header, "")
            existing["field_value_sources"][header] = _record_field_value_sources(incoming, header)
            existing["field_value_record_ids"][header] = _record_field_value_record_ids(incoming, header)

        existing.setdefault("field_evidence", {})
        existing.setdefault("field_confidence", {})
        existing.setdefault("match_methods", {})
        current_evidence = existing["field_evidence"].get(header, [])
        seen_keys = {(item.source_file, item.location, item.match_reason) for item in current_evidence}
        for item in incoming.get("field_evidence", {}).get(header, []):
            key = (item.source_file, item.location, item.match_reason)
            if key not in seen_keys:
                current_evidence.append(item)
                seen_keys.add(key)
        existing["field_evidence"][header] = current_evidence
    if not existing.get("source_file") and incoming.get("source_file"):
        existing["source_file"] = incoming["source_file"]
    if not existing.get("source_location") and incoming.get("source_location"):
        existing["source_location"] = incoming["source_location"]


def _should_merge_same_source_records(
    existing: dict,
    incoming: dict,
    headers: list[str],
    key_headers: list[str],
) -> bool:
    """Allow conservative same-source semantic merge for non-table narrative fragments only."""
    if _record_is_table_backed(existing) or _record_is_table_backed(incoming):
        return False
    existing_time = _normalized_record_temporal_scope(existing)
    incoming_time = _normalized_record_temporal_scope(incoming)
    if existing_time and incoming_time and existing_time != incoming_time:
        return False
    return _records_have_complementary_values(existing, incoming, headers, key_headers)


def _normalized_record_temporal_scope(record: dict) -> str:
    """Normalize optional record-level time scope for safe same-source narrative merge."""
    temporal_scope = clean_cell_value(str(record.get("temporal_scope", "")))
    if not temporal_scope:
        return ""
    normalized, _status = _normalize_date_value(temporal_scope)
    return normalized.lower()


def _records_have_complementary_values(
    existing: dict,
    incoming: dict,
    headers: list[str],
    key_headers: list[str],
) -> bool:
    """Only merge same-source narrative fragments when they mainly fill blanks, not rewrite facts."""
    fills_blank = False
    shared_values = 0
    conflicts = 0

    for header in headers:
        if header in key_headers:
            continue
        existing_value = clean_cell_value(existing.get("values", {}).get(header, ""))
        incoming_value = clean_cell_value(incoming.get("values", {}).get(header, ""))
        if not existing_value and incoming_value:
            fills_blank = True
            continue
        if existing_value and not incoming_value:
            fills_blank = True
            continue
        if not existing_value and not incoming_value:
            continue
        if existing_value == incoming_value:
            shared_values += 1
            continue
        conflicts += 1
        if conflicts >= 2:
            return False

    return fills_blank or shared_values > 0


def _semantic_key_value(header: str, value: Any) -> str:
    """Normalize entity/date keys so equivalent records from different sources can merge."""
    cleaned = clean_cell_value(str(value or ""))
    if not cleaned:
        return ""
    concept = _header_concept(header)
    if concept == "date":
        normalized, _status = _normalize_date_value(cleaned)
        return normalized.lower()
    if concept.startswith("entity"):
        normalized = cleaned.lower().replace(" ", "")
        for suffix in ("特别行政区", "自治区", "自治州", "地区", "省", "市", "盟", "县", "区"):
            if normalized.endswith(suffix.lower()) and len(normalized) > len(suffix) + 1:
                normalized = normalized[: -len(suffix)]
                break
        return normalized
    return cleaned.lower()


def _record_field_value_sources(record: dict, header: str) -> list[str]:
    """Return field-level value owners with a source_file fallback for legacy records."""
    sources = list(record.get("field_value_sources", {}).get(header, []))
    if sources:
        return _ordered_unique_strings(sources)
    value = clean_cell_value(record.get("values", {}).get(header, ""))
    source_file = clean_cell_value(str(record.get("source_file", "")))
    if value and source_file:
        return [source_file]
    return []


def _record_field_value_record_ids(record: dict, header: str) -> list[str]:
    """Return field-level raw record ids with a record_id fallback for legacy records."""
    record_ids = list(record.get("field_value_record_ids", {}).get(header, []))
    if record_ids:
        return _ordered_unique_strings(record_ids)
    value = clean_cell_value(record.get("values", {}).get(header, ""))
    record_id = clean_cell_value(str(record.get("record_id", "")))
    if value and record_id:
        return [record_id]
    return []


def _merge_field_value_provenance(existing: dict, incoming: dict, header: str):
    """Preserve every source that independently produced the chosen field value."""
    existing.setdefault("field_value_sources", {})
    existing.setdefault("field_value_record_ids", {})
    existing["field_value_sources"][header] = _ordered_unique_strings([
        *existing.get("field_value_sources", {}).get(header, []),
        *_record_field_value_sources(incoming, header),
    ])
    existing["field_value_record_ids"][header] = _ordered_unique_strings([
        *existing.get("field_value_record_ids", {}).get(header, []),
        *_record_field_value_record_ids(incoming, header),
    ])


def _resolve_conflicting_field_value(
    header: str,
    existing: dict,
    incoming: dict,
    requirement: RequirementSpec,
    use_llm: bool,
    usage_context: dict[str, Any] | None = None,
) -> str:
    """Choose between conflicting values from different sources."""
    existing_conf = existing.get("field_confidence", {}).get(header)
    incoming_conf = incoming.get("field_confidence", {}).get(header)
    existing_has_evidence = bool(existing.get("field_evidence", {}).get(header))
    incoming_has_evidence = bool(incoming.get("field_evidence", {}).get(header))

    if existing_conf is not None and incoming_conf is not None and abs(existing_conf - incoming_conf) >= 0.08:
        return "incoming" if incoming_conf > existing_conf else "existing"
    if incoming_has_evidence and not existing_has_evidence:
        return "incoming"
    if existing_has_evidence and not incoming_has_evidence:
        return "existing"

    existing_sources = {
        item.source_file
        for item in existing.get("field_evidence", {}).get(header, [])
        if item.source_file
    }
    incoming_sources = {
        item.source_file
        for item in incoming.get("field_evidence", {}).get(header, [])
        if item.source_file
    }
    if existing_sources and incoming_sources and existing_sources != incoming_sources:
        return _resolve_conflict_with_llm(
            header=header,
            existing=existing,
            incoming=incoming,
            requirement=requirement,
            use_llm=use_llm,
            usage_context=usage_context,
        )
    if incoming_conf is not None and (existing_conf is None or incoming_conf > existing_conf):
        return "incoming"
    return "existing"


def _resolve_conflict_with_llm(
    header: str,
    existing: dict,
    incoming: dict,
    requirement: RequirementSpec,
    use_llm: bool,
    usage_context: dict[str, Any] | None = None,
) -> str:
    """Ask qwen to resolve genuinely ambiguous multi-source field conflicts."""
    ollama = get_ollama_service()
    source_files = sorted({
        evidence.source_file
        for record in (existing, incoming)
        for evidence in record.get("field_evidence", {}).get(header, [])
        if evidence.source_file
    })
    llm_context = {
        **(usage_context or {}),
        "stage": "merge",
        "source_files": source_files,
    }
    ollama.mark_required_call("多源冲突字段需要 qwen 做语义裁决", llm_context)
    if not use_llm or not ollama.is_available:
        ollama.note_skip("检测到多源冲突，但 qwen 不可用", llm_context)
        raise RuntimeError(f"字段 {header} 存在多源冲突，必须由本地 qwen 语义裁决")

    existing_value = clean_cell_value(existing["values"].get(header, ""))
    incoming_value = clean_cell_value(incoming["values"].get(header, ""))
    existing_evidence = existing.get("field_evidence", {}).get(header, [])[:2]
    incoming_evidence = incoming.get("field_evidence", {}).get(header, [])[:2]
    prompt = (
        "请在两个有证据的候选值中选择更可信的一项。\n"
        "只输出 JSON 对象，不要解释。\n"
        "输出格式：{\"selected\":\"existing\"|\"incoming\"|\"unknown\"}。\n"
        f"字段：{header}\n"
        f"需求：{truncate_text(requirement.raw_text or '自动识别', 160)}\n"
        f"候选 A 值：{existing_value}\n"
        f"候选 A 证据：{' | '.join(f'{item.source_file}:{truncate_text(item.raw_snippet, 120)}' for item in existing_evidence) or '无'}\n"
        f"候选 B 值：{incoming_value}\n"
        f"候选 B 证据：{' | '.join(f'{item.source_file}:{truncate_text(item.raw_snippet, 120)}' for item in incoming_evidence) or '无'}\n"
    )
    parsed, error = ollama.generate_json(
        prompt,
        "你是多源证据裁决器，只能输出 JSON 对象。",
        num_predict=192,
        usage_context=llm_context,
    )
    if error or not isinstance(parsed, dict):
        raise RuntimeError(f"字段 {header} 的多源冲突裁决失败: {error or '空结果'}")
    selected = str(parsed.get("selected", "")).strip().lower()
    if selected == "incoming":
        return "incoming"
    return "existing"


def _estimate_entity_count(
    retrieval: RetrievalResult,
    candidate_row_estimates: list[int],
    records: list[dict],
    relevant_source_files: set[str] | None = None,
) -> int:
    """Estimate how many entities the source likely contains."""
    scoped_documents = [
        document
        for document in retrieval.source_docs
        if not relevant_source_files or document.source_file in relevant_source_files
    ]
    entity_hints = {
        _extract_leading_entity(segment)
        for document in scoped_documents
        for segment, _location in _candidate_text_segments(document)[:80]
        if _extract_leading_entity(segment)
    }
    if candidate_row_estimates:
        return max(max(candidate_row_estimates), len(entity_hints))
    if records:
        return max(len(records), len(entity_hints))
    if entity_hints:
        return len(entity_hints)
    if not _is_multi_entity_context(retrieval, []):
        return 0
    text = "\n".join(doc.raw_text[:3000] for doc in scoped_documents)
    bullet_count = len(re.findall(r'^(?:\s*(?:\d+[.)]|[-*•]))\s+', text, flags=re.MULTILINE))
    line_count = len(re.findall(r'^\s*[\u4e00-\u9fa5A-Za-z].{6,120}$', text, flags=re.MULTILINE))
    return max(bullet_count, min(line_count, 200))


def _detect_ranking_limit_with_context(
    records: list[dict],
    retrieval: RetrievalResult,
    requirement: RequirementSpec,
    template_context: dict[str, Any] | None = None,
) -> int:
    """Infer a stable top-N cap from template/requirement intent rather than incidental source titles."""
    del records, retrieval

    if requirement.sort_limit and requirement.sort_limit.get("top_n"):
        try:
            return max(1, int(requirement.sort_limit.get("top_n")))
        except (TypeError, ValueError):
            return 0

    corpus_parts = [requirement.raw_text]
    if template_context:
        corpus_parts.append(str(template_context.get("anchor_text", "")))
        corpus_parts.append(str(template_context.get("topic_text", "")))
        template_file = str(template_context.get("template_file", ""))
        if template_file:
            corpus_parts.append(Path(template_file).stem)
    corpus = " ".join(part for part in corpus_parts if part)

    match = re.search(r'(?:top|Top|TOP|前)\s*(\d{1,4})', corpus)
    if match:
        return max(1, int(match.group(1)))
    match = re.search(r'(\d{1,4})\s*(?:强|名|个)', corpus)
    if match:
        return max(1, int(match.group(1)))
    if "百强" in corpus or "百名" in corpus:
        return 100
    if "五十强" in corpus:
        return 50
    if "十强" in corpus:
        return 10
    return 0


def _apply_ranking_limit(
    records: list[dict],
    retrieval: RetrievalResult,
    requirement: RequirementSpec,
    warnings: list[str],
    template_context: dict[str, Any] | None = None,
) -> list[dict]:
    """Cap ranking-style outputs to the inferred Top-N size to avoid over-extraction."""
    ranking_limit = _detect_ranking_limit_with_context(
        records,
        retrieval,
        requirement,
        template_context=template_context,
    )
    if not ranking_limit or len(records) <= ranking_limit:
        return records
    warnings.append(
        f"检测到榜单型任务上限约为 {ranking_limit} 条，已从 {len(records)} 条候选中截取前 {ranking_limit} 条"
    )
    return records[:ranking_limit]


def _is_multi_entity_context(retrieval: RetrievalResult, headers: list[str]) -> bool:
    """Detect if the source context likely contains multiple distinct entities."""
    if any(len(candidate.get("filtered_rows", [])) > 1 for candidate in retrieval.table_candidates):
        return True

    title_corpus = " ".join(
        part
        for doc in retrieval.source_docs
        for part in [doc.source_file, str(doc.metadata.get("title", "")) if doc.metadata else ""]
        if part
    )
    if any(re.search(pattern, title_corpus, flags=re.IGNORECASE) for pattern in LIST_LIKE_TITLE_PATTERNS):
        return True

    entity_hints: set[str] = set()
    structured_blocks = 0
    list_like_blocks = 0
    for document in retrieval.source_docs:
        for segment, _location in _candidate_text_segments(document)[:80]:
            entity_hint = _extract_leading_entity(segment)
            if entity_hint:
                entity_hints.add(entity_hint)
            if _segment_has_structure(segment):
                structured_blocks += 1
            if re.match(LIST_ITEM_PATTERN, segment):
                list_like_blocks += 1

    entity_headers = [header for header in headers if _header_concept(header).startswith("entity")]
    if len(entity_hints) >= max(2, min(6, len(entity_headers) or 2)):
        return True
    if len(entity_hints) >= 2 and structured_blocks >= 3:
        return True
    if list_like_blocks >= 3 and structured_blocks >= 4:
        return True
    if structured_blocks >= 6 and len(retrieval.text_candidates) >= max(6, len(headers) * 2):
        return True
    return False


def _probe_source_coverage(
    headers: list[str],
    retrieval: RetrievalResult,
    requirement: RequirementSpec,
    usage_context: dict[str, Any] | None = None,
    template_context: dict[str, Any] | None = None,
    require_llm: bool = False,
) -> bool:
    """Run one bounded qwen probe per source to keep source-level usage observable."""
    ollama = get_ollama_service()
    llm_context = {
        **(usage_context or {}),
        "stage": "extract",
        "source_files": [doc.source_file for doc in retrieval.source_docs],
    }
    if not ollama.is_available:
        ollama.note_skip("source 语义探针未执行：Ollama/qwen 不可用", llm_context)
        return False

    if not retrieval.source_docs:
        return False

    ranked_docs = sorted(
        [
            document
            for document in retrieval.source_docs
            if _source_matches_template_context(
                document,
                headers,
                requirement,
                template_context=template_context,
            )
        ],
        key=lambda document: _source_context_score(
            document,
            headers,
            requirement,
            template_context=template_context,
        ),
        reverse=True,
    )
    probed = 0
    for document in ranked_docs:
        ranked_segments = _top_relevant_source_segments(
            document,
            headers,
            requirement,
            limit=4,
            template_context=template_context,
        )
        if not ranked_segments:
            ranked_segments = _fallback_probe_segments(document, limit=4)
        if not ranked_segments:
            continue
        chunk = ranked_segments[:4]
        if require_llm:
            ollama.mark_required_call(
                "叙事 source 语义校准必须真正经过 qwen",
                {
                    **(usage_context or {}),
                    "stage": "source_probe",
                    "source_file": document.source_file,
                },
            )
        _llm_extract_from_segments(
            headers=headers,
            requirement=requirement,
            source_file=document.source_file,
            chunk=chunk,
            usage_context={
                **(usage_context or {}),
                "stage": "source_probe",
                "source_file": document.source_file,
                "probe_only": True,
            },
        )
        logger.info(
            "Triggered narrative source probe via qwen on %s (%s segments)",
            Path(document.source_file).name,
            len(chunk),
        )
        probed += 1

    if probed == 0:
        ollama.note_skip("source 语义探针未找到可用上下文片段", llm_context)
    return probed > 0


def _fallback_probe_segments(document: DocumentBundle, limit: int = 4) -> list[tuple[str, str, int]]:
    """Build a tiny fallback context so tabular sources can also participate in probing."""
    segments: list[tuple[str, str, int]] = []
    for block in document.text_blocks[:2]:
        content = truncate_text(block.content.strip(), 700)
        if content:
            segments.append((content, f"text_block{block.block_index}", 1))
    if segments:
        return segments[:limit]

    for table in document.tables[:1]:
        headers = [clean_cell_value(header) for header in table.headers[:8] if clean_cell_value(header)]
        header_text = " | ".join(headers)
        for row_index, row in enumerate(table.rows[:3]):
            cells = [clean_cell_value(cell) for cell in row[:8] if clean_cell_value(cell)]
            if not cells:
                continue
            row_text = " | ".join(cells)
            snippet = f"{header_text}\n{row_text}" if header_text else row_text
            segments.append((truncate_text(snippet, 700), f"table{table.table_index}.row{row_index}", 1))
            if len(segments) >= limit:
                return segments
    return segments


def _needs_llm_backfill(
    records: list[dict],
    entity_estimate: int,
    retrieval: RetrievalResult,
    headers: list[str],
    use_llm: bool,
) -> bool:
    """Decide when an LLM fallback is justified."""
    if not use_llm:
        return False
    if not records:
        return True
    if not _is_multi_entity_context(retrieval, headers):
        return False
    if entity_estimate <= 1:
        return False
    return len(records) < max(2, int(entity_estimate * 0.6))


def _llm_extract_from_segments(
    headers: list[str],
    requirement: RequirementSpec,
    source_file: str,
    chunk: list[tuple[str, str, int]],
    usage_context: dict[str, Any] | None = None,
) -> list[list[str]]:
    """Run qwen on a chunk of narrative segments from one source."""
    ollama = get_ollama_service()
    if not ollama.is_available:
        return []
    header_str = ", ".join(headers)
    requirement_text = truncate_text(requirement.raw_text or "", 160)
    context = truncate_text(
        "\n\n".join(f"[{location}] {segment}" for segment, location, _ in chunk),
        2600,
    )
    prompt = (
        "按表头从叙事分段中抽取多条记录。\n"
        f"表头:[{header_str}]\n"
        f"需求:{requirement_text or '自动识别'}\n"
        f"来源:{Path(source_file).name}\n"
        "要求：每条记录只能对应一个实体，不能跨段拼接；没有证据的字段填 null；只输出 JSON 数组。\n"
        f"分段内容:\n{context}"
    )
    parsed, error = ollama.generate_json(
        prompt,
        "你是结构化抽取器，只能输出 JSON 数组，不要解释。",
        num_predict=768,
        usage_context=usage_context,
    )
    if error or not parsed:
        logger.warning("Narrative qwen extraction failed for %s: %s", Path(source_file).name, error)
        return []
    return _json_to_rows(parsed, headers)


def _llm_extract_multi_entity(
    headers: list[str],
    retrieval: RetrievalResult,
    requirement: RequirementSpec,
    usage_context: dict[str, Any] | None = None,
    template_context: dict[str, Any] | None = None,
) -> list[list[str]]:
    """Use the LLM to extract many entities from ranking or list-like documents."""
    ollama = get_ollama_service()
    if not ollama.is_available:
        ollama.note_skip("Ollama/qwen 不可用，无法进行多实体补抽", {
            **(usage_context or {}),
            "stage": "extract",
            "source_files": [doc.source_file for doc in retrieval.source_docs],
        })
        logger.warning("LLM not available for multi-entity extraction")
        return []

    blocks = _top_relevant_blocks(
        headers,
        retrieval,
        requirement,
        limit=12,
        template_context=template_context,
    )
    if not blocks:
        return []

    header_str = ", ".join(headers)
    requirement_text = truncate_text(requirement.raw_text or "", 160)
    system_prompt = "你是结构化抽取器，只能输出 JSON 数组，不要解释。"

    all_rows: list[list[str]] = []
    seen_keys: set[tuple[str, ...]] = set()

    for chunk_start in range(0, len(blocks), 4):
        chunk = blocks[chunk_start: chunk_start + 4]
        context = truncate_text("\n".join(chunk), 2200)
        prompt = (
            f"按表头抽取多条记录。\n"
            f"表头:[{header_str}]\n"
            f"需求:{requirement_text or '自动识别'}\n"
            "只输出 JSON 数组，字段缺失填 null。\n"
            f"文本:\n{context}"
        )
        parsed, error = ollama.generate_json(
            prompt,
            system_prompt,
            num_predict=768,
            usage_context={
                **(usage_context or {}),
                "stage": "extract",
                "source_files": [doc.source_file for doc in retrieval.source_docs],
            },
        )
        if error or not parsed:
            logger.warning("Chunk %s multi-entity extraction failed: %s", chunk_start, error)
            continue
        for row in _json_to_rows(parsed, headers):
            signature = tuple(cell.strip() for cell in row)
            if any(signature) and signature not in seen_keys:
                seen_keys.add(signature)
                all_rows.append(row)
    return all_rows


def _llm_extract_single(
    headers: list[str],
    retrieval: RetrievalResult,
    requirement: RequirementSpec,
    usage_context: dict[str, Any] | None = None,
    template_context: dict[str, Any] | None = None,
) -> list[list[str]]:
    """Use the LLM to extract a small number of rows from text evidence."""
    ollama = get_ollama_service()
    if not ollama.is_available:
        ollama.note_skip("Ollama/qwen 不可用，无法进行单表补抽", {
            **(usage_context or {}),
            "stage": "extract",
            "source_files": [doc.source_file for doc in retrieval.source_docs],
        })
        return []

    context_parts = _top_relevant_blocks(
        headers,
        retrieval,
        requirement,
        limit=8,
        template_context=template_context,
    )
    if not context_parts:
        return []

    header_str = ", ".join(headers)
    requirement_text = truncate_text(requirement.raw_text or "", 160)
    context = truncate_text("\n---\n".join(context_parts), 2200)
    system_prompt = "你是结构化抽取器，只能输出 JSON 数组，不要解释。"
    prompt = (
        f"请按表头抽取记录。\n表头:[{header_str}]\n需求:{requirement_text or '自动识别'}\n"
        "只输出 JSON 数组，字段缺失填 null。\n"
        f"文本:\n{context}"
    )
    parsed, error = ollama.generate_json(
        prompt,
        system_prompt,
        num_predict=640,
        usage_context={
            **(usage_context or {}),
            "stage": "extract",
            "source_files": [doc.source_file for doc in retrieval.source_docs],
        },
    )
    if error or not parsed:
        logger.warning("LLM single extraction failed: %s", error)
        return []
    return _json_to_rows(parsed, headers)


def _top_relevant_blocks(
    headers: list[str],
    retrieval: RetrievalResult,
    requirement: RequirementSpec,
    limit: int,
    template_context: dict[str, Any] | None = None,
) -> list[str]:
    """Pick the most relevant narrative blocks for a template table."""
    scored: list[tuple[int, str]] = []
    for document in retrieval.source_docs:
        for segment, _ in _candidate_text_segments(document):
            if not _segment_is_relevant(
                segment,
                headers,
                requirement,
                template_context=template_context,
            ):
                continue
            score = 0
            lowered = segment.lower()
            for header in headers:
                for alias in _header_aliases(header):
                    if alias.lower() in lowered:
                        score += 2
                        break
            if re.search(NUMBER_PATTERN, segment):
                score += 1
            if _extract_leading_entity(segment):
                score += 1
            if score > 0:
                scored.append((score, segment))
    scored.sort(key=lambda item: item[0], reverse=True)
    return [segment for _, segment in scored[:limit]]


def _apply_table_spec_filter(src_table: NormalizedTable, rows: list[list[str]], tmpl_table_idx: int, requirement: RequirementSpec) -> list[list[str]]:
    """Apply per-table filters from requirement.table_specs."""
    if not requirement.table_specs:
        return rows
    spec = next((item for item in requirement.table_specs if item.get("table_index") == tmpl_table_idx), None)
    if not spec:
        return rows

    header_index = {header: index for index, header in enumerate(src_table.headers)}
    filtered = rows
    for key, value in spec.items():
        if key == "table_index":
            continue
        matched_col = best_column_match(key, src_table.headers)
        if matched_col and matched_col in header_index:
            cell_index = header_index[matched_col]
            narrowed = [
                row for row in filtered
                if cell_index < len(row) and (
                    row[cell_index].strip() == value
                    or value in row[cell_index].strip()
                    or row[cell_index].strip() in value
                    or _fuzzy_date_match(row[cell_index].strip(), value)
                )
            ]
            if narrowed:
                filtered = narrowed
    return filtered


def _fuzzy_date_match(cell_val: str, target_val: str) -> bool:
    """Check whether two date strings refer to the same time."""
    if not cell_val or not target_val:
        return False

    def normalize(value: str) -> str:
        return re.sub(r'\.0$', '', value.strip()).replace('/', '-').replace('.', '-')

    return normalize(cell_val) == normalize(target_val)


def _json_to_rows(parsed: Any, headers: list[str]) -> list[list[str]]:
    """Convert LLM JSON output to row arrays."""
    if isinstance(parsed, list):
        rows = []
        for item in parsed:
            if not isinstance(item, dict):
                continue
            row = []
            for header in headers:
                value = item.get(header)
                if value is None:
                    for key, candidate in item.items():
                        if header and key and (header in key or key in header or similarity(header, key) > 0.60):
                            value = candidate
                            break
                row.append(clean_cell_value(value) if value is not None else "")
            rows.append(row)
        return rows
    if isinstance(parsed, dict):
        return [[clean_cell_value(parsed.get(header, "")) for header in headers]]
    return []
