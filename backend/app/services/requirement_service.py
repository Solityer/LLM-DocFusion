"""Requirement parsing and auto-inference service."""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from ..core.logging import logger
from ..schemas.models import FileRole, RequirementSpec
from .document_service import read_document
from .ollama_service import get_ollama_service
from .template_service import parse_template


REQUIREMENT_FILENAMES = [
    "用户要求.txt",
    "requirement.txt",
    "需求.txt",
    "说明.txt",
    "README.txt",
]

TIME_KEYWORDS = {"日期", "时间", "年月", "年份", "月份", "period", "date", "time"}
ENTITY_KEYWORDS = {"城市", "地区", "省份", "国家", "名称", "机构", "企业", "单位", "city", "country"}
GENERIC_INFERRED_TOKENS = {"sheet", "data", "table", "tables", "sheet1", "sheet2", "工作表", "数据", "指标关键词"}


def parse_requirement(text: str, strict_mode: bool = False) -> RequirementSpec:
    """Parse user requirement text into a structured RequirementSpec."""
    logger.info("Parsing requirement: %s", (text or "")[:100])

    spec = RequirementSpec(raw_text=(text or "").strip())
    spec.strict_matching = bool(strict_mode)

    if not text or not text.strip():
        spec.warnings.append("requirement 为空，需依赖自动推断")
        return spec

    text = text.strip()

    _extract_table_specs(text, spec)
    _extract_time_range(text, spec)
    _extract_entities(text, spec)
    _extract_indicators(text, spec)
    _extract_filters(text, spec)
    _extract_sort_limit(text, spec)
    _extract_output_granularity(text, spec)
    _extract_strict_matching(text, spec)
    _extract_special_notes(text, spec)

    if any(note in {"严格匹配", "精确匹配", "禁止猜测", "不要猜测"} for note in spec.special_notes):
        spec.strict_matching = True

    if not spec.output_granularity:
        spec.output_granularity = "逐条记录" if spec.sort_limit or spec.table_specs else "按模板字段填充"

    logger.info(
        "  -> time_range=%s, entities=%s, indicators=%s, filters=%s, granularity=%s",
        spec.time_range,
        spec.entity_keywords,
        spec.indicator_keywords,
        spec.filters,
        spec.output_granularity,
    )
    return spec


def _extract_time_range(text: str, spec: RequirementSpec):
    range_patterns = [
        r'(\d{4})[/年.-](\d{1,2})[/月.-](\d{1,2})[日号]?\s*[到至~－\-]\s*(\d{4})[/年.-](\d{1,2})[/月.-](\d{1,2})[日号]?',
        r'从?\s*(\d{4})[/年.-](\d{1,2})[/月.-](\d{1,2})[日号]?\s*[到至~－\-]\s*(\d{4})[/年.-](\d{1,2})[/月.-](\d{1,2})[日号]?',
    ]
    for pat in range_patterns:
        m = re.search(pat, text)
        if m:
            g = m.groups()
            spec.time_range = [
                f"{g[0]}-{g[1].zfill(2)}-{g[2].zfill(2)}",
                f"{g[3]}-{g[4].zfill(2)}-{g[5].zfill(2)}",
            ]
            return

    ym = re.search(r'(\d{4})[年/.-](\d{1,2})[月]?', text)
    if ym and not spec.time_range:
        spec.time_range = [
            f"{ym.group(1)}-{ym.group(2).zfill(2)}-01",
            f"{ym.group(1)}-{ym.group(2).zfill(2)}-31",
        ]


def _extract_table_specs(text: str, spec: RequirementSpec):
    """Extract per-table specs like '表一：城市：德州市'."""
    table_pattern = r'表[一二三四五六七八九十\d]+[：:]'
    parts = re.split(f'({table_pattern})', text)

    table_idx = 0
    for i, part in enumerate(parts):
        if re.match(table_pattern, part):
            content = parts[i + 1] if i + 1 < len(parts) else ""
            table_spec = {"table_index": table_idx}
            for line in content.strip().split('\n'):
                line = line.strip()
                if not line:
                    continue
                kv_match = re.match(r'^([^\s：:]{1,20})[：:]\s*(.+)$', line)
                if kv_match:
                    key = kv_match.group(1).strip()
                    val = kv_match.group(2).strip()
                    table_spec[key] = val

            if len(table_spec) > 1:
                spec.table_specs.append(table_spec)
                for key, value in table_spec.items():
                    if key != "table_index" and key not in TIME_KEYWORDS and value not in spec.entity_keywords:
                        spec.entity_keywords.append(value)
            table_idx += 1


def _extract_entities(text: str, spec: RequirementSpec):
    """Extract entity keywords (cities, organizations, etc.)."""
    entity_patterns = [
        r'(?:实体范围|实体关键词|实体)[：:]\s*([^\n。]+)',
        r'(?:城市|地区|区域|省份|国家|机构|单位)[：:]\s*([^\n，。；]+)',
        r'(?:关于|针对|涉及)[：:]?\s*([^\n，。]+)',
    ]
    for pat in entity_patterns:
        for m in re.finditer(pat, text):
            for item in re.split(r'[,，、;；]', m.group(1).strip()):
                item = item.strip()
                if item and len(item) <= 30 and item not in spec.entity_keywords:
                    spec.entity_keywords.append(item)


def _extract_indicators(text: str, spec: RequirementSpec):
    """Extract indicator/metric keywords."""
    indicator_patterns = [
        r'(?:指标关键词|指标范围|关键词)[：:]?\s*([^\n。]+)',
        r'(?:提取|统计|分析|计算|汇总|填写)[：:]?\s*([^\n。]+)',
        r'(?:指标|数据项|字段|表头)[：:]?\s*([^\n。]+)',
    ]
    for pat in indicator_patterns:
        for m in re.finditer(pat, text):
            for item in re.split(r'[,，、;；]', m.group(1).strip()):
                item = item.strip()
                if item and len(item) <= 40 and item not in spec.indicator_keywords:
                    spec.indicator_keywords.append(item)


def _extract_filters(text: str, spec: RequirementSpec):
    """Extract explicit filter conditions."""
    filter_pattern = r'(?:筛选条件|筛选|过滤|条件)[：:]?\s*([^\n。]+)'
    for m in re.finditer(filter_pattern, text):
        val = m.group(1).strip()
        if '=' in val or '：' in val or ':' in val:
            kv = re.split(r'[=：:]', val, maxsplit=1)
            if len(kv) == 2:
                key = kv[0].strip()
                values = [v.strip() for v in re.split(r'[,，、]', kv[1]) if v.strip()]
                if key and values:
                    spec.filters[key] = values

    datetime_value_pattern = (
        r'((?:19|20)\d{2}[\\/年.\-](?:1[0-2]|0?\d)[\\/月.\-](?:3[01]|[12]\d|0?\d)'
        r'(?:\s+\d{1,2}:\d{2}(?::\d{2}(?:\.\d+)?)?)?)'
    )
    time_filter_patterns = [
        rf'(监测时间|统计时间|填报时间|日期|时间)\s*(?:为|=|：|:)\s*{datetime_value_pattern}',
        rf'([^\s，。；:：]{{1,20}})\s*(?:为|=|：|:)\s*{datetime_value_pattern}',
    ]
    for pattern in time_filter_patterns:
        for key, value in re.findall(pattern, text):
            normalized_key = re.sub(r'^(?:将|按|把|根据|针对|其中)', '', key.strip())
            normalized_value = value.strip()
            if not normalized_key or not normalized_value:
                continue
            spec.filters.setdefault(normalized_key, [])
            if normalized_value not in spec.filters[normalized_key]:
                spec.filters[normalized_key].append(normalized_value)


def _extract_sort_limit(text: str, spec: RequirementSpec):
    """Extract sorting and limit conditions."""
    top_match = re.search(r'(?:前|Top|top)\s*(\d+)', text)
    if top_match:
        spec.sort_limit = {"top_n": int(top_match.group(1))}

    sort_match = re.search(r'(?:按|根据)\s*(.+?)\s*(?:排序|排列|降序|升序)', text)
    if sort_match:
        if spec.sort_limit is None:
            spec.sort_limit = {}
        spec.sort_limit["sort_by"] = sort_match.group(1).strip()
        spec.sort_limit["order"] = "desc" if ('降序' in text or '从高到低' in text) else "asc"


def _extract_output_granularity(text: str, spec: RequirementSpec):
    """Infer whether the user wants row-level or summary output."""
    if any(token in text for token in ["逐条", "逐行", "明细", "每个实体", "排行榜", "名单"]):
        spec.output_granularity = "逐条记录"
    elif any(token in text for token in ["汇总", "总计", "总览", "摘要"]):
        spec.output_granularity = "汇总输出"


def _extract_strict_matching(text: str, spec: RequirementSpec):
    """Extract explicit strict matching preference."""
    match = re.search(r'严格匹配[：:]\s*(是|否|true|false|yes|no)', text, flags=re.IGNORECASE)
    if not match:
        return
    value = match.group(1).strip().lower()
    spec.strict_matching = value in {"是", "true", "yes"}


def _extract_special_notes(text: str, spec: RequirementSpec):
    """Extract special instructions."""
    note_keywords = ['严格匹配', '允许留空', '禁止猜测', '不要猜测', '精确匹配', '模糊匹配', '不要留空', '必须填写']
    for kw in note_keywords:
        if kw in text and kw not in spec.special_notes:
            spec.special_notes.append(kw)


def auto_load_requirement(template_paths: list[str], source_paths: list[str]) -> tuple[str, list[str], list[str]]:
    """Load explicit requirement text from nearby files when available."""
    warnings: list[str] = []
    seen_dirs: set[Path] = set()

    search_dirs: list[Path] = []
    for path_str in [*template_paths, *source_paths]:
        path = Path(path_str)
        for candidate in [path.parent, path.parent.parent]:
            if candidate and candidate.exists() and candidate not in seen_dirs:
                search_dirs.append(candidate)
                seen_dirs.add(candidate)

    for directory in search_dirs:
        for filename in REQUIREMENT_FILENAMES:
            requirement_file = directory / filename
            if not requirement_file.exists():
                continue
            try:
                text = requirement_file.read_text(encoding="utf-8").strip()
            except UnicodeDecodeError:
                text = requirement_file.read_text(encoding="utf-8", errors="ignore").strip()
            except Exception as exc:
                warnings.append(f"读取 requirement 文件失败: {requirement_file.name}: {exc}")
                continue
            if text:
                logger.info("Auto-loaded requirement from: %s", requirement_file)
                return text, [str(requirement_file)], warnings

    warnings.append("未找到显式 requirement 文件，转为结构推断")
    return "", [], warnings


def auto_infer_requirement(
    template_paths: list[str],
    source_paths: list[str],
    use_llm: bool = True,
    usage_context: dict[str, Any] | None = None,
) -> tuple[RequirementSpec, str, list[str]]:
    """Infer a requirement from nearby files, template structure, and source content."""
    if not use_llm:
        raise RuntimeError("requirement 为空时必须启用本地 Ollama qwen2.5:14b 进行语义推断")

    explicit_text, inferred_from, warnings = auto_load_requirement(template_paths, source_paths)
    if explicit_text:
        base_spec = parse_requirement(explicit_text)
        spec, llm_text, llm_warning = _parse_requirement_with_llm(
            explicit_text,
            template_paths=template_paths,
            source_paths=source_paths,
            use_llm=use_llm,
            usage_context=usage_context,
            base_spec=base_spec,
        )
        if spec is None:
            warning = llm_warning or "qwen requirement 结构化失败，已显式降级为规则解析"
            warnings.append(warning)
            _record_requirement_degradation(usage_context, warning)
            spec = base_spec.model_copy(deep=True)
            llm_text = llm_text or explicit_text
        if llm_warning:
            warnings.append(llm_warning)
        spec.inferred_from = inferred_from
        spec.warnings.extend(warnings)
        return spec, llm_text or explicit_text, warnings

    template_headers: list[str] = []
    template_fields: list[str] = []
    source_titles: list[str] = []
    source_headers: list[str] = []
    time_candidates: list[str] = []
    entity_candidates: list[str] = []
    indicator_candidates: list[str] = []

    for template_path in template_paths[:5]:
        try:
            schema = parse_template(template_path)
        except Exception as exc:
            warnings.append(f"模板结构推断失败: {Path(template_path).name}: {exc}")
            continue

        template_headers.extend(header for table in schema.tables for header in table.headers if header)
        template_fields.extend(field.field_name for field in schema.fields if field.field_name)
        inferred_from.append(f"template:{Path(template_path).name}")

    template_stems = [Path(path).stem for path in template_paths]
    template_signal_text = " ".join([*template_headers, *template_fields, *template_stems])
    template_name_tokens = {
        token.lower()
        for stem in template_stems
        for token in re.findall(r'[\u4e00-\u9fa5A-Za-z0-9]{2,20}', stem)
        if _is_meaningful_inferred_token(token)
    }

    for source_path in source_paths[:6]:
        try:
            doc = read_document(source_path, FileRole.SOURCE)
        except Exception as exc:
            warnings.append(f"source 结构推断失败: {Path(source_path).name}: {exc}")
            continue

        if template_signal_text and len(source_paths) > 1:
            relevance = _source_relevance(doc, template_signal_text)
            source_name_text = " ".join([Path(source_path).stem, doc.metadata.get("title", "") if doc.metadata else ""])
            source_name_tokens = {
                token.lower()
                for token in re.findall(r'[\u4e00-\u9fa5A-Za-z0-9]{2,20}', source_name_text)
                if _is_meaningful_inferred_token(token)
            }
            name_overlap = len(template_name_tokens & source_name_tokens)
            if name_overlap == 0 and relevance < 3:
                continue

        source_titles.append(_guess_document_title(doc.raw_text, Path(source_path).stem))
        inferred_from.append(f"source:{Path(source_path).name}")
        if doc.text_blocks:
            first_block = doc.text_blocks[0].content.strip()
            if first_block and first_block not in source_titles:
                source_titles.append(first_block[:80])

        for table in doc.tables[:6]:
            for header in table.headers:
                if not header:
                    continue
                source_headers.append(header)
                if any(keyword in header for keyword in TIME_KEYWORDS):
                    time_candidates.append(header)
                elif any(keyword in header for keyword in ENTITY_KEYWORDS):
                    entity_candidates.append(header)
                else:
                    indicator_candidates.append(header)

        time_range = _find_time_range_in_text(doc.raw_text)
        if time_range and time_range not in time_candidates:
            time_candidates.append(time_range)

    dedup_template_headers = _deduplicate(template_headers)
    dedup_source_headers = _deduplicate(source_headers)
    dedup_entities = _deduplicate([
        item for item in entity_candidates + _extract_name_like_titles(source_titles)
        if _is_meaningful_inferred_token(item)
    ])[:10]
    dedup_indicators = _deduplicate(
        [
            item for item in template_fields + dedup_template_headers + indicator_candidates
            if item and item not in TIME_KEYWORDS and _is_meaningful_inferred_token(item)
        ]
    )[:20]

    lines = []
    time_line = next(
        (
            item for item in time_candidates
            if re.search(r'\d{4}', item)
            and ('到' in item or '至' in item or re.search(r'\d{4}.*\d{4}', item))
        ),
        None,
    )
    if not time_line and time_candidates:
        time_line = time_candidates[0]
    lines.append(f"时间范围：{time_line or '未明确，优先采用数据源最新且与模板匹配的时间范围'}")

    entity_line = "、".join(dedup_entities[:8]) if dedup_entities else "全部可识别实体"
    indicator_line = "、".join(dedup_indicators[:12]) if dedup_indicators else "按模板字段自动匹配"
    output_line = "按模板分别输出，且每个模板独立生成结果" if len(template_paths) > 1 else "逐条记录"

    lines.append(f"实体范围：{entity_line}")
    lines.append(f"指标关键词：{indicator_line}")
    lines.append("筛选条件：默认过滤=按模板字段、来源主题和时间范围综合匹配")
    lines.append(f"输出粒度：{output_line}")
    lines.append("严格匹配：否")

    if dedup_source_headers and not dedup_indicators:
        lines.append("候选表头：" + "、".join(dedup_source_headers[:12]))

    heuristic_text = "\n".join(line for line in lines if line).strip()
    inferred_text = heuristic_text
    if not inferred_text:
        inferred_text = (
            f"请根据数据源文件（{', '.join(Path(p).stem for p in source_paths) or '未命名数据源'}）"
            f"填写模板（{', '.join(Path(p).stem for p in template_paths) or '未命名模板'}）中的可匹配字段，"
            "无法确认的字段留空并标注无证据。"
        )
        warnings.append("自动 requirement 推断信息不足，已回退到通用说明")

    heuristic_spec = parse_requirement(inferred_text)
    llm_spec, llm_text, llm_warning = _infer_requirement_with_llm(
        template_paths=template_paths,
        source_paths=source_paths,
        template_headers=dedup_template_headers,
        template_fields=template_fields,
        source_titles=source_titles,
        source_headers=dedup_source_headers,
        heuristic_text=inferred_text,
        use_llm=use_llm,
        usage_context=usage_context,
        base_spec=heuristic_spec,
    )
    if llm_spec is not None:
        spec = llm_spec
        inferred_text = llm_text or inferred_text
    else:
        warning = llm_warning or "qwen requirement 推断失败，已显式降级为启发式规则"
        warnings.append(warning)
        _record_requirement_degradation(usage_context, warning)
        spec = heuristic_spec.model_copy(deep=True)
    if llm_warning:
        warnings.append(llm_warning)
    spec.inferred_from = inferred_from
    spec.warnings.extend(warnings)

    if not spec.indicator_keywords and dedup_indicators:
        spec.indicator_keywords = dedup_indicators[:12]
    if not spec.entity_keywords and dedup_entities:
        spec.entity_keywords = dedup_entities[:8]
    if not spec.output_granularity:
        spec.output_granularity = "逐条记录" if len(template_paths) <= 1 else "按模板分别输出"
    if not spec.filters:
        spec.filters["默认过滤"] = ["按模板字段、来源主题和时间范围综合匹配"]
    if spec.strict_matching is None:
        spec.strict_matching = False

    if not spec.time_range and warnings:
        spec.warnings.append("未能可靠识别时间范围，将按模板字段和 source 内容综合匹配")

    return spec, inferred_text, warnings


def _parse_requirement_with_llm(
    explicit_text: str,
    template_paths: list[str],
    source_paths: list[str],
    use_llm: bool,
    usage_context: dict[str, Any] | None,
    base_spec: RequirementSpec | None = None,
) -> tuple[RequirementSpec | None, str, str]:
    """Use qwen to normalize an explicit nearby requirement file into a structured spec."""
    if not explicit_text.strip():
        return None, "", ""

    ollama = get_ollama_service()
    llm_context = {
        **(usage_context or {}),
        "stage": "requirement",
        "source_files": source_paths,
        "template_files": template_paths,
    }
    ollama.mark_required_call("显式 requirement 文件需要 qwen 做结构化解析", llm_context)
    if not use_llm:
        ollama.note_skip("LLM 增强关闭，附近 requirement 文件改用规则解析", llm_context)
        return None, explicit_text, "requirement 文件未经过 qwen 结构化，已回退规则解析"
    if not ollama.is_available:
        ollama.note_skip("Ollama/qwen 不可用，附近 requirement 文件改用规则解析", llm_context)
        return None, explicit_text, "qwen requirement 结构化失败，已回退规则解析"

    system_prompt = "你是需求结构化解析器，只能输出 JSON 对象，不要解释。"
    prompt = (
        "将下面的填表需求解析为 JSON。\n"
        "必须输出键：time_range, entity_keywords, indicator_keywords, filters, table_specs, output_granularity, strict_matching, summary。\n"
        "time_range 为 [start, end] 或 null，日期用 YYYY-MM-DD。\n"
        "entity_keywords / indicator_keywords 为字符串数组，filters 为对象。\n"
        "如果需求中包含“表一/表二/表三...”之类的分表约束，table_specs 输出数组，元素形如"
        "{\"table_index\":0,\"字段A\":\"值A\",\"字段B\":\"值B\"}。\n"
        "summary 必须是中文多行文本，且必须包含以下 6 行：\n"
        "时间范围：...\n实体范围：...\n指标关键词：...\n筛选条件：...\n输出粒度：...\n严格匹配：...\n"
        f"需求原文：\n{explicit_text[:2000]}"
    )
    parsed, error = ollama.generate_json(
        prompt,
        system_prompt,
        num_predict=512,
        usage_context=llm_context,
    )
    if error or not isinstance(parsed, dict):
        return None, explicit_text, f"qwen requirement 结构化失败，已回退规则解析: {error or '空结果'}"
    spec, normalized_text = _spec_from_llm_payload(parsed, fallback_text=explicit_text, base_spec=base_spec)
    return spec, normalized_text, ""


def _infer_requirement_with_llm(
    template_paths: list[str],
    source_paths: list[str],
    template_headers: list[str],
    template_fields: list[str],
    source_titles: list[str],
    source_headers: list[str],
    heuristic_text: str,
    use_llm: bool,
    usage_context: dict[str, Any] | None,
    base_spec: RequirementSpec | None = None,
) -> tuple[RequirementSpec | None, str, str]:
    """Infer requirement via qwen from template/source signals."""
    ollama = get_ollama_service()
    llm_context = {
        **(usage_context or {}),
        "stage": "requirement",
        "source_files": source_paths,
        "template_files": template_paths,
    }
    ollama.mark_required_call("空 requirement 需要 qwen 基于模板与来源做语义推断", llm_context)
    if not use_llm:
        ollama.note_skip("LLM 增强关闭，自动 requirement 改用启发式推断", llm_context)
        return None, heuristic_text, "自动 requirement 未经过 qwen 推断，已回退启发式规则"
    if not ollama.is_available:
        ollama.note_skip("Ollama/qwen 不可用，自动 requirement 改用启发式推断", llm_context)
        return None, heuristic_text, "qwen requirement 推断失败，已回退启发式规则"

    template_names = "、".join(Path(path).stem for path in template_paths[:5]) or "未命名模板"
    source_names = "、".join(Path(path).stem for path in source_paths[:8]) or "未命名数据源"
    prompt = (
        "根据模板和数据源信号，推断一次填表任务的 requirement。\n"
        "只输出 JSON 对象，不要解释。\n"
        "必须输出键：time_range, entity_keywords, indicator_keywords, filters, table_specs, output_granularity, strict_matching, summary。\n"
        "summary 必须是中文多行文本，且必须包含以下 6 行：\n"
        "时间范围：...\n实体范围：...\n指标关键词：...\n筛选条件：...\n输出粒度：...\n严格匹配：...\n"
        "table_specs 若无法确定则输出空数组 []。\n"
        "推断要求：保守、可验证、不要编造不存在的实体；不确定就写“未明确”或留空。\n"
        f"模板名：{template_names}\n"
        f"模板表头：{'、'.join(template_headers[:24]) or '无'}\n"
        f"模板字段：{'、'.join(template_fields[:24]) or '无'}\n"
        f"数据源名：{source_names}\n"
        f"数据源标题：{'、'.join(source_titles[:12]) or '无'}\n"
        f"数据源表头：{'、'.join(source_headers[:24]) or '无'}\n"
        f"启发式参考：\n{heuristic_text[:1800]}"
    )
    parsed, error = ollama.generate_json(
        prompt,
        "你是需求推断器，只能输出 JSON 对象，不要解释。",
        num_predict=640,
        usage_context=llm_context,
    )
    if error or not isinstance(parsed, dict):
        return None, heuristic_text, f"qwen requirement 推断失败，已回退启发式规则: {error or '空结果'}"
    spec, normalized_text = _spec_from_llm_payload(parsed, fallback_text=heuristic_text, base_spec=base_spec)
    return spec, normalized_text, ""


def _spec_from_llm_payload(
    payload: dict[str, Any],
    fallback_text: str,
    base_spec: RequirementSpec | None = None,
) -> tuple[RequirementSpec, str]:
    """Convert qwen JSON payload to RequirementSpec plus normalized summary text."""
    summary = payload.get("summary")
    if not isinstance(summary, str) or "时间范围" not in summary:
        summary = _build_requirement_summary_from_payload(payload, fallback_text)
    summary_spec = parse_requirement(summary)
    spec = base_spec.model_copy(deep=True) if base_spec is not None else summary_spec
    if base_spec is not None:
        if not spec.time_range and summary_spec.time_range:
            spec.time_range = summary_spec.time_range
        if not spec.entity_keywords and summary_spec.entity_keywords:
            spec.entity_keywords = summary_spec.entity_keywords
        if not spec.indicator_keywords and summary_spec.indicator_keywords:
            spec.indicator_keywords = summary_spec.indicator_keywords
        if not spec.filters and summary_spec.filters:
            spec.filters = summary_spec.filters
        if not spec.output_granularity and summary_spec.output_granularity:
            spec.output_granularity = summary_spec.output_granularity
        if not spec.special_notes and summary_spec.special_notes:
            spec.special_notes = summary_spec.special_notes
    time_range = payload.get("time_range")
    if isinstance(time_range, list) and len(time_range) == 2 and all(isinstance(item, str) and item for item in time_range):
        spec.time_range = [time_range[0], time_range[1]]
    elif isinstance(time_range, str) and time_range.strip():
        inferred_time = parse_requirement(f"时间范围：{time_range.strip()}").time_range
        if inferred_time:
            spec.time_range = inferred_time
    entity_keywords = _normalize_payload_list(payload.get("entity_keywords"))
    indicator_keywords = _normalize_payload_list(payload.get("indicator_keywords"))
    if entity_keywords:
        spec.entity_keywords = _deduplicate(entity_keywords)
    if indicator_keywords:
        spec.indicator_keywords = _deduplicate(indicator_keywords)
    filters = payload.get("filters")
    if isinstance(filters, dict):
        normalized_filters: dict[str, list[str]] = {}
        for key, value in filters.items():
            if isinstance(value, list):
                normalized_filters[str(key)] = [str(item).strip() for item in value if str(item).strip()]
            elif value not in (None, ""):
                normalized_filters[str(key)] = [str(value).strip()]
        if normalized_filters:
            merged_filters = {key: list(values) for key, values in spec.filters.items()}
            for key, values in normalized_filters.items():
                merged_filters.setdefault(key, [])
                for value in values:
                    if value not in merged_filters[key]:
                        merged_filters[key].append(value)
            spec.filters = merged_filters
    elif isinstance(filters, (list, tuple, set, str)):
        normalized_filters = _normalize_payload_list(filters)
        if normalized_filters:
            merged_filters = {key: list(values) for key, values in spec.filters.items()}
            merged_filters.setdefault("默认过滤", [])
            for value in normalized_filters:
                if value not in merged_filters["默认过滤"]:
                    merged_filters["默认过滤"].append(value)
            spec.filters = merged_filters
    table_specs = payload.get("table_specs")
    if isinstance(table_specs, list):
        normalized_specs: list[dict[str, Any]] = []
        for item in table_specs:
            if not isinstance(item, dict):
                continue
            normalized_item: dict[str, Any] = {}
            for key, value in item.items():
                normalized_key = str(key).strip()
                if not normalized_key:
                    continue
                if normalized_key == "table_index":
                    try:
                        normalized_item[normalized_key] = int(value)
                    except (TypeError, ValueError):
                        normalized_item[normalized_key] = value
                elif value not in (None, ""):
                    normalized_item[normalized_key] = str(value).strip()
            if normalized_item:
                normalized_specs.append(normalized_item)
        if normalized_specs:
            spec.table_specs = normalized_specs
    if payload.get("output_granularity"):
        spec.output_granularity = str(payload["output_granularity"]).strip()
    if payload.get("strict_matching") is not None:
        spec.strict_matching = bool(payload.get("strict_matching"))
    spec.raw_text = summary
    return spec, summary


def _normalize_payload_list(value: Any) -> list[str]:
    """Normalize qwen payload values that may arrive as list, tuple, set, or delimited string."""
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        items = [str(item).strip() for item in value if str(item).strip()]
        return _deduplicate(items)
    if isinstance(value, str):
        items = [item.strip() for item in re.split(r'[\n,，、;；]+', value) if item.strip()]
        return _deduplicate(items)
    text = str(value).strip()
    return [text] if text else []


def _record_requirement_degradation(usage_context: dict[str, Any] | None, message: str):
    """Mark the task as degraded when qwen requirement output cannot be used directly."""
    task_id = str((usage_context or {}).get("task_id", "")).strip()
    if not task_id or not message:
        return
    get_ollama_service().record_validation_error(task_id, message)


def _build_requirement_summary_from_payload(payload: dict[str, Any], fallback_text: str) -> str:
    """Build a normalized requirement summary from qwen JSON when summary is missing."""
    filters = payload.get("filters") if isinstance(payload.get("filters"), dict) else {}
    filter_parts: list[str] = []
    for key, value in filters.items():
        if isinstance(value, list):
            value_text = "、".join(str(item).strip() for item in value if str(item).strip())
        else:
            value_text = str(value).strip()
        if value_text:
            filter_parts.append(f"{key}={value_text}")
    time_range = payload.get("time_range")
    if isinstance(time_range, list) and len(time_range) == 2 and all(time_range):
        time_text = f"{time_range[0]} 到 {time_range[1]}"
    else:
        time_text = "未明确"
    entity_text = "、".join(str(item).strip() for item in payload.get("entity_keywords", []) if str(item).strip()) or "全部可识别实体"
    indicator_text = "、".join(str(item).strip() for item in payload.get("indicator_keywords", []) if str(item).strip()) or "按模板字段自动匹配"
    filter_text = "、".join(filter_parts) or "默认过滤=按模板字段与来源主题综合匹配"
    output_text = str(payload.get("output_granularity") or "逐条记录").strip()
    strict_text = "是" if payload.get("strict_matching") else "否"
    return (
        f"时间范围：{time_text}\n"
        f"实体范围：{entity_text}\n"
        f"指标关键词：{indicator_text}\n"
        f"筛选条件：{filter_text}\n"
        f"输出粒度：{output_text}\n"
        f"严格匹配：{strict_text}\n"
        f"补充说明：{fallback_text[:120]}"
    )


def _guess_document_title(text: str, fallback: str) -> str:
    """Pick a short title from a document."""
    for line in text.splitlines()[:8]:
        line = line.strip()
        if 4 <= len(line) <= 60 and _is_meaningful_inferred_token(line):
            return line
    return fallback


def _find_time_range_in_text(text: str) -> str:
    """Find a compact time range hint inside free text."""
    m = re.search(
        r'((?:19\d{2}|20\d{2}|2100)[/年.-](?:0?[1-9]|1[0-2])[/月.-](?:0?[1-9]|[12]\d|3[01])[日号]?\s*[到至~－\-]\s*(?:19\d{2}|20\d{2}|2100)[/年.-](?:0?[1-9]|1[0-2])[/月.-](?:0?[1-9]|[12]\d|3[01])[日号]?)',
        text,
    )
    if m:
        return m.group(1)
    m = re.search(r'((?:19\d{2}|20\d{2}|2100)[年/.-](?:0?[1-9]|1[0-2])[月]?)', text)
    return m.group(1) if m else ""


def _extract_name_like_titles(titles: list[str]) -> list[str]:
    """Extract entity-like fragments from titles and headings."""
    items: list[str] = []
    for title in titles:
        for match in re.findall(r'[\u4e00-\u9fa5A-Za-z0-9]{3,20}', title):
            if any(ch.isdigit() for ch in match) and len(match) <= 4:
                continue
            if _is_meaningful_inferred_token(match) and match not in items:
                items.append(match)
    return items


def _is_meaningful_inferred_token(value: str) -> bool:
    """Filter out numeric noise and generic worksheet markers."""
    token = value.strip()
    if len(token) < 2:
        return False
    lowered = token.lower()
    if lowered in GENERIC_INFERRED_TOKENS:
        return False
    if re.fullmatch(r'[\d.\-_/:%]+', token):
        return False
    digit_count = sum(ch.isdigit() for ch in token)
    if digit_count and digit_count / max(len(token), 1) > 0.45:
        return False
    if token.startswith('关键词：') and len(token) <= 6:
        return False
    return True


def _deduplicate(values: list[str]) -> list[str]:
    """Deduplicate strings while preserving order."""
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        value = value.strip()
        if not value or value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def _source_relevance(doc, template_signal_text: str) -> int:
    """Score how relevant a source looks to the current template."""
    template_tokens = {
        token.lower()
        for token in re.findall(r'[\u4e00-\u9fa5A-Za-z0-9]{2,20}', template_signal_text)
        if _is_meaningful_inferred_token(token)
    }
    source_text = " ".join([
        doc.metadata.get("title", "") if doc.metadata else "",
        doc.source_file,
        " ".join(header for table in doc.tables[:4] for header in table.headers[:12]),
    ])
    source_tokens = {
        token.lower()
        for token in re.findall(r'[\u4e00-\u9fa5A-Za-z0-9]{2,20}', source_text)
        if _is_meaningful_inferred_token(token)
    }
    return len(template_tokens & source_tokens)
