"""Evidence retrieval service - rule-based candidate retrieval before LLM."""
import re
from datetime import datetime
from typing import Optional

from .ollama_service import get_ollama_service
from ..schemas.models import (
    DocumentBundle, TemplateSchema, RequirementSpec,
    CandidateEvidence, NormalizedTable,
)
from ..core.logging import logger
from ..utils.text_utils import best_column_match, similarity


HEADER_CONCEPT_KEYWORDS = {
    "entity": (
        "城市", "城市名", "城市名称", "国家", "地区", "名称", "省", "省份", "区域",
        "站点", "单位", "country", "city", "region", "province", "name", "station",
    ),
    "time": (
        "日期", "时间", "年月", "年份", "月份", "监测时间",
        "date", "time", "year", "month",
    ),
    "economy": (
        "gdp", "生产总值", "经济", "财政", "预算", "收入", "支出", "工业", "投资",
        "消费", "人口", "人均", "增速", "亿元", "万人",
    ),
    "air_quality": (
        "aqi", "空气", "环境", "质量", "pm2.5", "pm10", "so2", "no2", "co", "o3", "污染",
    ),
    "epidemic": (
        "covid", "疫情", "确诊", "死亡", "康复", "病例", "感染", "患者", "核酸", "检测", "新冠",
        "confirmed", "deaths", "recovered", "cases", "tests",
    ),
}
DOMAIN_CONCEPTS = {"economy", "air_quality", "epidemic"}


class RetrievalResult:
    """Container for retrieved evidence."""
    def __init__(self):
        self.table_candidates: list[dict] = []  # Each: {source_doc, table, col_mapping, filtered_rows, evidence}
        self.text_candidates: list[CandidateEvidence] = []
        self.source_docs: list[DocumentBundle] = []  # Reference to all source docs (for LLM context)


def retrieve_evidence(
    documents: list[DocumentBundle],
    template: TemplateSchema,
    requirement: RequirementSpec,
    use_llm: bool = True,
    usage_context: dict | None = None,
) -> RetrievalResult:
    """Retrieve candidate evidence from documents based on template fields and requirements."""
    logger.info(f"Retrieving evidence from {len(documents)} documents for template "
                f"with {len(template.tables)} tables, {len(template.fields)} fields")

    result = RetrievalResult()
    result.source_docs = list(documents)  # Store all source docs for LLM context

    for doc in documents:
        # 1. Table-to-table matching
        for tmpl_table in template.tables:
            for src_table in doc.tables:
                if not _table_pair_is_plausible(tmpl_table, src_table):
                    continue
                col_mapping = _match_table_columns(tmpl_table, src_table)
                if _mapping_needs_llm(tmpl_table, src_table, col_mapping):
                    try:
                        llm_mapping = _llm_match_table_columns(
                            tmpl_table,
                            src_table,
                            use_llm=use_llm,
                            usage_context={
                                **(usage_context or {}),
                                "stage": "retrieve",
                                "source_file": doc.source_file,
                                "source_files": [doc.source_file],
                                "template_file": template.source_file,
                                "template_files": [template.source_file],
                            },
                        )
                        if llm_mapping:
                            col_mapping = llm_mapping
                    except RuntimeError:
                        # LLM unavailable; fall back to rule-based col_mapping.
                        # _mapping_is_usable will gate whether it is sufficient.
                        pass
                if col_mapping and _mapping_is_usable(tmpl_table, src_table, col_mapping):
                    # Apply filters to source rows
                    filtered_rows = _filter_table_rows(
                        src_table, requirement, col_mapping
                    )

                    evidence = CandidateEvidence(
                        source_file=doc.source_file,
                        location=f"table{src_table.table_index}({src_table.sheet_name})",
                        raw_snippet=_table_snippet(src_table, max_rows=3),
                        match_reason=f"Column match: {col_mapping}",
                        confidence=len(col_mapping) / max(len(tmpl_table.headers), 1),
                    )

                    result.table_candidates.append({
                        "source_doc": doc,
                        "source_table": src_table,
                        "template_table": tmpl_table,
                        "col_mapping": col_mapping,
                        "filtered_rows": filtered_rows,
                        "evidence": evidence,
                    })

        # 2. Text-based evidence for fields
        for field in template.fields:
            text_ev = _search_text_evidence(doc, field.field_name, requirement)
            result.text_candidates.extend(text_ev)

    # Sort table candidates by confidence
    result.table_candidates.sort(key=lambda x: x["evidence"].confidence, reverse=True)

    logger.info(f"  -> Found {len(result.table_candidates)} table candidates, "
                f"{len(result.text_candidates)} text candidates")
    return result


def _mapping_needs_llm(tmpl_table, src_table: NormalizedTable, col_mapping: dict[str, str]) -> bool:
    """Detect header mappings that are too weak to trust without semantic help."""
    if not _table_pair_is_plausible(tmpl_table, src_table):
        return False
    template_headers = [header for header in tmpl_table.headers if header and header.strip()]
    source_headers = [header for header in src_table.headers if header and header.strip()]
    if not template_headers or not source_headers:
        return False
    coverage = len(col_mapping) / max(len(template_headers), 1)
    if coverage < 0.75:
        return True
    weak_pairs = 0
    for template_header, source_header in col_mapping.items():
        if template_header == source_header or template_header in source_header or source_header in template_header:
            continue
        if similarity(template_header, source_header) < 0.72:
            weak_pairs += 1
    return weak_pairs >= max(1, len(col_mapping) // 2)


def _llm_match_table_columns(
    tmpl_table,
    src_table: NormalizedTable,
    *,
    use_llm: bool,
    usage_context: dict | None = None,
) -> dict[str, str]:
    """Use qwen to resolve difficult header mappings when rule scores are weak."""
    ollama = get_ollama_service()
    llm_context = {
        **(usage_context or {}),
        "stage": "retrieve",
    }
    ollama.mark_required_call("困难字段映射需要 qwen 做语义列匹配", llm_context)
    if not use_llm or not ollama.is_available:
        ollama.note_skip("困难字段映射未能走 qwen", llm_context)
        raise RuntimeError("检测到困难字段映射，但本地 Ollama qwen2.5:14b 不可用")

    prompt = (
        "请将模板表头映射到来源表头。\n"
        "只输出 JSON 对象，不要解释。\n"
        "JSON 格式：{\"模板表头A\":\"来源表头X\", \"模板表头B\": null}\n"
        "要求：只能从给定来源表头中选择；不确定时填 null；优先实体列与时间列的正确语义。\n"
        f"模板表头：{tmpl_table.headers[:24]}\n"
        f"来源表头：{src_table.headers[:32]}\n"
    )
    parsed, error = ollama.generate_json(
        prompt,
        "你是表头语义映射器，只能输出 JSON 对象。",
        num_predict=256,
        usage_context=llm_context,
    )
    if error or not isinstance(parsed, dict):
        raise RuntimeError(f"qwen 字段映射失败: {error or '空结果'}")

    mapped: dict[str, str] = {}
    source_headers = {header.strip() for header in src_table.headers if header and header.strip()}
    for template_header, source_header in parsed.items():
        th = str(template_header).strip()
        sh = str(source_header).strip() if source_header not in (None, "") else ""
        if th and sh and sh in source_headers:
            mapped[th] = sh
    return mapped


def _match_table_columns(tmpl_table, src_table: NormalizedTable) -> dict[str, str]:
    """Match template table columns to source table columns."""
    mapping = {}
    used = set()

    for th in tmpl_table.headers:
        if not th or not th.strip():
            continue

        best = None
        best_score = 0.0

        for sh in src_table.headers:
            if not sh or sh in used:
                continue

            # Exact match
            if th == sh:
                best = sh
                best_score = 1.0
                break

            # Containment
            if th in sh or sh in th:
                score = 0.85
            else:
                score = similarity(th, sh)
                # Keyword overlap boost
                th_kw = set(re.findall(r'[\u4e00-\u9fa5a-zA-Z0-9]+', th))
                sh_kw = set(re.findall(r'[\u4e00-\u9fa5a-zA-Z0-9]+', sh))
                if th_kw & sh_kw:
                    score = max(score, 0.7)

            if score > best_score:
                best_score = score
                best = sh

        if best and best_score >= 0.45:
            mapping[th] = best
            used.add(best)

    return mapping


def _mapping_is_usable(tmpl_table, src_table: NormalizedTable, col_mapping: dict[str, str]) -> bool:
    """Reject weak cross-domain table matches that cause source pollution."""
    if not _table_pair_is_plausible(tmpl_table, src_table):
        return False
    template_headers = [header for header in tmpl_table.headers if header and header.strip()]
    if not template_headers:
        return False

    coverage = len(col_mapping) / max(len(template_headers), 1)
    min_coverage = 0.5 if len(template_headers) >= 4 else 0.67
    if coverage < min_coverage:
        return False

    entity_tokens = ('城市', '国家', '地区', '名称', '区', '省', '站点', '单位', 'city', 'country', 'name')
    template_entity_headers = [
        header for header in template_headers
        if any(token in header.lower() for token in entity_tokens)
    ]
    if template_entity_headers and not any(header in col_mapping for header in template_entity_headers):
        return False

    return True


def _table_pair_is_plausible(tmpl_table, src_table: NormalizedTable) -> bool:
    """Reject obviously cross-domain table pairs before we spend qwen calls on them."""
    template_headers = [header for header in tmpl_table.headers if header and header.strip()]
    source_headers = [header for header in src_table.headers if header and header.strip()]
    if not template_headers or not source_headers:
        return False

    template_concepts = _header_concepts(template_headers)
    source_concepts = _header_concepts(source_headers)

    template_domains = template_concepts & DOMAIN_CONCEPTS
    source_domains = source_concepts & DOMAIN_CONCEPTS
    if template_domains and source_domains and not (template_domains & source_domains):
        return False
    if template_domains and not source_domains:
        return False

    template_tokens = _header_tokens(template_headers)
    source_tokens = _header_tokens(source_headers)
    if template_tokens & source_tokens:
        return True

    shared_concepts = template_concepts & source_concepts
    if shared_concepts & {"entity", "time"}:
        return True
    if template_domains and (template_domains & shared_concepts):
        return True

    return not template_domains and bool(shared_concepts)


def _header_concepts(headers: list[str]) -> set[str]:
    """Infer coarse semantic concepts from a header list."""
    concepts: set[str] = set()
    normalized_headers = [_normalize_header_text(header) for header in headers]
    for concept, keywords in HEADER_CONCEPT_KEYWORDS.items():
        for keyword in keywords:
            keyword_norm = _normalize_header_text(keyword)
            if any(keyword_norm and keyword_norm in header for header in normalized_headers):
                concepts.add(concept)
                break
    return concepts


def _header_tokens(headers: list[str]) -> set[str]:
    """Build stable tokens from headers for cheap overlap checks."""
    tokens: set[str] = set()
    for header in headers:
        for token in re.findall(r'[A-Za-z0-9.]+|[\u4e00-\u9fa5]{2,12}', header or ""):
            normalized = _normalize_header_text(token)
            if len(normalized) >= 2:
                tokens.add(normalized)
    return tokens


def _normalize_header_text(value: str) -> str:
    """Normalize header text for concept and overlap matching."""
    return re.sub(r'[\s_\-:：,，、/\\()（）\[\]【】]+', '', (value or "").strip().lower())


def _filter_table_rows(
    table: NormalizedTable,
    requirement: RequirementSpec,
    col_mapping: dict[str, str],
) -> list[list[str]]:
    """Filter table rows based on requirement."""
    rows = table.rows

    if not rows:
        return rows

    # Build header index
    h_idx = {h: i for i, h in enumerate(table.headers)}

    # Time range filter
    if requirement.time_range:
        start_str, end_str = requirement.time_range
        date_cols = [h for h in table.headers if any(k in h for k in
                     ['日期', '时间', 'date', 'Date', '监测时间', '年份', '月份'])]
        if date_cols:
            date_col = date_cols[0]
            ci = h_idx.get(date_col)
            if ci is not None:
                filtered = []
                for row in rows:
                    if ci < len(row):
                        cell_val = row[ci].strip()
                        if _date_in_range(cell_val, start_str, end_str):
                            filtered.append(row)
                if filtered:
                    rows = filtered

    # Explicit key/value filters
    if requirement.filters:
        for filter_key, filter_values in requirement.filters.items():
            if not filter_values:
                continue
            matched_col = best_column_match(filter_key, table.headers)
            if not matched_col or matched_col not in h_idx:
                continue
            ci = h_idx[matched_col]
            narrowed = []
            for row in rows:
                if ci >= len(row):
                    continue
                cell_val = row[ci].strip()
                if any(_filter_value_match(cell_val, value) for value in filter_values):
                    narrowed.append(row)
            rows = narrowed

    # Entity filter from table_specs
    if requirement.table_specs:
        # This will be applied per-table in the fill service
        pass

    # General entity filter
    if requirement.entity_keywords:
        entity_cols = [h for h in table.headers if any(k in h for k in
                       ['城市', '地区', '省份', '名称', '国家', 'country', 'city', '区域'])]
        if entity_cols:
            ci = h_idx.get(entity_cols[0])
            if ci is not None:
                filtered = []
                for row in rows:
                    if ci < len(row):
                        cell_val = row[ci].strip()
                        if any(ek in cell_val or cell_val in ek
                               for ek in requirement.entity_keywords):
                            filtered.append(row)
                if filtered:
                    rows = filtered

    # Sort/limit
    if requirement.sort_limit:
        top_n = requirement.sort_limit.get("top_n")
        if top_n and top_n < len(rows):
            rows = rows[:top_n]

    return rows


def _date_in_range(cell_val: str, start: str, end: str) -> bool:
    """Check if a date string falls within the given range."""
    if not cell_val:
        return False

    # Normalize date formats
    date_formats = [
        '%Y-%m-%d', '%Y/%m/%d', '%Y.%m.%d',
        '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f',
        '%Y/%m/%d %H:%M:%S',
    ]

    cell_date = None
    for fmt in date_formats:
        try:
            # Handle .0 suffix
            clean_val = re.sub(r'\.0$', '', cell_val.strip())
            cell_date = datetime.strptime(clean_val, fmt)
            break
        except ValueError:
            continue

    if cell_date is None:
        # Try partial match
        m = re.search(r'(\d{4})[-/.](\d{1,2})[-/.](\d{1,2})', cell_val)
        if m:
            try:
                cell_date = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
            except ValueError:
                return False
        else:
            return False

    try:
        start_date = datetime.strptime(start, '%Y-%m-%d')
        end_date = datetime.strptime(end, '%Y-%m-%d')
    except ValueError:
        return False

    return start_date <= cell_date <= end_date


def _filter_value_match(cell_val: str, target_val: str) -> bool:
    """Match explicit filter values, including tolerant timestamp equality."""
    if not cell_val or not target_val:
        return False
    normalized_cell = _normalize_filter_text(cell_val)
    normalized_target = _normalize_filter_text(target_val)
    if normalized_cell == normalized_target:
        return True
    if normalized_target in normalized_cell or normalized_cell in normalized_target:
        return True
    return _same_temporal_value(cell_val, target_val)


def _same_temporal_value(cell_val: str, target_val: str) -> bool:
    left = _normalize_filter_text(cell_val).replace('t', ' ')
    right = _normalize_filter_text(target_val).replace('t', ' ')
    return bool(left and right and left == right)


def _normalize_filter_text(value: str) -> str:
    normalized = (
        (value or "")
        .strip()
        .lower()
        .replace('/', '-')
        .replace('年', '-')
        .replace('月', '-')
        .replace('日', '')
        .replace('号', '')
        .replace('.0', '')
    )
    normalized = re.sub(r'\s+', ' ', normalized)
    normalized = normalized.replace('--', '-')
    return normalized.strip()


def _search_text_evidence(
    doc: DocumentBundle, field_name: str, requirement: RequirementSpec
) -> list[CandidateEvidence]:
    """Search text blocks for evidence of a field."""
    results = []
    keywords = re.findall(r'[\u4e00-\u9fa5a-zA-Z0-9]+', field_name)

    for block in doc.text_blocks:
        content = block.content
        score = 0.0
        matched_kw = []

        for kw in keywords:
            if kw in content:
                score += 1.0 / len(keywords) if keywords else 0
                matched_kw.append(kw)

        if score >= 0.3:
            # Extract snippet around matched keywords
            snippet = content[:200]
            results.append(CandidateEvidence(
                source_file=doc.source_file,
                location=f"text_block{block.block_index}",
                raw_snippet=snippet,
                match_reason=f"Keywords matched: {matched_kw}",
                confidence=min(score, 1.0),
            ))

    return results


def _table_snippet(table: NormalizedTable, max_rows: int = 3) -> str:
    """Create a text snippet from a table."""
    lines = [" | ".join(table.headers)]
    for row in table.rows[:max_rows]:
        lines.append(" | ".join(row[:len(table.headers)]))
    if len(table.rows) > max_rows:
        lines.append(f"... ({len(table.rows)} rows total)")
    return "\n".join(lines)
