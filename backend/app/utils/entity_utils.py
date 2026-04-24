"""Entity legality helpers used before template write-back and during validation."""
from __future__ import annotations

import re
from typing import Any

from .text_utils import clean_cell_value


NUMBER_PATTERN = r'[-+]?\d[\d,]*(?:\.\d+)?'
DATE_PATTERN = (
    r'(?:19|20)\d{2}[/-年.](?:1[0-2]|0?[1-9])[/-月.]'
    r'(?:3[01]|[12]\d|0?[1-9])(?:日)?'
)
NARRATIVE_PREFIXES = (
    "作为",
    "在",
    "以",
    "凭借",
    "围绕",
    "聚焦",
    "持续",
    "推进",
    "推动",
    "加强",
    "确保",
    "立足",
    "通过",
    "同时",
    "其中",
    "筑牢",
    "优化",
    "聚集",
    "整合",
    "协同",
    "统筹",
)
METRIC_TERMS = (
    "GDP",
    "人均",
    "人口",
    "病例",
    "确诊",
    "检测",
    "预算",
    "指数",
    "监测值",
    "新增",
)
NARRATIVE_TERMS = (
    "高质量发展",
    "联防联控",
    "复工复产",
    "防控态势",
    "平稳可控",
    "重点场所",
    "重点区域",
    "三位一体",
)
GENERAL_NAME_PATTERN = re.compile(r"^[A-Za-z\u4e00-\u9fa5·\s（）().'&-]{2,40}$")
ENTITY_NAME_PATTERN = re.compile(r"^[A-Za-z0-9#\u4e00-\u9fa5·\s（）().'&-]{2,40}$")
GEO_ENTITY_TYPES = {"country", "sub_national", "city", "district"}
ORGANIZATION_TERMS = (
    "公司",
    "集团",
    "大学",
    "学院",
    "医院",
    "委员会",
    "研究院",
    "银行",
    "中心",
    "协会",
    "实验室",
    "局",
)
SITE_TERMS = (
    "站",
    "站点",
    "监测点",
    "采样点",
    "口岸",
    "园区",
    "港区",
    "枢纽",
    "site",
    "station",
)
COUNTRY_ONLY_CONTEXT_TERMS = (
    "全球",
    "世界",
    "国际",
    "各国",
    "world",
    "global",
    "international",
    "continent",
    "洲",
)
DOMESTIC_ADMIN_CONTEXT_TERMS = (
    "省",
    "市",
    "自治区",
    "特别行政区",
    "直辖",
    "地方",
    "行政区",
    "区县",
    "城市",
    "地区",
    "辖区",
)
GEOGRAPHIC_DESCRIPTORS = (
    "华北",
    "华南",
    "华东",
    "华西",
    "华中",
    "西南",
    "东北",
    "西北",
    "东南",
    "长三角",
    "珠三角",
    "京津冀",
    "高原",
    "盆地",
    "平原",
)
# Administrative suffixes used to distinguish real admin-unit names (e.g. "平原县") from
# plain geographic descriptors (e.g. "平原").
ADMIN_SUFFIXES = (
    "省", "市", "区", "县", "旗", "镇", "乡", "街道",
    "自治区", "自治州", "自治县", "特别行政区", "开发区", "新区", "园区",
)

NON_ENTITY_REGION_TERMS = (
    "消杀",
    "边境",
    "境外",
    "口岸",
    "防线",
    "经济带",
    "地区联防",
    "防控机制",
    "立足",
    "枢纽",
)


def entity_header_kind(header: str) -> str:
    """Classify whether a field header is an entity-like field."""
    normalized = re.sub(r'[\s_/（）()\-]+', '', str(header or "")).lower()
    header_text = str(header or "")
    if any(term in header_text for term in ("生产总值", "预算", "病例", "检测", "指数", "监测值", "收入", "支出", "金额", "占比")):
        return ""
    if "国家" in header_text or "地区" in header_text or "区域" in header_text:
        return "region"
    if "省" in header_text or "自治区" in header_text or "特别行政区" in header_text:
        return "province"
    if "城市" in header_text or normalized == "city":
        return "city"
    if header_text.strip() == "区" or "区县" in header_text or "城区" in header_text:
        return "district"
    if "站点" in header_text or "名称" in header_text or "单位" in header_text or "机构" in header_text:
        return "name"
    return ""


def is_entity_header(header: str) -> bool:
    """Return True when a header should contain an entity-like value."""
    return bool(entity_header_kind(header))


def semantic_field_profile(
    header: str,
    peer_headers: list[str] | None = None,
    context_text: str = "",
) -> dict[str, Any]:
    """Infer the semantic role and allowed granularities for one template field."""
    kind = entity_header_kind(header)
    header_list = [item for item in (peer_headers or []) if item]
    context_blob = _build_context_blob(header, header_list, context_text)
    country_only = kind == "region" and "地区" not in header and _has_country_only_signals(context_blob, header_list)

    if kind in {"region", "province", "city", "district"}:
        if kind == "region":
            allowed = {"country"} if country_only else {"country", "sub_national", "city"}
        elif kind == "province":
            allowed = {"sub_national"}
        elif kind == "city":
            allowed = {"city"}
        else:
            # County-level cities (县级市) are administratively equivalent to
            # districts/counties and commonly appear in "区" columns.
            allowed = {"district", "city"}
        return {
            "field_kind": kind,
            "semantic_field_role": "geo_entity",
            "allowed_granularity_set": sorted(allowed),
            "country_only": country_only,
        }

    if kind == "name":
        normalized_header = re.sub(r"[\s_/（）()\-]+", "", header.lower())
        if any(term in header for term in SITE_TERMS) or any(term in normalized_header for term in ("site", "station")):
            return {
                "field_kind": kind,
                "semantic_field_role": "station/site",
                "allowed_granularity_set": ["station/site"],
                "country_only": False,
            }
        if any(term in header for term in ORGANIZATION_TERMS):
            return {
                "field_kind": kind,
                "semantic_field_role": "organization",
                "allowed_granularity_set": ["organization"],
                "country_only": False,
            }
        return {
            "field_kind": kind,
            "semantic_field_role": "metric_subject",
            "allowed_granularity_set": ["metric_subject", "organization", "station/site", "country", "sub_national", "city", "district"],
            "country_only": False,
        }

    return {
        "field_kind": kind,
        "semantic_field_role": "",
        "allowed_granularity_set": [],
        "country_only": False,
    }


def infer_entity_profile(
    value: str,
    header: str = "",
    peer_headers: list[str] | None = None,
    context_text: str = "",
    record_values: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Infer a normalized entity type and granularity from one candidate value."""
    text = clean_cell_value(value)
    field_profile = semantic_field_profile(header, peer_headers=peer_headers, context_text=context_text)
    context_blob = _build_context_blob(header, peer_headers or [], context_text)

    profile = {
        "normalized_entity_text": text,
        "normalized_entity_type": "",
        "normalized_granularity": "",
        "parent_scope": "",
        "semantic_role": field_profile.get("semantic_field_role", ""),
    }
    if not text:
        return profile

    if re.search(DATE_PATTERN, text):
        profile.update(normalized_entity_type="time_scope", normalized_granularity="time_scope")
        return profile

    if any(term in text for term in SITE_TERMS) or re.search(r"\b(site|station)\b", text, flags=re.IGNORECASE):
        profile.update(normalized_entity_type="station/site", normalized_granularity="station/site")
        return profile

    if any(term in text for term in ORGANIZATION_TERMS):
        profile.update(normalized_entity_type="organization", normalized_granularity="organization")
        return profile

    if text.endswith(("省", "自治区", "特别行政区", "自治州", "地区")):
        profile.update(normalized_entity_type="sub_national", normalized_granularity="sub_national")
        return profile

    if text.endswith(("市", "州", "盟")):
        profile.update(normalized_entity_type="city", normalized_granularity="city")
        return profile

    if text.endswith(("区", "县", "旗", "镇", "乡", "街道")):
        profile.update(normalized_entity_type="district", normalized_granularity="district")
        return profile

    if text.endswith(("国", "共和国", "联邦", "王国", "公国", "酋长国")):
        profile.update(normalized_entity_type="country", normalized_granularity="country")
        return profile

    if _contains_latin(text):
        profile.update(normalized_entity_type="country", normalized_granularity="country")
        return profile

    field_kind = field_profile.get("field_kind", "")
    if field_kind == "province":
        profile.update(normalized_entity_type="sub_national", normalized_granularity="sub_national")
        return profile
    if field_kind == "city":
        profile.update(normalized_entity_type="city", normalized_granularity="city")
        return profile
    if field_kind == "district":
        profile.update(normalized_entity_type="district", normalized_granularity="district")
        return profile
    if field_kind == "region":
        if field_profile.get("country_only") and _record_has_continent_context(record_values):
            profile.update(normalized_entity_type="country", normalized_granularity="country")
            return profile
        if _has_domestic_admin_signals(context_blob):
            profile.update(normalized_entity_type="sub_national", normalized_granularity="sub_national")
        else:
            profile.update(normalized_entity_type="country", normalized_granularity="country")
        return profile

    if field_profile.get("semantic_field_role") in {"organization", "station/site", "metric_subject"}:
        profile.update(
            normalized_entity_type=field_profile["semantic_field_role"],
            normalized_granularity=field_profile["semantic_field_role"],
        )
        return profile

    profile.update(normalized_entity_type="metric_subject", normalized_granularity="metric_subject")
    return profile


def evaluate_entity_compatibility(
    value: str,
    header: str,
    peer_headers: list[str] | None = None,
    context_text: str = "",
    record_values: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Evaluate legality and granularity-aware compatibility for one entity candidate."""
    field_profile = semantic_field_profile(header, peer_headers=peer_headers, context_text=context_text)
    entity_profile = infer_entity_profile(
        value,
        header=header,
        peer_headers=peer_headers,
        context_text=context_text,
        record_values=record_values,
    )
    text = entity_profile.get("normalized_entity_text", "")
    allowed = list(field_profile.get("allowed_granularity_set", []))

    result = {
        **entity_profile,
        "semantic_field_role": field_profile.get("semantic_field_role", ""),
        "allowed_granularity_set": allowed,
        "compatibility_score": 1.0,
        "recoverable_mismatch_reason": "",
        "filter_reason": "",
        "filter_level": "accepted",
        "filter_stage": "entity_compatibility",
        "whether_recoverable": False,
        "accepted": True,
    }
    if not field_profile.get("semantic_field_role") or not text:
        return result

    legal, reason = _basic_entity_legality(text, field_profile)
    if not legal:
        result.update(
            compatibility_score=0.0,
            filter_reason=reason,
            filter_level="hard_filter",
            whether_recoverable=False,
            accepted=False,
        )
        return result

    granularity = entity_profile.get("normalized_granularity", "")
    role = field_profile.get("semantic_field_role", "")
    field_kind = field_profile.get("field_kind", "")

    if role == "geo_entity":
        if entity_profile.get("normalized_entity_type") not in GEO_ENTITY_TYPES:
            result.update(
                compatibility_score=0.0,
                filter_reason="entity_type_mismatch",
                filter_level="hard_filter",
                whether_recoverable=False,
                accepted=False,
            )
            return result
        if granularity in allowed:
            result["compatibility_score"] = _granularity_match_score(field_kind, granularity)
            if field_kind == "region" and granularity in {"sub_national", "city"}:
                result["recoverable_mismatch_reason"] = "compatible_admin_granularity"
                result["whether_recoverable"] = True
            return result
        if field_kind == "region" and field_profile.get("country_only") and granularity in {"sub_national", "city", "district"}:
            result.update(
                compatibility_score=0.42 if granularity == "sub_national" else 0.34,
                filter_reason="region_field_country_only",
                filter_level="soft_filter",
                recoverable_mismatch_reason="region_field_country_only",
                whether_recoverable=True,
                accepted=False,
            )
            return result
        if field_kind == "province" and granularity == "city":
            result.update(
                compatibility_score=0.46,
                filter_reason="city_in_province_field",
                filter_level="soft_filter",
                recoverable_mismatch_reason="city_in_province_field",
                whether_recoverable=True,
                accepted=False,
            )
            return result
        if field_kind == "city" and granularity == "district":
            result.update(
                compatibility_score=0.52,
                filter_reason="district_in_city_field",
                filter_level="soft_filter",
                recoverable_mismatch_reason="district_in_city_field",
                whether_recoverable=True,
                accepted=False,
            )
            return result
        if field_kind == "city" and granularity == "sub_national":
            result.update(
                compatibility_score=0.38,
                filter_reason="province_like_in_city_field",
                filter_level="soft_filter",
                recoverable_mismatch_reason="province_like_in_city_field",
                whether_recoverable=True,
                accepted=False,
            )
            return result
        if field_kind == "district" and granularity in {"city", "sub_national"}:
            result.update(
                compatibility_score=0.31,
                filter_reason="granularity_mismatch",
                filter_level="soft_filter",
                recoverable_mismatch_reason="granularity_mismatch",
                whether_recoverable=True,
                accepted=False,
            )
            return result
        result.update(
            compatibility_score=0.2,
            filter_reason="granularity_mismatch",
            filter_level="soft_filter",
            recoverable_mismatch_reason="granularity_mismatch",
            whether_recoverable=True,
            accepted=False,
        )
        return result

    if role == "organization":
        if entity_profile.get("normalized_entity_type") == "organization":
            result["compatibility_score"] = 0.95
            return result
        result.update(
            compatibility_score=0.25,
            filter_reason="entity_type_mismatch",
            filter_level="soft_filter",
            recoverable_mismatch_reason="entity_type_mismatch",
            whether_recoverable=True,
            accepted=False,
        )
        return result

    if role == "station/site":
        # Monitoring/sampling stations are routinely named after the organizations or
        # locations that host them (schools, hospitals, government buildings, parks, etc.).
        # Any value that passes the basic legality gate is a valid station name.
        entity_type = entity_profile.get("normalized_entity_type", "")
        result["compatibility_score"] = 0.95 if entity_type == "station/site" else 0.88
        return result

    if role == "metric_subject":
        result["compatibility_score"] = 0.82 if entity_profile.get("normalized_entity_type") in GEO_ENTITY_TYPES else 0.76
        return result

    return result


def validate_entity_value(
    value: str,
    header: str,
    peer_headers: list[str] | None = None,
    context_text: str = "",
    record_values: dict[str, Any] | None = None,
) -> tuple[bool, str]:
    """Return whether one entity value looks legal for the expected header."""
    assessment = evaluate_entity_compatibility(
        value,
        header,
        peer_headers=peer_headers,
        context_text=context_text,
        record_values=record_values,
    )
    if assessment.get("accepted"):
        return True, ""
    return False, str(assessment.get("filter_reason") or assessment.get("recoverable_mismatch_reason") or "entity_incompatible")


def describe_entity_reason(reason: str) -> str:
    """Map internal legality reason codes to human-readable text."""
    mapping = {
        "too_short": "长度过短",
        "too_long": "长度过长",
        "contains_sentence_punctuation": "包含句子级标点",
        "looks_numeric_or_date": "看起来像数字或日期",
        "starts_with_narrative_prefix": "以叙事前缀开头",
        "contains_narrative_phrase": "包含叙事短语",
        "contains_metric_term": "包含指标术语",
        "contains_predicate_particle": "包含叙事谓词结构",
        "contains_illegal_chars": "包含非法字符",
        "invalid_province_shape": "不符合省级实体形态",
        "province_like_in_city_field": "城市字段出现省级实体",
        "invalid_city_shape": "不符合城市实体形态",
        "invalid_district_shape": "不符合区县实体形态",
        "invalid_region_shape": "不符合地区实体形态",
        "entity_type_mismatch": "实体类型与模板语义不匹配",
        "granularity_mismatch": "实体粒度与模板要求不匹配",
        "region_field_country_only": "模板语义更偏向国家级实体",
        "compatible_admin_granularity": "模板字段允许兼容行政层级实体",
        "city_in_province_field": "省级字段出现城市级实体",
        "district_in_city_field": "城市字段出现区县级实体",
        "geographic_descriptor_not_entity": "地理描述词不是可写实体",
        "non_entity_phrase": "叙事短语不是可写实体",
    }
    return mapping.get(reason, reason or "未知原因")


def _build_context_blob(header: str, peer_headers: list[str], context_text: str) -> str:
    return " ".join(item for item in [header, *(peer_headers or []), context_text] if item)


def _has_country_only_signals(context_blob: str, peer_headers: list[str]) -> bool:
    if any(header.strip() in {"大洲", "洲"} or "大洲" in header for header in peer_headers):
        return True
    normalized = context_blob.lower()
    return any(term.lower() in normalized for term in COUNTRY_ONLY_CONTEXT_TERMS)


def _has_domestic_admin_signals(context_blob: str) -> bool:
    normalized = context_blob.lower()
    return any(term.lower() in normalized for term in DOMESTIC_ADMIN_CONTEXT_TERMS)


def _record_has_continent_context(record_values: dict[str, Any] | None = None) -> bool:
    """Return True when the current record already carries an explicit continent field value."""
    if not record_values:
        return False
    for header, value in record_values.items():
        if value in (None, ""):
            continue
        normalized_header = re.sub(r'[\s_/（）()\-]+', '', str(header or '').lower())
        if normalized_header in {"大洲", "洲", "continent"} or "continent" in normalized_header:
            return True
    return False


def _basic_entity_legality(text: str, field_profile: dict[str, Any]) -> tuple[bool, str]:
    role = str(field_profile.get("semantic_field_role", "") or "")
    max_len = 40 if role in {"metric_subject", "geo_entity"} else 50
    if len(text) < 2:
        return False, "too_short"
    if len(text) > max_len:
        return False, "too_long"
    if re.search(r'[\n\r\t，,。；;！？!：:]', text):
        return False, "contains_sentence_punctuation"
    if re.search(DATE_PATTERN, text) or re.fullmatch(rf'{NUMBER_PATTERN}(?:[%％万亿元例份]*)', text):
        return False, "looks_numeric_or_date"
    if text.startswith(NARRATIVE_PREFIXES):
        return False, "starts_with_narrative_prefix"
    if any(term in text for term in NARRATIVE_TERMS):
        return False, "contains_narrative_phrase"
    if role != "metric_subject" and any(term in text for term in METRIC_TERMS):
        return False, "contains_metric_term"
    if "的" in text and role in {"geo_entity", "organization", "station/site"}:
        return False, "contains_predicate_particle"
    if role in {"organization", "station/site", "metric_subject"}:
        if not ENTITY_NAME_PATTERN.fullmatch(text):
            return False, "contains_illegal_chars"
        return True, ""
    if not GENERAL_NAME_PATTERN.fullmatch(text):
        return False, "contains_illegal_chars"
    for descriptor in GEOGRAPHIC_DESCRIPTORS:
        if text == descriptor:
            return False, "geographic_descriptor_not_entity"
        # Allow names that start with a geographic descriptor but end with an
        # administrative suffix, e.g. "平原县" (Pingyuan County) vs "平原" (plain).
        if text.startswith(descriptor) and not any(text.endswith(suf) for suf in ADMIN_SUFFIXES):
            return False, "geographic_descriptor_not_entity"
    for bad_term in NON_ENTITY_REGION_TERMS:
        if bad_term in text:
            return False, "non_entity_phrase"
    return True, ""


def _granularity_match_score(field_kind: str, granularity: str) -> float:
    if field_kind == "region":
        return {
            "country": 0.98,
            "sub_national": 0.88,
            "city": 0.78,
        }.get(granularity, 0.6)
    if field_kind == "province":
        return 0.96 if granularity == "sub_national" else 0.5
    if field_kind == "city":
        return 0.96 if granularity == "city" else 0.5
    if field_kind == "district":
        if granularity == "district":
            return 0.96
        if granularity == "city":
            return 0.82  # county-level cities in district/sub-unit columns
        return 0.5
    return 0.9


def _contains_latin(text: str) -> bool:
    """Return True when the candidate contains Latin letters."""
    return bool(re.search(r"[A-Za-z]", text))
