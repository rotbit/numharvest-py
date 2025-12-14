"""State/Province normalization utilities for NumberBarn-style URLs."""

from __future__ import annotations

import re
from urllib.parse import urlencode

# 固定列表：US 50 州 + DC + 4 领地 + 常见加拿大省份/地区（NANP 覆盖）
CODE_TO_NAME: dict[str, str] = {
    # United States (50 states + DC)
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming",
    "DC": "District of Columbia",
    # US territories commonly seen on NumberBarn / NANP
    "PR": "Puerto Rico",
    "GU": "Guam",
    "VI": "U.S. Virgin Islands",
    "AS": "American Samoa",
    "MP": "Northern Mariana Islands",
    # Canada (NANP coverage)
    "AB": "Alberta",
    "BC": "British Columbia",
    "MB": "Manitoba",
    "NB": "New Brunswick",
    "NL": "Newfoundland and Labrador",
    "NS": "Nova Scotia",
    "NT": "Northwest Territories",
    "NU": "Nunavut",
    "ON": "Ontario",
    "PE": "Prince Edward Island",
    "QC": "Quebec",
    "SK": "Saskatchewan",
    "YT": "Yukon",
}


def _normalize_key(value: str) -> str:
    """Lowercase and collapse internal whitespace for name matching."""
    collapsed = re.sub(r"\s+", " ", value.strip().lower())
    return collapsed


# 反向索引：名称 -> 代码（忽略大小写与多空格）
NAME_TO_CODE: dict[str, str] = {_normalize_key(name): code for code, name in CODE_TO_NAME.items()}


def normalize_state(state_input: str) -> str:
    """
    标准化州/省输入，返回两位大写代码。
    - 接受代码或名称，忽略大小写和多余空格。
    - 未匹配时抛出 ValueError。
    """
    if not state_input or not state_input.strip():
        raise ValueError("state 输入为空")

    raw = state_input.strip()
    code_candidate = raw.upper()
    if code_candidate in CODE_TO_NAME:
        return code_candidate

    key = _normalize_key(raw)
    if key in NAME_TO_CODE:
        return NAME_TO_CODE[key]

    raise ValueError(f"未知州/省: {state_input}")


def is_valid_state_code(code: str) -> bool:
    """仅当 code 在 CODE_TO_NAME 中存在时返回 True。"""
    if not code:
        return False
    return code.strip().upper() in CODE_TO_NAME


def state_name(code: str) -> str:
    """根据两位代码返回正式名称；不存在时抛出 ValueError。"""
    normalized = normalize_state(code)  # 复用校验逻辑
    return CODE_TO_NAME[normalized]


def build_numberbarn_url(search: str, state_input: str) -> str:
    """
    生成 NumberBarn 搜索 URL，state 先经过 normalize_state。
    示例：build_numberbarn_url("car", "New Mexico")
    -> https://www.numberbarn.com/search?type=local&search=car&state=NM&moreResults=true&sort=price%2B&limit=24
    """
    state_code = normalize_state(state_input)
    base = "https://www.numberbarn.com/search"
    params = {
        "type": "local",
        "search": search,
        "state": state_code,
        # 与现有抓取保持一致的常用参数
        "moreResults": "true",
        "sort": "price+",
        "limit": "24",
    }
    return f"{base}?{urlencode(params)}"

