import os
import re
import json
import time
import html
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse

import pandas as pd
import requests
import streamlit as st
from bs4 import BeautifulSoup

try:
    from openai import OpenAI
except Exception:
    OpenAI = None

st.set_page_config(page_title="미샵 DB 생성기", layout="wide")

BASE_URL = "https://www.misharp.co.kr"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
BASE_COLUMNS = [
    "product_no", "product_url", "product_name", "category", "sub_category", "price", "fabric",
    "fit_type", "size_range", "recommended_body_type", "body_cover_features", "style_tags",
    "season", "length_type", "sleeve_type", "color_options", "recommended_age",
    "coordination_items", "product_summary"
]
MEASUREMENT_COLUMNS = [
    "shoulder", "chest", "chest_measure_type", "armhole", "sleeve", "sleeve_circumference",
    "length", "length_front", "length_back", "measurement_source", "raw_measurements"
]
DB_COLUMNS = BASE_COLUMNS + MEASUREMENT_COLUMNS

EXCLUDED_URL_KEYWORDS = [
    "cate_no=", "/product/list", "/category/", "/board/", "/article/", "/member/",
    "/order/", "/myshop/", "/exec/front/newcoupon", "/search/", "/product/recent_view_product",
]

OPENAI_API_KEY = st.secrets.get("OPENAI_API_KEY", os.getenv("OPENAI_API_KEY", ""))


def get_client():
    if OPENAI_API_KEY and OpenAI is not None:
        return OpenAI(api_key=OPENAI_API_KEY)
    return None


def clean_text(text: str) -> str:
    if text is None:
        return ""
    text = html.unescape(str(text))
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def uniq_keep_order(items):
    out, seen = [], set()
    for item in items:
        item = clean_text(item)
        if item and item not in seen:
            seen.add(item)
            out.append(item)
    return out


def to_abs_url(url: str) -> str:
    if not url:
        return ""
    return urljoin(BASE_URL, url)


def extract_product_no(url: str) -> str:
    if not url:
        return ""
    m = re.search(r"product_no=(\d+)", url)
    if m:
        return m.group(1)
    m = re.search(r"/product/[^\s\"'>]+/(\d+)(?:/|\?|$)", url)
    if m:
        return m.group(1)
    return ""


def normalize_product_url(url: str) -> str:
    pno = extract_product_no(url)
    return f"{BASE_URL}/product/detail.html?product_no={pno}" if pno else to_abs_url(url)


def extract_cate_no(url: str) -> str:
    m = re.search(r"cate_no=(\d+)", url or "")
    return m.group(1) if m else ""


def is_product_url(url: str) -> bool:
    url = (url or "").lower()
    return bool(extract_product_no(url)) and "/product/list" not in url and "/category/" not in url


def is_category_url(url: str) -> bool:
    url = (url or "").lower()
    return ((("/product/list.html" in url and "cate_no=" in url) or "/category/" in url) and not is_product_url(url))


def build_page_url(category_url: str, page: int) -> str:
    parsed = urlparse(category_url)
    q = parse_qs(parsed.query)
    q["page"] = [str(page)]
    new_query = urlencode({k: v[0] for k, v in q.items()})
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))


def fetch_html(url: str) -> str:
    headers = {"User-Agent": USER_AGENT, "Referer": BASE_URL}
    r = requests.get(url, headers=headers, timeout=25)
    r.raise_for_status()
    return r.text


@st.cache_data(ttl=1800, show_spinner=False)
def fetch_html_cached(url: str) -> str:
    return fetch_html(url)


def extract_total_count(html_text: str) -> int | None:
    t = clean_text(html_text)
    m = re.search(r"TOTAL\s*[:：]?\s*(\d+)", t, flags=re.I)
    if m:
        return int(m.group(1))
    return None


def parse_product_cards_from_category_html(category_url: str, html_text: str) -> list[dict]:
    soup = BeautifulSoup(html_text, "html.parser")
    records = []
    candidates = soup.select("li[id^='anchorBoxId_']")
    if not candidates:
        candidates = soup.select("ul.prdList > li, .xans-product-listnormal li, .prdList li")

    for li in candidates:
        li_html = str(li)
        pno = ""
        m = re.search(r"anchorBoxId_(\d+)", li.get("id", ""))
        if m:
            pno = m.group(1)

        link = ""
        for a in li.select("a[href]"):
            href = to_abs_url(a.get("href", ""))
            if not href:
                continue
            if any(bad in href.lower() for bad in EXCLUDED_URL_KEYWORDS):
                continue
            found_pno = extract_product_no(href)
            if found_pno:
                pno = pno or found_pno
                link = normalize_product_url(href)
                break

        if not pno:
            m = re.search(r"product_no=(\d+)", li_html)
            if m:
                pno = m.group(1)
                link = f"{BASE_URL}/product/detail.html?product_no={pno}"

        if not pno:
            continue

        text = clean_text(li.get_text(" ", strip=True))
        name = ""
        m = re.search(r"상품명\s*[:：]\s*(.*?)(?:상품 요약설명|판매가|할인판매가|$)", text)
        if m:
            name = clean_text(m.group(1))
        if not name:
            texts = [clean_text(a.get_text(" ", strip=True)) for a in li.select("a")]
            texts = [x for x in texts if len(x) >= 4 and not re.fullmatch(r"자세히|장바구니 담기|관심상품 등록 전|할인기간|닫기", x)]
            if texts:
                name = max(texts, key=len)

        price = ""
        m = re.search(r"할인판매가\s*[:：]\s*([0-9,]+)원", text)
        if m:
            price = m.group(1).replace(",", "")
        if not price:
            m = re.search(r"판매가\s*[:：]\s*([0-9,]+)원", text)
            if m:
                price = m.group(1).replace(",", "")

        summary = ""
        m = re.search(r"상품 요약설명\s*[:：]\s*(.*?)(?:판매가|할인판매가|$)", text)
        if m:
            summary = clean_text(m.group(1))

        records.append({
            "product_no": pno,
            "product_url": link or f"{BASE_URL}/product/detail.html?product_no={pno}",
            "card_name": name,
            "card_price": price,
            "card_summary": summary,
        })

    cleaned = []
    seen = set()
    for r in records:
        pno = clean_text(r.get("product_no"))
        if not pno.isdigit():
            continue
        name = clean_text(r.get("card_name"))
        if not name or name in {"전체상품", "이번주 신상"}:
            continue
        if pno in seen:
            continue
        seen.add(pno)
        cleaned.append(r)
    return cleaned


def collect_product_cards_from_category(category_url: str, max_products: int = 500, delay_sec: float = 0.2) -> tuple[list[dict], int | None]:
    first_html = fetch_html_cached(category_url)
    total_count = extract_total_count(first_html)
    all_cards = []
    seen = set()
    page = 1
    empty_streak = 0
    max_pages = 50

    while page <= max_pages:
        page_url = category_url if page == 1 else build_page_url(category_url, page)
        try:
            html_text = first_html if page == 1 else fetch_html(page_url)
        except Exception:
            break

        cards = parse_product_cards_from_category_html(category_url, html_text)
        newly_added = 0
        for c in cards:
            pno = c["product_no"]
            if pno in seen:
                continue
            seen.add(pno)
            all_cards.append(c)
            newly_added += 1
            if len(all_cards) >= max_products:
                return all_cards, total_count

        if newly_added == 0:
            empty_streak += 1
        else:
            empty_streak = 0
        if total_count and len(all_cards) >= total_count:
            break
        if empty_streak >= 2:
            break

        page += 1
        if delay_sec > 0:
            time.sleep(delay_sec)

    return all_cards, total_count


def normalize_name(name: str) -> str:
    name = clean_text(name)
    name = re.sub(r"\s*\([^)]*color[^)]*\)", "", name, flags=re.I)
    return clean_text(name)


def infer_category_from_name(name: str) -> tuple[str, str]:
    name_l = (name or "").lower()
    pairs = [
        ("아우터", "자켓", ["자켓", "재킷", "jk"]),
        ("아우터", "점퍼", ["점퍼", "후드", "사파리"]),
        ("아우터", "코트", ["코트"]),
        ("니트/가디건", "니트", ["니트"]),
        ("니트/가디건", "가디건", ["가디건"]),
        ("팬츠", "슬랙스", ["슬랙스"]),
        ("팬츠", "데님", ["데님", "청바지", "진"]),
        ("팬츠", "팬츠", ["팬츠", "바지"]),
        ("블라우스/셔츠", "블라우스", ["블라우스"]),
        ("블라우스/셔츠", "셔츠", ["셔츠"]),
        ("티셔츠", "티셔츠", ["티셔츠", "맨투맨", "mtm"]),
        ("원피스/스커트", "원피스", ["원피스"]),
        ("원피스/스커트", "스커트", ["스커트"]),
    ]
    for cat, sub, kws in pairs:
        if any(kw in name_l for kw in kws):
            return cat, sub
    return "", ""


def infer_fabric(text: str) -> str:
    t = clean_text(text)
    patterns = [
        r"(면\s*\d+%[^\n,.]*)", r"(코튼\s*\d+%[^\n,.]*)", r"(폴리(?:에스터)?\s*\d+%[^\n,.]*)",
        r"(레이온\s*\d+%[^\n,.]*)", r"(울\s*\d+%[^\n,.]*)", r"(비스코스\s*\d+%[^\n,.]*)",
        r"(나일론\s*\d+%[^\n,.]*)", r"(스판(?:덱스)?\s*\d+%[^\n,.]*)",
    ]
    found = []
    for p in patterns:
        for m in re.findall(p, t, flags=re.I):
            cm = clean_text(m)
            if cm not in found:
                found.append(cm)
    return " / ".join(found[:4]) if found else ""


def infer_fit_type(text: str, category: str = "", sub_category: str = "") -> str:
    """상품명/상세설명에서 실제 핏 키워드를 추출합니다.

    기존처럼 정핏/세미루즈/루즈핏/오버핏 같은 상의 중심 핏만 넣지 않고,
    하의 실루엣(와이드/일자/부츠컷/배기/조거 등), 스커트·원피스 라인(A라인/H라인/플레어),
    어깨·소매 핏(레글런/드롭숄더/퍼프/벌룬 등)도 세미콜론(;) 태그로 함께 저장합니다.
    픽톡 추천에서는 "일자", "와이드", "레글런"처럼 contains 검색으로 필터링할 수 있습니다.
    """
    t = clean_text(text)
    cat = clean_text(category)
    sub = clean_text(sub_category)
    out = []

    def add(label: str, keys: list[str]):
        if label not in out and any(k.lower() in t.lower() for k in keys):
            out.append(label)

    # 하의 전용: 고객이 실제로 요청하는 팬츠 핏을 우선 추출
    if cat == "팬츠" or sub in ["팬츠", "슬랙스", "데님"]:
        add("세미와이드핏", ["세미 와이드", "세미와이드"])
        add("와이드핏", ["와이드", "통팬츠"])
        add("세미부츠컷", ["세미 부츠컷", "세미부츠컷"])
        add("부츠컷", ["부츠컷"])
        add("일자핏", ["일자", "스트레이트", "straight"])
        add("배기핏", ["배기", "배기핏"])
        add("조거핏", ["조거", "죠거"])
        add("테이퍼드핏", ["테이퍼드", "배럴", "barrel"])
        add("슬림핏", ["슬림", "슬림핏"])
        add("스키니핏", ["스키니"])
        add("루즈핏", ["루즈", "루즈핏", "낙낙"])
        add("밴딩핏", ["밴딩", "이밴드", "허리 밴드"])
        add("핀턱핏", ["핀턱", "턱 팬츠", "턱팬츠"])

    # 원피스/스커트 라인
    if cat == "원피스/스커트" or sub in ["원피스", "스커트"]:
        add("A라인", ["a라인", "a line", "에이라인"])
        add("H라인", ["h라인", "h line", "에이치라인"])
        add("플레어핏", ["플레어", "훌", "훌스커트"])
        add("머메이드핏", ["머메이드"])
        add("랩핏", ["랩스커트", "랩 원피스", "랩원피스", "랩 스타일"])
        add("슬림핏", ["슬림", "슬림핏"])
        add("루즈핏", ["루즈", "루즈핏", "낙낙"])

    # 상의/아우터 공통 전체 핏
    add("오버핏", ["오버핏", "오버 핏"])
    add("루즈핏", ["루즈핏", "루즈 핏", "낙낙한 핏", "넉넉한 핏"])
    add("세미루즈", ["세미루즈", "세미 루즈", "여유 있는 핏", "여유있는 핏", "살짝 여유", "적당히 여유"])
    add("슬림핏", ["슬림핏", "슬림한 핏", "슬림하게"])
    add("정핏", ["정핏", "정 사이즈 핏", "단정한 핏", "기본 핏", "레귤러핏", "레귤러 핏"])

    # 어깨/암홀/소매 실루엣도 fit_type에 함께 저장
    add("레글런핏", ["레글런", "래글런", "raglan"])
    add("드롭숄더", ["드롭숄더", "드롭 숄더", "어깨가 드롭"])
    add("퍼프소매", ["퍼프", "퍼프소매", "퍼프 소매"])
    add("벌룬소매", ["벌룬", "벌룬소매", "볼륨소매", "볼륨 소매"])
    add("플레어소매", ["플레어 소매", "나팔소매", "나팔 소매"])
    add("캡소매", ["캡소매", "캡 소매"])
    add("민소매핏", ["민소매", "나시", "슬리브리스"])

    # 텍스트에 명시된 핏이 없을 때만 최소 기본값 보정
    if not out:
        # 카테고리 수집 오류가 있을 수 있으므로 상품명에 다른 품목명이 명확하면 기본핏을 억지 입력하지 않습니다.
        non_pants_keys = ["셔츠", "티셔츠", "블라우스", "가디건", "니트", "자켓", "점퍼", "코트", "가방", "원피스", "스커트"]
        is_non_pants_name = any(k in t for k in non_pants_keys)
        if cat == "팬츠" and not is_non_pants_name:
            return "팬츠기본핏"
        if cat == "원피스/스커트" and not any(k in t for k in ["팬츠", "슬랙스", "바지"]):
            return "기본라인"
        return ""
    return ";".join(out[:8])

def infer_style_tags(text: str, name: str) -> str:
    t = f"{name} {text}"
    rules = [
        ("클래식", ["클래식", "단정", "라운드 자켓"]),
        ("페미닌", ["페미닌", "여성스러운", "플라워", "트위드"]),
        ("데일리", ["데일리", "기본", "편하게"]),
        ("오피스룩", ["오피스", "출근룩", "직장인"]),
        ("학모룩", ["학부모", "학교", "상담룩"]),
        ("모임룩", ["모임룩", "하객룩"]),
    ]
    out = [tag for tag, keys in rules if any(k in t for k in keys)]
    return ";".join(out[:4])


def infer_season(text: str, name: str) -> str:
    t = f"{name} {text}"
    out = []
    for tag, keys in [
        ("봄", ["봄", "간절기"]),
        ("여름", ["여름", "반팔", "린넨"]),
        ("가을", ["가을", "간절기"]),
        ("겨울", ["겨울", "울", "기모"]),
        ("간절기", ["간절기"]),
    ]:
        if any(k in t for k in keys) and tag not in out:
            out.append(tag)
    return ";".join(out[:3])


def infer_length_type(name: str, text: str) -> str:
    t = f"{name} {text}"
    if "크롭" in t:
        return "크롭"
    if "롱" in t:
        return "롱"
    if "하프" in t:
        return "하프"
    return "기본"


def infer_sleeve_type(name: str, text: str, category: str = "") -> str:
    t = f"{name} {text}"
    if category in ["팬츠", "원피스/스커트"] and "원피스" not in t:
        return "없음"
    if any(k in t for k in ["민소매", "나시", "슬리브리스"]):
        return "민소매"
    if any(k in t for k in ["캡소매", "캡 소매"]):
        return "캡소매"
    if "반팔" in t:
        return "반팔"
    if any(k in t for k in ["7부", "칠부"]):
        return "7부소매"
    if any(k in t for k in ["5부", "오부"]):
        return "5부소매"
    if any(k in t for k in ["퍼프", "퍼프소매"]):
        return "퍼프소매"
    if any(k in t for k in ["벌룬", "볼륨소매", "벌룬소매"]):
        return "벌룬소매"
    if any(k in t for k in ["레글런", "래글런"]):
        return "레글런소매"
    if "드롭숄더" in t or "드롭 숄더" in t:
        return "드롭숄더"
    if any(k in t for k in ["긴팔", "롱슬리브", "소매"]):
        return "긴팔"
    return ""


def infer_color_options(text: str, name: str) -> str:
    t = f"{name} {text}"
    colors = [c for c in ["블랙", "화이트", "아이보리", "베이지", "그레이", "핑크", "네이비", "브라운", "카키", "소라"] if c in t]
    m = re.search(r"\((\d+)\s*color\)", name, flags=re.I)
    if not colors and m:
        return f"{m.group(1)}컬러"
    return ";".join(colors[:6]) if colors else ""


def infer_body_cover(text: str, name: str) -> str:
    t = f"{name} {text}"
    rules = [
        ("팔뚝커버", ["팔뚝", "퍼프", "레글런"]),
        ("뱃살커버", ["복부", "배라인", "군살"]),
        ("힙커버", ["힙", "롱기장", "하프"]),
        ("허리라인보정", ["허리선", "라인", "A라인"]),
    ]
    out = [tag for tag, keys in rules if any(k in t for k in keys)]
    return ";".join(out[:4])


def infer_recommended_body_type(name: str, text: str) -> str:
    t = f"{name} {text}"
    out = []
    if any(k in t for k in ["어깨", "레글런"]):
        out.append("어깨좁음")
    if any(k in t for k in ["팔뚝", "퍼프"]):
        out.append("팔뚝통통")
    if any(k in t for k in ["복부", "배라인", "허리"]):
        out.append("복부체형")
    if any(k in t for k in ["와이드", "A라인", "힙"]):
        out.append("하체통통")
    if not out:
        out.append("4050 여성 일반체형")
    return ";".join(out[:3])


def infer_coordination_items(name: str, text: str) -> str:
    t = f"{name} {text}"
    out = []
    for item in ["슬랙스", "데님", "스커트", "원피스", "니트"]:
        if item in t:
            out.append(item)
    return ";".join(out[:4]) if out else "슬랙스;데님;스커트"


def extract_detail_text_blocks(soup: BeautifulSoup) -> str:
    blocks = []
    selectors = [
        "meta[property='og:title']",
        ".headingArea h2", ".headingArea h3", ".headingArea .name",
        ".infoArea", ".xans-product-detaildesign", ".prdInfo", ".detailArea",
        "#prdDetail", ".cont", ".ec-base-table", ".xans-product-additional"
    ]
    for sel in selectors:
        for node in soup.select(sel):
            if node.name == "meta":
                txt = clean_text(node.get("content", ""))
            else:
                txt = clean_text(node.get_text(" ", strip=True))
            if txt:
                blocks.append(txt)
    return "\n".join(uniq_keep_order(blocks))[:20000]


def extract_size_context(soup: BeautifulSoup, full_text: str) -> str:
    blocks = []
    for sel in [".infoArea", ".xans-product-detaildesign", ".detailArea", "#prdDetail", ".ec-base-table", "table"]:
        for node in soup.select(sel):
            txt = clean_text(node.get_text(" ", strip=True))
            if any(k.lower() in txt.lower() for k in ["사이즈", "추천", "free", "프리", "55", "66", "77", "88", "xl", "l(", "xl("]):
                blocks.append(txt)
    if not blocks:
        blocks.append(full_text)
    return " ".join(uniq_keep_order(blocks))[:12000]


def infer_size_range(text: str) -> tuple[str, str]:
    t = clean_text(text)
    if not t:
        return "", ""

    ranges = re.findall(r"(44|55반|55|66반|66|77반|77|88|99)\s*[-~]\s*(44|55반|55|66반|66|77반|77|88|99)", t)
    if ranges:
        order = {"44":1,"55":2,"55반":3,"66":4,"66반":5,"77":6,"77반":7,"88":8,"99":9}
        mins = sorted(ranges, key=lambda x: order.get(x[0], 999))[0][0]
        maxs = sorted(ranges, key=lambda x: order.get(x[1], -1))[-1][1]
        return f"{mins}-{maxs}", "option_range"

    m = re.search(r"(44|55반|55|66반|66|77반|77|88|99)\s*까지\s*(?:추천|착용|가능)?", t)
    if m:
        return f"55-{m.group(1)}", "recommend_text"

    singles = re.findall(r"(?<!\d)(44|55반|55|66반|66|77반|77|88|99)(?!\d)", t)
    if len(singles) >= 2:
        uniq = []
        for s in singles:
            if s not in uniq:
                uniq.append(s)
        return f"{uniq[0]}-{uniq[-1]}", "size_tokens"

    if re.search(r"\bFREE\b|프리사이즈|FREE사이즈|F사이즈|\bF\b", t, re.I):
        return "55-66", "free_default"

    return "", ""


def _extract_number(text: str) -> str:
    m = re.search(r"(-?\d+(?:\.\d+)?)", clean_text(text))
    return m.group(1) if m else ""


def _normalize_measure_header(header: str) -> str:
    h = clean_text(header).replace(" ", "")
    if not h:
        return ""
    if "어깨" in h:
        return "shoulder"
    if "가슴" in h and "둘레" in h:
        return "chest_circumference"
    if "가슴" in h:
        return "chest"
    if "암홀" in h:
        return "armhole"
    if "소매" in h and "둘레" in h:
        return "sleeve_circumference"
    if "소매" in h:
        return "sleeve"
    if ("총장" in h or "기장" in h) and "앞" in h:
        return "length_front"
    if ("총장" in h or "기장" in h) and "뒤" in h:
        return "length_back"
    if "총장" in h or "기장" in h:
        return "length"
    return ""


def _measurement_payload():
    return {
        "shoulder": "",
        "chest": "",
        "chest_measure_type": "",
        "armhole": "",
        "sleeve": "",
        "sleeve_circumference": "",
        "length": "",
        "length_front": "",
        "length_back": "",
        "measurement_source": "",
        "raw_measurements": "",
    }


def _apply_measure_value(payload: dict, key: str, raw_value: str):
    value = _extract_number(raw_value)
    if not value:
        return
    if key == "chest_circumference":
        payload["raw_measurements"] = payload.get("raw_measurements", "")
        payload["chest_measure_type"] = "circumference"
        try:
            f = float(value)
            payload["chest"] = str(int(f / 2)) if float(f / 2).is_integer() else f"{f/2:.1f}"
        except Exception:
            payload["chest"] = value
        return
    payload[key] = value


def parse_measurement_tables(soup: BeautifulSoup) -> dict:
    payload = _measurement_payload()
    raw_pairs = []
    found = False

    for table in soup.select("table"):
        rows = []
        for tr in table.select("tr"):
            cells = [clean_text(c.get_text(" ", strip=True)) for c in tr.select("th,td")]
            cells = [c for c in cells if c]
            if cells:
                rows.append(cells)
        if not rows:
            continue

        flat = " ".join([" ".join(r) for r in rows])
        if not any(k in flat for k in ["어깨", "가슴", "암홀", "소매", "총장", "기장"]):
            continue

        # case 1: horizontal table headers + values
        if len(rows) >= 2:
            headers = rows[0]
            values = rows[1]
            if len(headers) == len(values) and len(headers) >= 2:
                local_hits = 0
                for h, v in zip(headers, values):
                    key = _normalize_measure_header(h)
                    if key:
                        _apply_measure_value(payload, key, v)
                        raw_pairs.append({clean_text(h): _extract_number(v)})
                        local_hits += 1
                if local_hits >= 2:
                    found = True

        # case 2: vertical th-td pairs
        for row in rows:
            if len(row) >= 2:
                key = _normalize_measure_header(row[0])
                if key:
                    _apply_measure_value(payload, key, row[1])
                    raw_pairs.append({clean_text(row[0]): _extract_number(row[1])})
                    found = True

    if payload["length"] == "":
        vals = [v for v in [payload["length_front"], payload["length_back"]] if v]
        if vals:
            try:
                payload["length"] = str(max(float(v) for v in vals)).rstrip('0').rstrip('.')
            except Exception:
                payload["length"] = vals[-1]

    if found:
        payload["measurement_source"] = "table"
        payload["raw_measurements"] = json.dumps(raw_pairs, ensure_ascii=False)
    return payload


def parse_measurements_from_text(full_text: str) -> dict:
    payload = _measurement_payload()
    patterns = {
        "shoulder": [r"어깨단면", r"어깨"],
        "chest": [r"가슴단면", r"가슴"],
        "armhole": [r"암홀둘레", r"암홀"],
        "sleeve": [r"소매길이", r"소매장", r"소매"],
        "sleeve_circumference": [r"소매둘레"],
        "length_front": [r"총장\(앞\)", r"앞총장"],
        "length_back": [r"총장\(뒤\)", r"뒤총장"],
        "length": [r"총장", r"기장"],
    }
    raw_pairs = []
    for field, keys in patterns.items():
        for key in keys:
            m = re.search(rf"(?:{key})\s*[:：]?\s*(-?\d+(?:\.\d+)?)", full_text)
            if m:
                payload[field] = m.group(1)
                raw_pairs.append({key: m.group(1)})
                break

    m = re.search(r"가슴둘레\s*[:：]?\s*(-?\d+(?:\.\d+)?)", full_text)
    if m and not payload["chest"]:
        raw_val = m.group(1)
        payload["chest_measure_type"] = "circumference"
        try:
            f = float(raw_val)
            payload["chest"] = str(int(f / 2)) if float(f / 2).is_integer() else f"{f/2:.1f}"
        except Exception:
            payload["chest"] = raw_val
        raw_pairs.append({"가슴둘레": raw_val})

    if payload["length"] == "":
        vals = [v for v in [payload["length_front"], payload["length_back"]] if v]
        if vals:
            try:
                payload["length"] = str(max(float(v) for v in vals)).rstrip('0').rstrip('.')
            except Exception:
                payload["length"] = vals[-1]

    if any(payload[k] for k in ["shoulder", "chest", "armhole", "sleeve", "sleeve_circumference", "length", "length_front", "length_back"]):
        payload["measurement_source"] = "text"
        payload["raw_measurements"] = json.dumps(raw_pairs, ensure_ascii=False)
    return payload


def parse_detail_page(url: str, fallback_name: str = "", fallback_price: str = "", fallback_summary: str = "") -> dict:
    html_text = fetch_html_cached(url)
    soup = BeautifulSoup(html_text, "html.parser")
    full_text = extract_detail_text_blocks(soup)

    name = fallback_name
    for sel in [".headingArea h2", ".headingArea h3", ".headingArea .name"]:
        node = soup.select_one(sel)
        if node and clean_text(node.get_text(" ", strip=True)):
            name = clean_text(node.get_text(" ", strip=True))
            break
    if not name:
        meta = soup.select_one("meta[property='og:title']")
        if meta:
            name = clean_text(meta.get("content", ""))
    name = normalize_name(name)

    price = fallback_price
    if not price:
        text = clean_text(full_text)
        m = re.search(r"할인판매가\s*[:：]?\s*([0-9,]+)원", text)
        if m:
            price = m.group(1).replace(",", "")
        else:
            m = re.search(r"판매가\s*[:：]?\s*([0-9,]+)원", text)
            if m:
                price = m.group(1).replace(",", "")

    category, sub_category = infer_category_from_name(name)
    fabric = infer_fabric(full_text)
    size_range, size_source = infer_size_range(extract_size_context(soup, full_text))
    fit_type = infer_fit_type(f"{name} {full_text}", category, sub_category)
    recommended_body_type = infer_recommended_body_type(name, full_text)
    body_cover_features = infer_body_cover(full_text, name)
    style_tags = infer_style_tags(full_text, name)
    season = infer_season(full_text, name)
    length_type = infer_length_type(name, full_text)
    sleeve_type = infer_sleeve_type(name, full_text, category)
    color_options = infer_color_options(full_text, name)
    coordination_items = infer_coordination_items(name, full_text)

    measurements = parse_measurement_tables(soup)
    if not measurements.get("measurement_source"):
        measurements = parse_measurements_from_text(full_text)

    summary_src = fallback_summary or full_text
    summary_clean = clean_text(summary_src)
    summary_clean = re.sub(r"^.*?상품 요약설명\s*[:：]?", "", summary_clean)
    summary_clean = re.sub(r"(최근 본 상품|전체상품목록 바로가기|본문 바로가기|LOGIN|JOIN|MYPAGE|CART|ABOUT|SHOP).*$", "", summary_clean)
    product_summary = clean_text(summary_clean)[:220] or clean_text(name)[:220]

    return {
        "product_no": extract_product_no(url),
        "product_url": normalize_product_url(url),
        "product_name": name,
        "category": category,
        "sub_category": sub_category,
        "price": price,
        "fabric": fabric,
        "fit_type": fit_type,
        "size_range": size_range or "",
        "recommended_body_type": recommended_body_type,
        "body_cover_features": body_cover_features,
        "style_tags": style_tags,
        "season": season,
        "length_type": length_type,
        "sleeve_type": sleeve_type,
        "color_options": color_options,
        "recommended_age": "4050",
        "coordination_items": coordination_items,
        "product_summary": product_summary,
        **measurements,
        "_size_source": size_source,
    }


def normalize_with_openai(row: dict) -> dict:
    client = get_client()
    if client is None:
        return row
    normalize_cols = BASE_COLUMNS
    prompt = f"""
다음 미샵 상품 정보를 미야언니 DB 형식으로 표준화하세요.
반드시 아래 키만 JSON으로 반환하세요.
키 순서:
{normalize_cols}
규칙:
- 출력은 한국어
- style_tags, body_cover_features, coordination_items, season은 세미콜론(;)으로 구분
- product_summary는 120자 내외
- 빈 값은 추론하되 과장 금지
- 제공된 product_no, product_url, product_name, price는 바꾸지 말 것
입력 데이터:
{json.dumps({k: row.get(k, '') for k in DB_COLUMNS}, ensure_ascii=False)}
"""
    try:
        resp = client.chat.completions.create(
            model="gpt-4.1-mini",
            temperature=0.2,
            messages=[
                {"role": "system", "content": "너는 패션 상품 DB 정규화 전문가다."},
                {"role": "user", "content": prompt},
            ],
        )
        text = resp.choices[0].message.content.strip()
        m = re.search(r"\{.*\}", text, re.S)
        if not m:
            return row
        data = json.loads(m.group(0))
        out = row.copy()
        for col in normalize_cols:
            out[col] = clean_text(data.get(col, row.get(col, "")))
        return out
    except Exception:
        return row


def build_dataframe(rows: list[dict]) -> pd.DataFrame:
    normalized = []
    for r in rows:
        item = {col: clean_text(r.get(col, "")) for col in DB_COLUMNS}
        normalized.append(item)
    df = pd.DataFrame(normalized, columns=DB_COLUMNS)
    if not df.empty:
        df = df.drop_duplicates(subset=["product_no"], keep="first")
        df = df[df["product_no"].astype(str).str.fullmatch(r"\d+")]
        df = df[df["product_name"].astype(str).str.len() >= 2]
    return df.reset_index(drop=True)


def analyze_urls(input_text: str, use_openai: bool, delay_sec: float, max_products: int):
    urls = [clean_text(x) for x in re.split(r"[\n,]", input_text) if clean_text(x)]
    if not urls:
        return pd.DataFrame(columns=DB_COLUMNS), []

    audit = []
    rows = []
    product_targets = []

    for url in urls:
        if is_category_url(url):
            cards, total_count = collect_product_cards_from_category(url, max_products=max_products, delay_sec=delay_sec)
            for c in cards:
                product_targets.append(c)
            audit.append({
                "input_url": url,
                "type": "category",
                "detected_total": total_count or "",
                "collected_products": len(cards),
            })
        elif is_product_url(url):
            purl = normalize_product_url(url)
            product_targets.append({
                "product_no": extract_product_no(purl),
                "product_url": purl,
                "card_name": "",
                "card_price": "",
                "card_summary": "",
            })
            audit.append({
                "input_url": url,
                "type": "product",
                "detected_total": "",
                "collected_products": 1,
            })

    dedup_targets = []
    seen = set()
    for t in product_targets:
        pno = clean_text(t.get("product_no"))
        if not pno or pno in seen:
            continue
        seen.add(pno)
        dedup_targets.append(t)

    prog = st.progress(0)
    status = st.empty()
    total = len(dedup_targets)

    for i, t in enumerate(dedup_targets, start=1):
        status.info(f"{i}/{total} 처리 중: {t.get('product_no')} {t.get('card_name','')[:40]}")
        try:
            row = parse_detail_page(
                t["product_url"],
                fallback_name=t.get("card_name", ""),
                fallback_price=t.get("card_price", ""),
                fallback_summary=t.get("card_summary", ""),
            )
            if use_openai:
                row = normalize_with_openai(row)
            rows.append(row)
        except Exception as e:
            audit.append({
                "input_url": t.get("product_url", ""),
                "type": "detail_error",
                "detected_total": "",
                "collected_products": str(e),
            })
        prog.progress(i / max(total, 1))
        if delay_sec > 0:
            time.sleep(delay_sec)

    status.success(f"완료: {len(rows)}개 상품 DB 생성")
    df = build_dataframe(rows)
    return df, audit


st.title("미샵 상품 DB 생성기")
st.caption("상품 URL 또는 카테고리 URL을 넣으면 미야언니용 DB CSV를 생성합니다. 실측 표(어깨/가슴/소매/총장) 파싱을 우선 시도합니다.")

with st.sidebar:
    st.subheader("설정")
    use_openai = st.toggle("OpenAI로 속성 정규화", value=False)
    max_products = st.number_input("카테고리 최대 수집 상품 수", min_value=1, max_value=2000, value=500, step=50)
    delay_sec = st.slider("요청 간 딜레이(초)", min_value=0.0, max_value=2.0, value=0.2, step=0.1)
    st.markdown("- 카테고리에서는 **상품 카드만** 수집합니다.")
    st.markdown("- 실측은 **HTML 테이블 우선 / 텍스트 보조** 방식으로 추출합니다.")
    st.markdown("- 가슴둘레가 있는 경우 chest 컬럼에는 단면 환산값(둘레÷2)을 저장합니다.")

input_text = st.text_area(
    "상품 URL / 카테고리 URL 입력",
    height=160,
    placeholder="https://www.misharp.co.kr/product/list.html?cate_no=541\nhttps://www.misharp.co.kr/product/detail.html?product_no=28579",
)

col1, col2 = st.columns([1, 1])
with col1:
    run = st.button("CSV 생성 시작", use_container_width=True, type="primary")
with col2:
    preview_only = st.button("카테고리 상품 URL 미리보기", use_container_width=True)

if preview_only and input_text.strip():
    urls = [clean_text(x) for x in re.split(r"[\n,]", input_text) if clean_text(x)]
    preview_rows = []
    for u in urls:
        if is_category_url(u):
            try:
                cards, total_count = collect_product_cards_from_category(u, max_products=max_products, delay_sec=delay_sec)
                for c in cards:
                    preview_rows.append({
                        "product_no": c["product_no"],
                        "product_name": c["card_name"],
                        "product_url": c["product_url"],
                    })
                st.info(f"카테고리 예상 TOTAL: {total_count or '미확인'} / 실제 수집: {len(cards)}")
            except Exception as e:
                st.error(f"미리보기 오류: {e}")
        elif is_product_url(u):
            preview_rows.append({
                "product_no": extract_product_no(u),
                "product_name": "",
                "product_url": normalize_product_url(u),
            })
    if preview_rows:
        st.dataframe(pd.DataFrame(preview_rows), use_container_width=True)

if run and input_text.strip():
    try:
        df, audit = analyze_urls(input_text, use_openai, delay_sec, max_products)
        st.success(f"최종 DB 행 수: {len(df)}")
        st.dataframe(df, use_container_width=True)

        csv_bytes = df.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            "미야언니 DB CSV 다운로드",
            data=csv_bytes,
            file_name="misharp_miya_db.csv",
            mime="text/csv",
            use_container_width=True,
        )

        audit_df = pd.DataFrame(audit)
        if not audit_df.empty:
            st.subheader("수집 감사 로그")
            st.dataframe(audit_df, use_container_width=True)
            st.download_button(
                "audit CSV 다운로드",
                data=audit_df.to_csv(index=False).encode("utf-8-sig"),
                file_name="misharp_miya_db_audit.csv",
                mime="text/csv",
                use_container_width=True,
            )
    except Exception as e:
        st.error(f"오류: {e}")
