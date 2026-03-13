import os
import re
import json
import time
from urllib.parse import urljoin, urlparse, parse_qs

import pandas as pd
import requests
import streamlit as st
from bs4 import BeautifulSoup
from openai import OpenAI

st.set_page_config(page_title="MISHARP 상품 DB 생성기", layout="wide")

SCHEMA_COLUMNS = [
    "product_no",
    "product_name",
    "category",
    "sub_category",
    "price",
    "fabric",
    "fit_type",
    "size_range",
    "recommended_body_type",
    "body_cover_features",
    "style_tags",
    "season",
    "length_type",
    "sleeve_type",
    "color_options",
    "recommended_age",
    "coordination_items",
    "product_summary",
    "product_url",
]

DEFAULT_ROW = {
    "product_no": "",
    "product_name": "",
    "category": "",
    "sub_category": "",
    "price": "",
    "fabric": "",
    "fit_type": "",
    "size_range": "",
    "recommended_body_type": "4050 여성 일반체형",
    "body_cover_features": "",
    "style_tags": "데일리",
    "season": "간절기",
    "length_type": "기본",
    "sleeve_type": "긴팔",
    "color_options": "",
    "recommended_age": "4050",
    "coordination_items": "슬랙스;데님;스커트",
    "product_summary": "",
    "product_url": "",
}

OPENAI_API_KEY = st.secrets.get("OPENAI_API_KEY", os.getenv("OPENAI_API_KEY", ""))
client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
    )
}

SYSTEM_PROMPT = """
너는 MISHARP 여성의류 쇼핑몰의 상품 DB 정규화 담당자다.
주어진 상품 페이지 원문에서 구조화된 CSV용 속성을 추출한다.

규칙:
- 반드시 JSON 객체 1개만 출력한다.
- 빈칸은 최소화하되, 근거가 부족하면 보수적으로 추론한다.
- 언어는 한국어.
- style_tags, recommended_body_type, body_cover_features, coordination_items, color_options는 세미콜론(;) 구분 문자열로 만든다.
- fit_type은 정핏/세미루즈/루즈핏/슬림핏/오버핏 중 가장 가까운 값.
- season은 봄/여름/가을/겨울/간절기 또는 세미콜론 조합.
- length_type은 크롭/기본/하프/롱 중 선택.
- sleeve_type은 긴팔/반팔/민소매/드롭숄더/퍼프소매 중 가장 가까운 값.
- recommended_age는 기본 4050.
- product_summary는 80자 이내로 핵심만 요약.
- product_no와 product_url은 입력값을 유지한다.
""".strip()


def clean_text(text: str) -> str:
    text = re.sub(r"\s+", " ", str(text or "")).strip()
    return text


def parse_product_no(url: str) -> str:
    if not url:
        return ""
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    if "product_no" in qs and qs["product_no"]:
        return qs["product_no"][0]
    m = re.search(r"/(\d{3,})/(?:category|display|$)", parsed.path)
    if m:
        return m.group(1)
    m = re.search(r"product_no=(\d+)", url)
    return m.group(1) if m else ""


def normalize_url(url: str) -> str:
    url = clean_text(url)
    if not url:
        return ""
    if url.startswith("//"):
        url = "https:" + url
    if not url.startswith("http"):
        url = "https://" + url.lstrip("/")
    return url


def is_product_url(url: str) -> bool:
    low = (url or "").lower()
    return "/product/" in low or "product_no=" in low


def is_category_url(url: str) -> bool:
    low = (url or "").lower()
    return "/category/" in low and not is_product_url(url)


def fetch_html(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=25)
    r.raise_for_status()
    return r.text


def soup_from_url(url: str) -> BeautifulSoup:
    return BeautifulSoup(fetch_html(url), "html.parser")


def get_text_blocks(soup: BeautifulSoup) -> str:
    clone = BeautifulSoup(str(soup), "html.parser")
    for tag in clone(["script", "style", "noscript", "svg", "iframe"]):
        tag.decompose()
    text = clone.get_text("\n")
    text = re.sub(r"\n{2,}", "\n", text)
    lines = [clean_text(x) for x in text.splitlines()]
    lines = [x for x in lines if x]
    return "\n".join(lines)


def extract_product_name(soup: BeautifulSoup) -> str:
    selectors = [
        "#span_product_name",
        ".infoArea #span_product_name",
        ".headingArea h2",
        ".headingArea h3",
        "meta[property='og:title']",
        "title",
    ]
    for sel in selectors:
        el = soup.select_one(sel)
        if not el:
            continue
        if el.name == "meta":
            value = clean_text(el.get("content", ""))
        else:
            value = clean_text(el.get_text(" ", strip=True))
        value = re.sub(r"\s*\|.*$", "", value)
        value = re.sub(r"\s*-\s*미샵.*$", "", value, flags=re.I)
        value = re.sub(r"\s*-\s*MISHARP.*$", "", value, flags=re.I)
        if value and value.lower() not in {"misharp", "미샵"}:
            return value
    return ""


def extract_price(text: str) -> str:
    patterns = [
        r"할인판매가\s*[:：]?\s*([0-9,]+)원",
        r"판매가\s*[:：]?\s*([0-9,]+)원",
        r"price\s*[:：]?\s*([0-9,]+)원",
    ]
    for pat in patterns:
        m = re.search(pat, text, re.I)
        if m:
            return m.group(1).replace(",", "")
    return ""


def extract_fabric(text: str) -> str:
    candidates = []
    for pat in [
        r"소재\s*[:：]\s*([^\n]{1,180})",
        r"fabric\s*[:：]\s*([^\n]{1,180})",
    ]:
        for m in re.finditer(pat, text, re.I):
            val = clean_text(m.group(1))
            if 2 <= len(val) <= 150:
                candidates.append(val)
    for val in candidates:
        if any(k in val for k in ["면", "폴리", "울", "나일론", "레이온", "텐셀", "스판", "아크릴", "모달", "%"]):
            return val
    return candidates[0] if candidates else ""


def extract_size_range(text: str) -> str:
    patterns = [
        r"free사이즈로\s*([0-9가-힣~\-]+)까지\s*추천",
        r"사이즈\s*TIP\s*([^\n]{1,120})",
        r"사이즈\s*[:：]\s*([^\n]{1,120})",
    ]
    for pat in patterns:
        m = re.search(pat, text, re.I)
        if m:
            val = clean_text(m.group(1))
            if "추천" in val and len(val) < 40:
                return val
            if any(k in val for k in ["55", "66", "77", "88", "FREE", "free", "S", "M", "L", "XL"]):
                return val
    hits = []
    for token in ["44", "55", "55반", "66", "66반", "77", "77반", "88", "FREE", "S", "M", "L", "XL"]:
        if re.search(rf"\b{re.escape(token)}\b", text):
            hits.append(token)
    hits = list(dict.fromkeys(hits))
    if hits:
        return "-".join(hits[:4])
    return ""


def extract_colors(text: str) -> str:
    m = re.search(r"모델\s*착용\s*[:：]?\s*([^\n]{1,100})", text)
    if m:
        raw = clean_text(m.group(1))
        raw = raw.replace(",", ";")
        return raw
    colors = [
        "아이보리", "크림", "베이지", "모카", "브라운", "카멜", "그레이", "차콜", "블랙",
        "화이트", "네이비", "블루", "소라", "민트", "카키", "핑크", "라벤더", "와인",
    ]
    found = [c for c in colors if c in text]
    found = list(dict.fromkeys(found))
    return ";".join(found[:6])


def infer_basic_fields(product_name: str, text: str, url: str) -> dict:
    low_name = f"{product_name} {text[:2500]}"
    category = ""
    sub_category = ""
    if any(k in low_name for k in ["자켓", "점퍼", "코트", "패딩", "가디건", "아우터", "블루종"]):
        category = "아우터"
        if "자켓" in low_name:
            sub_category = "자켓"
        elif "코트" in low_name:
            sub_category = "코트"
        elif "가디건" in low_name:
            sub_category = "가디건"
        elif "점퍼" in low_name or "블루종" in low_name:
            sub_category = "점퍼"
    elif any(k in low_name for k in ["니트", "맨투맨", "티셔츠", "블라우스", "셔츠"]):
        category = "상의"
        if "니트" in low_name:
            sub_category = "니트"
        elif "블라우스" in low_name:
            sub_category = "블라우스"
        elif "셔츠" in low_name:
            sub_category = "셔츠"
        elif "맨투맨" in low_name:
            sub_category = "맨투맨"
        else:
            sub_category = "티셔츠"
    elif any(k in low_name for k in ["슬랙스", "팬츠", "데님", "진", "청바지", "스커트"]):
        category = "하의"
        if "스커트" in low_name:
            sub_category = "스커트"
        elif any(k in low_name for k in ["데님", "진", "청바지"]):
            sub_category = "데님"
        else:
            sub_category = "팬츠"
    elif any(k in low_name for k in ["원피스", "드레스"]):
        category = "원피스"
        sub_category = "원피스"

    fit_type = "정핏"
    if any(k in low_name for k in ["루즈핏", "루즈", "오버핏"]):
        fit_type = "루즈핏" if "루즈" in low_name else "오버핏"
    elif any(k in low_name for k in ["슬림", "슬리밍"]):
        fit_type = "슬림핏"
    elif any(k in low_name for k in ["세미루즈", "적당히 여유", "레귤러"]):
        fit_type = "세미루즈"

    season = []
    for token in ["봄", "여름", "가을", "겨울", "간절기"]:
        if token in low_name:
            season.append(token)
    if not season:
        season = ["간절기"]

    style_tags = []
    mapping = {
        "클래식": ["클래식", "단정", "테일러드"],
        "페미닌": ["여성스러운", "페미닌", "우아"],
        "데일리": ["데일리", "일상"],
        "오피스룩": ["오피스", "출근"],
        "학모룩": ["학모룩", "학교상담", "학교"],
        "모임룩": ["모임", "하객", "격식"],
    }
    for tag, keys in mapping.items():
        if any(k in low_name for k in keys):
            style_tags.append(tag)
    if not style_tags:
        style_tags = ["데일리"]

    body_cover = []
    for label, keys in {
        "팔뚝커버": ["팔뚝", "소매"],
        "뱃살커버": ["복부", "배", "뱃살"],
        "힙커버": ["힙", "엉덩이"],
        "허리라인보정": ["허리라인", "라인", "A라인"],
    }.items():
        if any(k in low_name for k in keys):
            body_cover.append(label)

    coord = []
    for item in ["슬랙스", "데님", "스커트", "원피스", "니트"]:
        if item in low_name:
            coord.append(item)
    if not coord:
        coord = ["슬랙스", "데님", "스커트"]

    length_type = "기본"
    if any(k in low_name for k in ["크롭"]):
        length_type = "크롭"
    elif any(k in low_name for k in ["하프"]):
        length_type = "하프"
    elif any(k in low_name for k in ["롱"]):
        length_type = "롱"

    sleeve_type = "긴팔"
    if "반팔" in low_name:
        sleeve_type = "반팔"
    elif "민소매" in low_name:
        sleeve_type = "민소매"
    elif "퍼프" in low_name:
        sleeve_type = "퍼프소매"
    elif "드롭숄더" in low_name:
        sleeve_type = "드롭숄더"

    return {
        "category": category,
        "sub_category": sub_category,
        "fit_type": fit_type,
        "season": ";".join(dict.fromkeys(season)),
        "style_tags": ";".join(dict.fromkeys(style_tags)),
        "body_cover_features": ";".join(dict.fromkeys(body_cover)),
        "coordination_items": ";".join(dict.fromkeys(coord)),
        "length_type": length_type,
        "sleeve_type": sleeve_type,
    }


def extract_product_urls_from_category(category_url: str, max_products: int = 0) -> list[str]:
    soup = soup_from_url(category_url)
    candidates = []
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if not href:
            continue
        full = urljoin(category_url, href)
        if is_product_url(full):
            candidates.append(full)
    normalized = []
    seen = set()
    for url in candidates:
        pn = parse_product_no(url)
        if not pn:
            continue
        if pn in seen:
            continue
        seen.add(pn)
        normalized.append(url)
    if max_products and max_products > 0:
        normalized = normalized[:max_products]
    return normalized


def build_product_payload(product_url: str) -> dict:
    soup = soup_from_url(product_url)
    page_text = get_text_blocks(soup)
    product_name = extract_product_name(soup)
    product_no = parse_product_no(product_url)
    heuristics = infer_basic_fields(product_name, page_text, product_url)
    payload = {
        "product_no": product_no,
        "product_name": product_name,
        "price": extract_price(page_text),
        "fabric": extract_fabric(page_text),
        "size_range": extract_size_range(page_text),
        "color_options": extract_colors(page_text),
        "product_url": product_url,
        **heuristics,
        "page_text": page_text[:14000],
    }
    return payload


def merge_row(base: dict, incoming: dict) -> dict:
    row = dict(DEFAULT_ROW)
    row.update(base)
    for key in SCHEMA_COLUMNS:
        val = incoming.get(key, None)
        if val is None:
            continue
        sval = clean_text(val)
        if sval:
            row[key] = sval
    return row


def ai_normalize(payload: dict) -> dict:
    if client is None:
        raise RuntimeError("OPENAI_API_KEY가 없어 AI 정규화를 사용할 수 없습니다.")

    user_prompt = f"""
입력 메타데이터:
{json.dumps({k: v for k, v in payload.items() if k != 'page_text'}, ensure_ascii=False, indent=2)}

상품 원문:
{payload['page_text']}

반드시 아래 키만 가진 JSON 객체 1개로 답해:
{json.dumps(DEFAULT_ROW, ensure_ascii=False, indent=2)}
""".strip()

    response = client.chat.completions.create(
        model="gpt-5-mini",
        temperature=0.2,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
    )
    raw = response.choices[0].message.content
    data = json.loads(raw)
    return merge_row({k: v for k, v in payload.items() if k in SCHEMA_COLUMNS}, data)


def heuristic_only_row(payload: dict) -> dict:
    summary = payload.get("page_text", "")
    summary = re.sub(r"\s+", " ", summary)
    summary = summary[:80]
    base = {
        k: v
        for k, v in payload.items()
        if k in SCHEMA_COLUMNS
    }
    if not base.get("product_summary"):
        base["product_summary"] = summary
    return merge_row(base, {})


def process_urls(urls: list[str], use_ai: bool, crawl_delay: float) -> pd.DataFrame:
    rows = []
    progress = st.progress(0.0)
    status = st.empty()
    total = max(len(urls), 1)

    for idx, url in enumerate(urls, start=1):
        status.info(f"처리 중 {idx}/{total} : {url}")
        try:
            payload = build_product_payload(url)
            row = ai_normalize(payload) if use_ai else heuristic_only_row(payload)
            rows.append(row)
        except Exception as e:
            error_row = dict(DEFAULT_ROW)
            error_row["product_no"] = parse_product_no(url)
            error_row["product_url"] = url
            error_row["product_name"] = f"오류: {type(e).__name__}"
            error_row["product_summary"] = clean_text(str(e))[:200]
            rows.append(error_row)
        progress.progress(idx / total)
        if crawl_delay > 0:
            time.sleep(crawl_delay)

    progress.empty()
    status.empty()
    df = pd.DataFrame(rows)
    for col in SCHEMA_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    return df[SCHEMA_COLUMNS]


def expand_input_urls(raw_urls: list[str], max_products: int) -> list[str]:
    out = []
    for raw in raw_urls:
        url = normalize_url(raw)
        if not url:
            continue
        if is_category_url(url):
            out.extend(extract_product_urls_from_category(url, max_products=max_products))
        elif is_product_url(url):
            out.append(url)
    deduped = []
    seen = set()
    for url in out:
        pn = parse_product_no(url) or url
        if pn in seen:
            continue
        seen.add(pn)
        deduped.append(url)
    return deduped


def to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")


st.title("MISHARP 상품 DB 생성기")
st.caption("상품별 URL 또는 카테고리 URL을 넣으면 미야언니용 CSV DB를 바로 생성합니다.")

with st.sidebar:
    st.subheader("설정")
    use_ai = st.toggle("OpenAI로 속성 정규화", value=bool(client))
    max_products = st.number_input("카테고리당 최대 상품 수", min_value=0, max_value=500, value=50, step=10)
    crawl_delay = st.slider("요청 간 딜레이(초)", min_value=0.0, max_value=2.0, value=0.3, step=0.1)
    st.markdown("- 0은 제한 없이 처리\n- AI 정규화 OFF면 휴리스틱만으로 빠르게 CSV 생성")

sample_text = "\n".join([
    "https://www.misharp.co.kr/product/detail.html?product_no=28522&cate_no=24&display_group=1",
    "https://www.misharp.co.kr/category/%EC%95%84%EC%9A%B0%ED%84%B0/24/",
])

raw = st.text_area(
    "상품 URL / 카테고리 URL 입력",
    value="",
    height=180,
    placeholder=sample_text,
)

col1, col2 = st.columns([1, 1])
with col1:
    run = st.button("CSV 생성 시작", use_container_width=True, type="primary")
with col2:
    preview_only = st.button("URL 확장 미리보기", use_container_width=True)

input_urls = [clean_text(x) for x in raw.splitlines() if clean_text(x)]

if preview_only and input_urls:
    try:
        expanded = expand_input_urls(input_urls, max_products=max_products)
        st.success(f"총 {len(expanded)}개 상품 URL을 찾았습니다.")
        st.dataframe(pd.DataFrame({"product_url": expanded}), use_container_width=True)
    except Exception as e:
        st.error(f"미리보기 실패: {e}")

if run:
    if not input_urls:
        st.error("먼저 URL을 1개 이상 입력해 주세요.")
        st.stop()
    try:
        urls = expand_input_urls(input_urls, max_products=max_products)
    except Exception as e:
        st.error(f"URL 확장 중 오류가 발생했습니다: {e}")
        st.stop()

    if not urls:
        st.error("처리 가능한 상품 URL을 찾지 못했습니다.")
        st.stop()

    st.info(f"총 {len(urls)}개 상품을 처리합니다.")
    df = process_urls(urls, use_ai=use_ai, crawl_delay=float(crawl_delay))
    st.success(f"완료: {len(df)}개 행 생성")
    st.dataframe(df, use_container_width=True, height=520)

    ts = time.strftime("%Y%m%d_%H%M%S")
    st.download_button(
        "CSV 다운로드",
        data=to_csv_bytes(df),
        file_name=f"misharp_product_db_{ts}.csv",
        mime="text/csv",
        use_container_width=True,
    )

with st.expander("출력 컬럼 보기"):
    st.code(", ".join(SCHEMA_COLUMNS))

with st.expander("사용 팁"):
    st.markdown(
        """
1. 상품 URL과 카테고리 URL을 섞어서 넣어도 됩니다.  
2. 카테고리 URL은 내부 상품 링크를 자동으로 수집합니다.  
3. AI 정규화 ON이면 fit_type, style_tags, 체형/커버 포인트가 더 정교해집니다.  
4. 첫 DB는 카테고리별 30~50개씩 나눠서 구축하는 방식이 가장 안정적입니다.
        """
    )
