import os
import re
import json
import time
import html
from urllib.parse import urljoin, urlparse, parse_qs

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
DEFAULT_COLUMNS = [
    "product_no", "product_name", "category", "sub_category", "price", "fabric",
    "fit_type", "size_range", "recommended_body_type", "body_cover_features", "style_tags",
    "season", "length_type", "sleeve_type", "color_options", "recommended_age",
    "coordination_items", "product_summary", "product_url"
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
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def to_abs_url(url: str) -> str:
    if not url:
        return ""
    return urljoin(BASE_URL, url)


def normalize_product_url(url: str) -> str:
    url = to_abs_url(url)
    pno = extract_product_no(url)
    if pno:
        return f"{BASE_URL}/product/detail.html?product_no={pno}"
    return url


def extract_product_no(url: str) -> str:
    if not url:
        return ""
    m = re.search(r"product_no=(\d+)", url)
    if m:
        return m.group(1)
    m = re.search(r"/product/.+/(\d+)(?:/category|/display|/)?", url)
    if m:
        return m.group(1)
    return ""


def extract_cate_no(url: str) -> str:
    if not url:
        return ""
    m = re.search(r"cate_no=(\d+)", url)
    if m:
        return m.group(1)
    return ""


def is_product_url(url: str) -> bool:
    url = (url or "").lower()
    return ("product_no=" in url) or ("/product/detail.html" in url) or bool(re.search(r"/product/.+?/\d+", url))


def is_category_url(url: str) -> bool:
    url = (url or "").lower()
    if is_product_url(url):
        return False
    return ("/product/list.html" in url and "cate_no=" in url) or ("/category/" in url)


def fetch_html(url: str) -> str:
    headers = {"User-Agent": USER_AGENT, "Referer": BASE_URL}
    r = requests.get(url, headers=headers, timeout=20)
    r.raise_for_status()
    return r.text


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_html_cached(url: str) -> str:
    return fetch_html(url)


def gather_product_urls_from_html(category_url: str, html_text: str) -> list[str]:
    urls = []
    soup = BeautifulSoup(html_text, "html.parser")

    # 1) 가장 일반적인 a[href] 수집
    for a in soup.select("a[href]"):
        href = clean_text(a.get("href", ""))
        if not href:
            continue
        abs_url = to_abs_url(href)
        pno = extract_product_no(abs_url)
        if pno:
            urls.append(normalize_product_url(abs_url))

    # 2) HTML 원문에서 직접 추출
    for m in re.finditer(r"(?:href=|location\.href=|product_no=|/product/)([^\"'<>\s]+)", html_text, flags=re.I):
        raw = m.group(0)
        pno = extract_product_no(raw)
        if pno:
            urls.append(f"{BASE_URL}/product/detail.html?product_no={pno}")

    # 3) product_no 만 뽑히는 경우 대비
    for pno in re.findall(r"product_no\s*[=:]\s*['\"]?(\d+)", html_text, flags=re.I):
        urls.append(f"{BASE_URL}/product/detail.html?product_no={pno}")

    # 4) category url 자체가 하나의 상품으로 잘못 들어오는 상황 제거
    filtered = []
    cate_no = extract_cate_no(category_url)
    for u in urls:
        if is_category_url(u) and not extract_product_no(u):
            continue
        filtered.append(u)

    # 순서 유지 중복 제거
    seen = set()
    out = []
    for u in filtered:
        pno = extract_product_no(u)
        if not pno:
            continue
        key = pno
        if key in seen:
            continue
        seen.add(key)
        out.append(f"{BASE_URL}/product/detail.html?product_no={pno}")
    return out


def find_pagination_urls(category_url: str, html_text: str, max_pages: int = 20) -> list[str]:
    page_urls = [category_url]
    cate_no = extract_cate_no(category_url)
    if not cate_no:
        return page_urls

    soup = BeautifulSoup(html_text, "html.parser")
    candidates = []
    for a in soup.select("a[href]"):
        href = clean_text(a.get("href", ""))
        if not href:
            continue
        abs_url = to_abs_url(href)
        if extract_cate_no(abs_url) == cate_no and abs_url not in candidates:
            candidates.append(abs_url)

    numbered = []
    for u in candidates:
        q = parse_qs(urlparse(u).query)
        if "page" in q:
            try:
                numbered.append(int(q["page"][0]))
            except Exception:
                pass

    max_found = max(numbered) if numbered else 1
    max_found = min(max_found, max_pages)

    # 기본 페이지 규칙 추가
    for page in range(2, max_found + 1):
        sep = "&" if "?" in category_url else "?"
        page_urls.append(f"{category_url}{sep}page={page}")

    # 혹시 pagination anchor가 못 잡혀도 최소한 앞 몇 페이지까지 확인 가능하도록 후보 추가
    if len(page_urls) == 1:
        for page in range(2, min(max_pages, 10) + 1):
            sep = "&" if "?" in category_url else "?"
            page_urls.append(f"{category_url}{sep}page={page}")

    # 중복 제거
    dedup = []
    seen = set()
    for u in page_urls:
        if u not in seen:
            seen.add(u)
            dedup.append(u)
    return dedup


def collect_product_urls_from_category(category_url: str, max_products: int = 300, delay_sec: float = 0.2) -> list[str]:
    first_html = fetch_html_cached(category_url)
    page_urls = find_pagination_urls(category_url, first_html)

    all_urls = []
    seen = set()

    for idx, page_url in enumerate(page_urls, start=1):
        try:
            html_text = first_html if idx == 1 else fetch_html(page_url)
        except Exception:
            continue

        product_urls = gather_product_urls_from_html(category_url, html_text)
        newly_added = 0
        for u in product_urls:
            pno = extract_product_no(u)
            if not pno or pno in seen:
                continue
            seen.add(pno)
            all_urls.append(u)
            newly_added += 1
            if len(all_urls) >= max_products:
                return all_urls

        # 새로 추가된 상품이 아예 없으면 뒤 페이지는 그만
        if idx > 1 and newly_added == 0:
            break

        if delay_sec > 0:
            time.sleep(delay_sec)

    return all_urls


def parse_price(text: str) -> str:
    text = clean_text(text)
    m = re.search(r"([0-9][0-9,]{2,})\s*원", text)
    if m:
        return m.group(1).replace(",", "")
    m = re.search(r"([0-9][0-9,]{2,})", text)
    if m:
        return m.group(1).replace(",", "")
    return ""


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
    if found:
        return " / ".join(found[:4])
    return ""


def infer_size_range(text: str) -> str:
    t = clean_text(text)
    tokens = []
    for token in ["44", "55", "55반", "66", "66반", "77", "77반", "88", "FREE", "F"]:
        if token in t and token not in tokens:
            tokens.append(token)
    if "FREE" in tokens or "F" in tokens:
        return "FREE"
    if not tokens:
        m = re.search(r"(\d{2}\s*~\s*\d{2})", t)
        if m:
            return clean_text(m.group(1)).replace(" ~ ", "-")
        return ""
    return "-".join(tokens[:2]) if len(tokens) >= 2 else tokens[0]


def infer_fit_type(text: str) -> str:
    t = clean_text(text)
    rules = [
        ("오버핏", ["오버핏"]),
        ("루즈핏", ["루즈핏"]),
        ("세미루즈", ["세미루즈", "여유 있는 핏", "살짝 여유"]),
        ("슬림핏", ["슬림핏", "라인감", "슬림하게"]),
        ("정핏", ["정핏", "기본핏", "스탠다드핏"]),
    ]
    for label, kws in rules:
        if any(k in t for k in kws):
            return label
    return ""


def infer_tags(text: str, name: str) -> dict:
    t = f"{clean_text(name)} {clean_text(text)}"
    style = []
    body = []
    coord = []

    style_rules = {
        "클래식": ["클래식", "단정", "고급스러운"],
        "페미닌": ["페미닌", "여성스러운", "우아한"],
        "데일리": ["데일리", "매일", "기본템"],
        "오피스룩": ["오피스", "출근룩", "직장"],
        "학모룩": ["학모", "학교", "상담룩"],
        "모임룩": ["모임", "하객", "격식"],
    }
    for tag, kws in style_rules.items():
        if any(k in t for k in kws):
            style.append(tag)

    body_rules = {
        "팔뚝커버": ["팔뚝", "레글런", "드롭숄더"],
        "뱃살커버": ["복부", "뱃살", "허리선 정리"],
        "힙커버": ["힙", "엉덩이", "롱기장"],
        "허리라인보정": ["허리라인", "라인감", "A라인"],
    }
    for tag, kws in body_rules.items():
        if any(k in t for k in kws):
            body.append(tag)

    coord_rules = {
        "슬랙스": ["슬랙스"],
        "데님": ["데님", "청바지"],
        "스커트": ["스커트"],
        "원피스": ["원피스"],
        "니트": ["니트"],
    }
    for tag, kws in coord_rules.items():
        if any(k in t for k in kws):
            coord.append(tag)

    return {
        "style_tags": ";".join(style[:4]),
        "body_cover_features": ";".join(body[:4]),
        "coordination_items": ";".join(coord[:4]),
    }


def infer_season(text: str, name: str) -> str:
    t = f"{clean_text(name)} {clean_text(text)}"
    seasons = []
    if any(k in t for k in ["봄", "간절기", "스프링"]):
        seasons.append("봄")
    if any(k in t for k in ["여름", "썸머", "반팔"]):
        seasons.append("여름")
    if any(k in t for k in ["가을", "간절기"]):
        seasons.append("가을")
    if any(k in t for k in ["겨울", "울", "기모"]):
        seasons.append("겨울")
    if not seasons and any(k in t for k in ["간절기"]):
        seasons.append("간절기")
    return ";".join(dict.fromkeys(seasons))


def infer_length_type(name: str, text: str) -> str:
    t = f"{name} {text}"
    if any(k in t for k in ["크롭"]):
        return "크롭"
    if any(k in t for k in ["롱", "롱기장"]):
        return "롱"
    if any(k in t for k in ["하프"]):
        return "하프"
    return "기본"


def infer_sleeve_type(name: str, text: str) -> str:
    t = f"{name} {text}"
    if "반팔" in t:
        return "반팔"
    if "퍼프" in t:
        return "퍼프소매"
    if "드롭숄더" in t:
        return "드롭숄더"
    return "긴팔"


def infer_colors(text: str) -> str:
    t = clean_text(text)
    colors = []
    for c in ["블랙", "아이보리", "화이트", "베이지", "그레이", "네이비", "카키", "브라운", "핑크", "소라", "블루"]:
        if c in t and c not in colors:
            colors.append(c)
    if not colors:
        m = re.search(r"\((\d+)\s*color\)", t, flags=re.I)
        if m:
            return f"{m.group(1)}컬러"
    return ";".join(colors[:6])


def make_summary(name: str, text: str) -> str:
    t = clean_text(text)
    if not t:
        return name
    sentences = re.split(r"[.!?]|\n", t)
    picked = []
    for s in sentences:
        s = clean_text(s)
        if len(s) >= 10:
            picked.append(s)
        if len(" ".join(picked)) >= 90:
            break
    return clean_text(" ".join(picked))[:140]


def parse_product_page(url: str) -> dict:
    html_text = fetch_html(url)
    soup = BeautifulSoup(html_text, "html.parser")

    name = ""
    for selector in ["#span_product_name", ".headingArea h2", ".headingArea h3", "title"]:
        el = soup.select_one(selector)
        if el:
            name = normalize_name(el.get_text(" ", strip=True))
            if name:
                break

    if not name:
        for m in re.finditer(r"상품명\s*:?\s*([^\n<]+)", soup.get_text("\n"), flags=re.I):
            cand = normalize_name(m.group(1))
            if cand:
                name = cand
                break

    body_text = clean_text(soup.get_text("\n", strip=True))
    pno = extract_product_no(url)
    cat, sub = infer_category_from_name(name)

    price = ""
    for label in ["할인판매가", "판매가"]:
        m = re.search(label + r"\s*:?\s*([0-9][0-9,]{2,}\s*원)", body_text)
        if m:
            price = parse_price(m.group(1))
            break

    tags = infer_tags(body_text, name)
    row = {
        "product_no": pno,
        "product_name": name,
        "category": cat,
        "sub_category": sub,
        "price": price,
        "fabric": infer_fabric(body_text),
        "fit_type": infer_fit_type(body_text),
        "size_range": infer_size_range(body_text),
        "recommended_body_type": "4050 여성 일반체형",
        "body_cover_features": tags["body_cover_features"],
        "style_tags": tags["style_tags"],
        "season": infer_season(body_text, name),
        "length_type": infer_length_type(name, body_text),
        "sleeve_type": infer_sleeve_type(name, body_text),
        "color_options": infer_colors(body_text),
        "recommended_age": "4050",
        "coordination_items": tags["coordination_items"],
        "product_summary": make_summary(name, body_text),
        "product_url": normalize_product_url(url),
    }
    return row


def refine_with_openai(row: dict) -> dict:
    client = get_client()
    if client is None:
        return row

    prompt = f"""
아래 상품 DB 초안을 미야언니용 상품 DB 규격으로 정리해줘.
빈 값은 문맥상 합리적으로 보완하되 과장하지 말고, 반드시 JSON만 반환해.
세미콜론(;)으로 다중값을 표기해.

필드:
{', '.join(DEFAULT_COLUMNS)}

초안:
{json.dumps(row, ensure_ascii=False)}
"""
    try:
        resp = client.chat.completions.create(
            model="gpt-5-mini",
            temperature=0.2,
            messages=[
                {"role": "system", "content": "너는 여성 패션 상품 DB 정규화 전문가다. 반드시 JSON 객체만 반환한다."},
                {"role": "user", "content": prompt},
            ],
        )
        content = resp.choices[0].message.content.strip()
        content = re.sub(r"^```json\s*|\s*```$", "", content, flags=re.S)
        data = json.loads(content)
        for col in DEFAULT_COLUMNS:
            row[col] = clean_text(data.get(col, row.get(col, "")))
        return row
    except Exception:
        return row


def rows_to_csv_bytes(rows: list[dict]) -> bytes:
    df = pd.DataFrame(rows)
    for col in DEFAULT_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    df = df[DEFAULT_COLUMNS]
    return df.to_csv(index=False).encode("utf-8-sig")


st.title("미샵 상품 DB CSV 생성기")
st.caption("상품 URL 또는 카테고리 URL을 넣으면 미야언니용 DB CSV를 생성합니다.")

with st.sidebar:
    st.subheader("설정")
    use_openai = st.toggle("OpenAI로 속성 정규화", value=False, disabled=not bool(get_client()))
    if not get_client():
        st.caption("OPENAI_API_KEY가 없으면 규칙기반 추출로 동작합니다.")
    max_products = st.number_input("카테고리 최대 상품 수", min_value=1, max_value=1000, value=100, step=10)
    delay_sec = st.slider("요청 간 딜레이(초)", 0.0, 2.0, 0.2, 0.1)
    st.markdown("- 카테고리 URL이면 페이지를 순회하며 상품을 최대한 수집합니다.\n- 동일 상품은 product_no 기준으로 중복 제거합니다.\n- 결과 CSV는 UTF-8 BOM으로 저장됩니다.")

urls_text = st.text_area(
    "상품 URL / 카테고리 URL 입력",
    height=180,
    placeholder="https://www.misharp.co.kr/product/detail.html?product_no=28522\nhttps://www.misharp.co.kr/product/list.html?cate_no=541",
)

col1, col2 = st.columns([2, 1])
with col1:
    run_btn = st.button("CSV 생성 시작", use_container_width=True, type="primary")
with col2:
    preview_btn = st.button("URL 확장 미리보기", use_container_width=True)

input_urls = [clean_text(x) for x in urls_text.splitlines() if clean_text(x)]

if preview_btn and input_urls:
    preview_rows = []
    for u in input_urls:
        if is_category_url(u):
            try:
                product_urls = collect_product_urls_from_category(u, max_products=min(30, int(max_products)), delay_sec=0)
                preview_rows.append({"입력URL": u, "유형": "카테고리", "수집예상상품수": len(product_urls), "예시상품URL": product_urls[:5]})
            except Exception as e:
                preview_rows.append({"입력URL": u, "유형": "카테고리", "수집예상상품수": 0, "예시상품URL": [f"오류: {e}"]})
        else:
            preview_rows.append({"입력URL": u, "유형": "상품", "수집예상상품수": 1, "예시상품URL": [normalize_product_url(u)]})
    st.dataframe(pd.DataFrame(preview_rows), use_container_width=True)

if run_btn:
    if not input_urls:
        st.warning("URL을 1개 이상 입력해주세요.")
        st.stop()

    expanded_urls = []
    progress = st.progress(0.0)
    status = st.empty()

    for idx, u in enumerate(input_urls, start=1):
        try:
            if is_category_url(u):
                urls = collect_product_urls_from_category(u, max_products=int(max_products), delay_sec=delay_sec)
                if len(urls) <= 1:
                    status.warning(f"카테고리 URL에서 상품이 1개 이하만 잡혔습니다. 수집 로직을 강하게 보완한 버전이지만, 사이트 구조 변경 시 추가 보완이 필요할 수 있습니다. URL: {u}")
                expanded_urls.extend(urls)
            else:
                expanded_urls.append(normalize_product_url(u))
        except Exception as e:
            status.error(f"URL 확장 실패: {u} / {e}")
        progress.progress(idx / max(len(input_urls), 1))

    # 최종 중복 제거
    dedup = []
    seen = set()
    for u in expanded_urls:
        pno = extract_product_no(u) or u
        if pno in seen:
            continue
        seen.add(pno)
        dedup.append(u)
    expanded_urls = dedup

    st.info(f"총 {len(expanded_urls)}개 상품을 처리합니다.")

    rows = []
    progress = st.progress(0.0)
    for i, u in enumerate(expanded_urls, start=1):
        try:
            row = parse_product_page(u)
            if use_openai:
                row = refine_with_openai(row)
            rows.append(row)
        except Exception as e:
            rows.append({col: "" for col in DEFAULT_COLUMNS})
            rows[-1]["product_no"] = extract_product_no(u)
            rows[-1]["product_url"] = u
            rows[-1]["product_summary"] = f"수집 실패: {e}"
        progress.progress(i / max(len(expanded_urls), 1))

    df = pd.DataFrame(rows)
    for col in DEFAULT_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    df = df[DEFAULT_COLUMNS]

    st.success(f"완료: {len(df)}개 행 생성")
    st.dataframe(df, use_container_width=True, height=500)

    csv_bytes = rows_to_csv_bytes(rows)
    st.download_button(
        "CSV 다운로드",
        data=csv_bytes,
        file_name="misharp_miya_db.csv",
        mime="text/csv",
        use_container_width=True,
    )
