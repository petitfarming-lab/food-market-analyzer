# -*- coding: utf-8 -*-
"""
블루시스 제품 상세정보 수집 모듈 (로컬 전용 — Railway에서는 실행 안 됨)
실행 방법:
  python bluesis_collector.py 새우만두 2025   ← 직접 실행
  dashboard.py 에서 subprocess로 호출          ← 간접 실행
"""
import os, sys, subprocess, json, glob, asyncio
from datetime import datetime

BLUESIS_ID = os.environ.get("BLUESIS_ID", "씨제이프레시웨이서울")
BLUESIS_PW = os.environ.get("BLUESIS_PW", "1234")

_JS_ROWS = """
() => {
    var brandEls = Array.from(document.querySelectorAll(".w_brand"));
    var res = [];
    for (var i = 0; i < brandEls.length; i++) {
        var row = brandEls[i].parentElement;
        if (!row) continue;
        var g = function(cls) {
            var el = row.querySelector("." + cls);
            if (!el) return "";
            var cl = el.cloneNode(true);
            Array.from(cl.querySelectorAll("button")).forEach(function(b){ b.remove(); });
            return cl.innerText.trim().replace(/\\s+/g, " ");
        };
        var b = brandEls[i].innerText.split("\\n")[0].replace("가입","").trim();
        var c = g("w_com").split("\\n")[0].replace("가입","").trim();
        var kpRaw = g("w_kprice").replace("학교 kg단가","").trim();
        var km = kpRaw.match(/([0-9]{1,3}(?:,[0-9]{3})+)/);
        var kprice = km ? km[1] + "원/kg" : "싯가";
        var pnEl = row.querySelector(".w_pname");
        var pn = "";
        if (pnEl) {
            var cl2 = pnEl.cloneNode(true);
            Array.from(cl2.querySelectorAll("button")).forEach(function(b){ b.remove(); });
            pn = cl2.innerText.trim().split("\\n")[0].slice(0, 80);
        }
        var std  = g("w_standard").split("\\n")[0].trim();
        var desc = g("w_description").replace(/\\s+/g, " ").trim().slice(0, 500);
        var imgEl = row.querySelector(".w_image img");
        var imgSrc = imgEl ? (imgEl.getAttribute("src") || "") : "";
        if (imgSrc && !imgSrc.startsWith("http")) imgSrc = location.origin + imgSrc;
        res.push({brand: b, com: c, pname: pn, kprice: kprice,
                  standard: std, description: desc, imgSrc: imgSrc});
    }
    return res;
}
"""


def _parse_ingredients(desc: str, n: int = 5) -> str:
    if not desc:
        return ""
    parts, depth, cur = [], 0, ""
    for ch in desc:
        if ch in ("(", "（"):   depth += 1; cur += ch
        elif ch in (")", "）"): depth -= 1; cur += ch
        elif ch == "," and depth == 0:
            parts.append(cur.strip()); cur = ""
        else:
            cur += ch
    if cur.strip():
        parts.append(cur.strip())
    import re
    cleaned = [p for p in parts if len(p) >= 2 and not re.fullmatch(r"[\d%.\s/]+", p)]
    return ", ".join(cleaned[:n])


def _parse_standard(std: str) -> str:
    import re
    if not std:
        return "확인 필요"
    m = re.search(r"(\d+(?:\.\d+)?\s*(?:kg|g|ml|L|KG|G|ML))", std, re.IGNORECASE)
    return m.group(0).strip() if m else (std.split()[0] if std else "확인 필요")


def _norm(s: str) -> str:
    """공백·괄호 등을 제거해 블루시스 품명(pname) 비교용으로 정규화."""
    import re
    return re.sub(r"[\s\(\)\[\]（）【】·,]+", "", s or "")


def _best_match(items, target_name):
    """target_name(정규화 제품명)과 품명(pname)이 가장 가깝게 일치하는 item을 반환."""
    target = _norm(target_name)
    if not target:
        return None
    best, best_score = None, None
    for it in items:
        pn = _norm(it.get("pname", ""))
        if not pn:
            continue
        if target in pn:
            score = (2, -(len(pn) - len(target)))
        elif pn in target:
            score = (1, -(len(target) - len(pn)))
        else:
            continue
        if best_score is None or score > best_score:
            best_score, best = score, it
    return best


async def _fetch_img_b64(page, url: str) -> str:
    if not url or "noimage" in url:
        return ""
    try:
        result = await page.evaluate("""async (url) => {
            try {
                const r = await fetch(url, {credentials:'include'});
                if (!r.ok) return '';
                const ab = await r.arrayBuffer();
                const bytes = new Uint8Array(ab);
                let bin = '';
                for (let i=0; i<bytes.byteLength; i++) bin += String.fromCharCode(bytes[i]);
                const ct = r.headers.get('content-type') || 'image/jpeg';
                return 'data:' + ct + ';base64,' + btoa(bin);
            } catch(e) { return ''; }
        }""", url)
        return result or ""
    except Exception:
        return ""


async def _run_async(keyword: str, product_info: list) -> dict:
    import urllib.parse
    from playwright.async_api import async_playwright

    LOGIN_URL = "https://market.bluesis.com/web/pc/login.php"

    def _search_url(q: str) -> str:
        return (f"https://market.bluesis.com/web/pc/product.php"
                f"?from=main&_qr={urllib.parse.quote(q)}")

    empty  = {"kprice": "블루시스 미등록", "standard": "", "ingredients": "", "image_b64": ""}
    result = {p["rank"]: dict(empty) for p in product_info}

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        page    = await browser.new_page()
        try:
            await page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(1500)
            await page.fill("#blue_uid", BLUESIS_ID)
            await page.fill("#pwd",      BLUESIS_PW)
            await page.click("input[value='로그인하기']")
            await page.wait_for_timeout(3000)

            async def _search(q):
                await page.goto(_search_url(q), wait_until="domcontentloaded", timeout=15000)
                await page.wait_for_timeout(2000)
                try:
                    await page.select_option("#rows", "100")
                except Exception:
                    pass
                items = []
                for _ in range(3):
                    await page.wait_for_timeout(3000)
                    items = await page.evaluate(_JS_ROWS)
                    if len(items) >= 50:
                        break
                return items

            # ── 전체 키워드 검색 (1차 후보 풀) ──
            broad_items = await _search(keyword)

            # ── 제품별 매칭 ──
            for p in product_info:
                company = p["company"]
                pname_q = p.get("product", "")

                best = _best_match(broad_items, pname_q)

                if not best and pname_q:
                    items = await _search(_norm(pname_q))
                    best = _best_match(items, pname_q)

                    if not best:
                        words = pname_q.split()
                        if len(words) > 1:
                            items2 = await _search(words[-1])
                            best = _best_match(items2, pname_q)

                if not best:
                    matches = [it for it in broad_items
                               if company in it["brand"] or company in it["com"]]
                    if matches:
                        def score(it):
                            return (it["kprice"] not in ("", "싯가"),
                                    bool(it.get("description")),
                                    bool(it.get("imgSrc")) and "noimage" not in it.get("imgSrc", ""))
                        best = max(matches, key=score)

                if not best:
                    continue

                result[p["rank"]] = {
                    "kprice":      best["kprice"] if best["kprice"] not in ("", "싯가") else "싯가",
                    "standard":    _parse_standard(best.get("standard", "")),
                    "ingredients": _parse_ingredients(best.get("description", "")),
                    "image_b64":   await _fetch_img_b64(page, best.get("imgSrc", "")),
                }
                print(f"  {p['rank']}위 {company}/{pname_q}: {result[p['rank']]['kprice']}  규격={result[p['rank']]['standard']}")
        except Exception as e:
            print(f"[bluesis_collector] 오류: {e}")
        finally:
            await browser.close()
    return result


def run(keyword: str, year: int, log_dir: str) -> int:
    """
    기존 제품정보 JSON에 블루시스 데이터를 병합해서 새 JSON 저장.
    Returns: 0=성공, 1=실패
    """
    files = sorted(glob.glob(os.path.join(log_dir, f"{keyword}_제품정보_{year}_*.json")))
    if not files:
        print(f"[bluesis_collector] {keyword} 제품정보 JSON 없음")
        return 1

    with open(files[-1], encoding="utf-8") as f:
        product_info = json.load(f)

    print(f"[bluesis_collector] {keyword} {len(product_info)}개 제품 수집 시작...")

    details = asyncio.run(_run_async(keyword, product_info))

    for p in product_info:
        d = details.get(p["rank"], {})
        if d.get("kprice", "블루시스 미등록") == "블루시스 미등록" and not d.get("ingredients"):
            continue  # 이번 수집에서 매칭 실패 → 기존 값 유지 (재수집이 기존 데이터를 덮어쓰지 않도록)
        p["bluesis_kprice"]      = d.get("kprice",      "블루시스 미등록")
        p["bluesis_standard"]    = d.get("standard",    "")
        p["bluesis_ingredients"] = d.get("ingredients", "")
        p["bluesis_image_b64"]   = d.get("image_b64",   "")

    date_str = datetime.now().strftime("%Y%m%d_%H%M")
    out = os.path.join(log_dir, f"{keyword}_제품정보_{year}_{date_str}.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(product_info, f, ensure_ascii=False, indent=2)
    print(f"[bluesis_collector] 저장: {os.path.basename(out)}")
    return 0


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("사용법: python bluesis_collector.py <키워드> <연도> <log_dir>")
        sys.exit(1)
    kw      = sys.argv[1]
    yr      = int(sys.argv[2])
    log_dir = sys.argv[3] if len(sys.argv) > 3 else os.path.join(os.path.dirname(__file__), "log")
    sys.exit(run(kw, yr, log_dir))
