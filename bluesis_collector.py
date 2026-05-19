# -*- coding: utf-8 -*-
"""
블루시스 제품 상세정보 수집 모듈
- kg단가 / 규격(중량) / 원재료명 TOP5 / 이미지(base64)
- dashboard.py에서 백그라운드 스레드로 호출
"""
import os, sys, subprocess, json, glob, asyncio
from datetime import datetime

BLUESIS_ID  = os.environ.get("BLUESIS_ID",  "씨제이프레시웨이서울")
BLUESIS_PW  = os.environ.get("BLUESIS_PW",  "1234")

# ── Playwright 자동 설치 ──────────────────────────────────
def ensure_playwright():
    try:
        import playwright
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "playwright", "-q"])
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            _ = p.chromium.executable_path
    except Exception:
        subprocess.check_call(
            [sys.executable, "-m", "playwright", "install", "chromium", "--with-deps"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )


# ── JS: 리스팅 페이지 전체 컬럼 추출 ─────────────────────
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


def parse_ingredients(desc: str, n: int = 5) -> str:
    """식품설명 → 상위 n개 원재료 (괄호 인식 comma 분리)"""
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


def parse_standard(std: str) -> str:
    """규격 문자열 → 총 중량"""
    import re
    if not std:
        return "확인 필요"
    m = re.search(r"(\d+(?:\.\d+)?\s*(?:kg|g|ml|L|KG|G|ML))", std, re.IGNORECASE)
    return m.group(0).strip() if m else (std.split()[0] if std else "확인 필요")


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


async def _collect(keyword: str, company_names: list) -> dict:
    import urllib.parse
    from playwright.async_api import async_playwright

    LOGIN_URL  = "https://market.bluesis.com/web/pc/login.php"
    SEARCH_URL = (f"https://market.bluesis.com/web/pc/product.php"
                  f"?from=main&_qr={urllib.parse.quote(keyword)}")

    empty  = {"kprice": "블루시스 미등록", "standard": "", "ingredients": "", "image_b64": ""}
    result = {name: dict(empty) for name in company_names}

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

            await page.goto(SEARCH_URL, wait_until="domcontentloaded", timeout=15000)
            await page.wait_for_timeout(2000)
            try:
                await page.select_option("#rows", "100")
                await page.wait_for_timeout(3000)
            except Exception:
                pass

            items = await page.evaluate(_JS_ROWS)

            for company in company_names:
                matches = [it for it in items
                           if company in it["brand"] or company in it["com"]]
                if not matches:
                    continue

                def score(it):
                    return (it["kprice"] not in ("", "싯가"),
                            bool(it.get("description")),
                            bool(it.get("imgSrc")) and "noimage" not in it.get("imgSrc", ""))

                best = max(matches, key=score)
                result[company] = {
                    "kprice":      best["kprice"] if best["kprice"] not in ("", "싯가") else "싯가",
                    "standard":    parse_standard(best.get("standard", "")),
                    "ingredients": parse_ingredients(best.get("description", "")),
                    "image_b64":   await _fetch_img_b64(page, best.get("imgSrc", "")),
                }
        except Exception as e:
            print(f"[bluesis_collector] 오류: {e}")
        finally:
            await browser.close()
    return result


def collect_and_save(keyword: str, year: int, product_info: list, log_dir: str) -> str:
    """
    블루시스 데이터를 수집해 product_info에 병합 후 JSON 저장.
    Returns: 저장된 파일 경로
    """
    ensure_playwright()
    company_names = [p["company"] for p in product_info]

    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        details = loop.run_until_complete(_collect(keyword, company_names))
    finally:
        loop.close()

    for p in product_info:
        d = details.get(p["company"], {})
        p["bluesis_kprice"]      = d.get("kprice",      "블루시스 미등록")
        p["bluesis_standard"]    = d.get("standard",    "")
        p["bluesis_ingredients"] = d.get("ingredients", "")
        p["bluesis_image_b64"]   = d.get("image_b64",   "")

    date_str = datetime.now().strftime("%Y%m%d_%H%M")
    fpath    = os.path.join(log_dir, f"{keyword}_제품정보_{year}_{date_str}.json")
    with open(fpath, "w", encoding="utf-8") as f:
        json.dump(product_info, f, ensure_ascii=False, indent=2)
    print(f"[bluesis_collector] 저장: {os.path.basename(fpath)}")
    return fpath
