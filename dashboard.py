# -*- coding: utf-8 -*-
"""
학교급식 시장규모 대시보드 서버
실행: py -X utf8 dashboard.py
접속: http://localhost:8765
"""
import sys, os, subprocess, json, glob, re, threading
from datetime import datetime

IS_CLOUD = bool(os.environ.get("RAILWAY_ENVIRONMENT") or os.environ.get("RAILWAY_PROJECT_ID"))

def ensure_flask():
    try:
        import flask
    except ImportError:
        print("[설치 중] flask...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "flask", "-q"])

ensure_flask()

from flask import Flask, jsonify, send_file, request

SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
LOG_DIR      = os.path.join(SCRIPT_DIR, "log")
OUTPUT_DIR   = os.path.join(SCRIPT_DIR, "output")
SKILL_PY     = os.path.join(SCRIPT_DIR, "학교급식규모.py")

# ── 분석 기준 연도 (매년 1월에 수동 업데이트) ────────────────
ANALYSIS_YEAR = 2025   # 항상 이 연도 vs 전년도(2024) 비교

VACATION    = {1, 7, 12}
MONTH_NAMES = ["1월","2월","3월","4월","5월","6월",
               "7월","8월","9월","10월","11월","12월"]

app = Flask(__name__)
running_tasks  = {}   # keyword → "running" | "done" | "error:..."
bluesis_tasks  = {}   # keyword → "running" | "done" | "error:..."


# ── 블루시스 자동 수집 ────────────────────────────────────
def _bluesis_needs_update(product_info: list) -> bool:
    """product_info에 블루시스 상세 데이터가 없으면 True"""
    if not product_info:
        return False
    return any(not p.get("bluesis_ingredients") for p in product_info)


def _start_bluesis_bg(keyword: str, year: int, product_info: list):
    """백그라운드로 블루시스 상세정보 수집 후 JSON 저장"""
    if bluesis_tasks.get(keyword) == "running":
        return
    import copy
    pi_copy = copy.deepcopy(product_info)   # 스레드 안전

    def _run():
        bluesis_tasks[keyword] = "running"
        try:
            from bluesis_collector import collect_and_save
            collect_and_save(keyword, year, pi_copy, LOG_DIR)
            bluesis_tasks[keyword] = "done"
            print(f"[bluesis_bg] {keyword} 수집 완료")
        except Exception as e:
            bluesis_tasks[keyword] = f"error:{e}"
            print(f"[bluesis_bg] {keyword} 오류: {e}")

    threading.Thread(target=_run, daemon=True).start()
    print(f"[bluesis_bg] {keyword} 백그라운드 수집 시작")


# ── 유틸 ──────────────────────────────────────────────────
def get_latest_log(keyword: str, year: int):
    pattern = os.path.join(LOG_DIR, f"{keyword}_학교급식_{year}_*.json")
    files   = sorted(glob.glob(pattern))
    return files[-1] if files else None


def read_product_from_excel(keyword: str, year: int) -> list:
    """Excel 경쟁사_제품정보 Sheet에서 제품 정보를 읽어 반환.
    레이블(B열)을 동적으로 읽어 구버전/신버전 Sheet 모두 지원."""
    try:
        import openpyxl, re as _re
    except ImportError:
        return []

    def _find_excel(yr):
        pat = os.path.join(OUTPUT_DIR, f"{keyword}_학교급식규모_{yr}_*.xlsx")
        return [f for f in sorted(glob.glob(pat)) if not os.path.basename(f).startswith("~$")]

    files = _find_excel(year) or _find_excel(year - 1)
    if not files:
        return []

    try:
        wb = openpyxl.load_workbook(files[-1], data_only=True)
        if "경쟁사_제품정보" not in wb.sheetnames:
            return []
        ws = wb["경쟁사_제품정보"]

        # B열 레이블 → 행번호 매핑 (구버전/신버전 모두 지원)
        row_map = {}
        for row in range(1, 15):
            lbl = str(ws.cell(row=row, column=2).value or "").strip()
            if "업체명"   in lbl: row_map["company"]     = row
            if "제품명"   in lbl: row_map["product"]     = row
            if "규격"    in lbl or "중량" in lbl: row_map["standard"] = row
            if "방학제외" in lbl: row_map["mavg"]        = row
            if "원재료"   in lbl: row_map["ingredients"] = row
            if "원산지"   in lbl: row_map["origin"]      = row  # 구버전
            if "블루시스" in lbl and "단가" in lbl: row_map["kprice"] = row

        r_co   = row_map.get("company",     4)
        r_prod = row_map.get("product",     5)
        r_std  = row_map.get("standard",    6)
        r_mavg = row_map.get("mavg",        7)
        r_ingr = row_map.get("ingredients", 8)
        r_orig = row_map.get("origin",      None)
        r_kp   = row_map.get("kprice",     10)

        result = []
        for col in range(3, 9):
            company = str(ws.cell(row=r_co, column=col).value or "").strip()
            if not company or company in ("항목", "FoodnBid"):
                break

            product     = str(ws.cell(row=r_prod, column=col).value or "").strip()
            standard    = str(ws.cell(row=r_std,  column=col).value or "확인 필요").strip()
            mavg_raw    = str(ws.cell(row=r_mavg, column=col).value or "0").strip()
            ingredients = str(ws.cell(row=r_ingr, column=col).value or "").strip()
            origin      = str(ws.cell(row=r_orig, column=col).value or "").strip() if r_orig else ""
            bluesis     = str(ws.cell(row=r_kp,   column=col).value or "직접 확인").strip()

            m = _re.search(r"([\d,]+)", mavg_raw)
            monthly_avg = int(m.group(1).replace(",", "")) if m else 0

            # 구버전 placeholder 텍스트는 빈값으로 처리
            if any(kw in ingredients for kw in ("직접 기재", "직접 확인", "주원료(")):
                ingredients = ""

            result.append({
                "rank":                  col - 2,
                "company":               company,
                "product":               product,
                "annual":                0,
                "monthly_avg":           monthly_avg,
                "bluesis_standard":      standard,
                "bluesis_ingredients":   ingredients,
                "bluesis_kprice":        bluesis,
                "origin":                origin,
            })
        wb.close()
        return result
    except Exception as e:
        print(f"[경고] Excel 제품정보 읽기 실패: {e}")
        return []


def find_best_year(keyword: str) -> int:
    """ANALYSIS_YEAR 기준 연도 로그가 있으면 반환, 없으면 ANALYSIS_YEAR 고정."""
    for yr in [ANALYSIS_YEAR, ANALYSIS_YEAR - 1]:
        if get_latest_log(keyword, yr):
            return ANALYSIS_YEAR if get_latest_log(keyword, ANALYSIS_YEAR) else yr
    return ANALYSIS_YEAR


def compute_data(keyword: str):
    """키워드만으로 현재연도+전년도 데이터를 자동 결합해 반환."""
    year = find_best_year(keyword)
    curr_path = get_latest_log(keyword, year)
    if not curr_path:
        return None

    with open(curr_path, encoding="utf-8") as f:
        curr = json.load(f)

    prev_path = get_latest_log(keyword, year - 1)
    prev_raw  = None
    if prev_path:
        with open(prev_path, encoding="utf-8") as f:
            prev_raw = json.load(f)

    results     = curr["results"]
    annual      = curr["annual_total"]
    prev_annual = prev_raw["annual_total"] if prev_raw else 0

    # 시장규모 환산
    COV, SU, DAN = 0.60, 0.405, 0.225
    def market(a):
        if not a:
            return {"foodnbid": 0, "school_suwon": 0, "school_nation": 0, "total": 0}
        sw = int(a / COV)
        sn = int(sw / SU)
        return {"foodnbid": a, "school_suwon": sw, "school_nation": sn, "total": int(sn * (1 + DAN))}

    curr_mkt = market(annual)
    prev_mkt = market(prev_annual)

    # 전년 월별 맵
    prev_map = {}
    if prev_raw:
        for r in prev_raw["results"]:
            prev_map[r["month"]] = r["total"]

    # 월별 데이터
    best_month = max(results, key=lambda x: x["total"])["month"]
    monthly = []
    for r in results:
        prev_m = prev_map.get(r["month"], 0)
        yoy    = round((r["total"] - prev_m) / prev_m * 100, 1) if prev_m else None
        monthly.append({
            "month":       r["month"],
            "label":       MONTH_NAMES[r["month"] - 1],
            "current":     r["total"],
            "prev":        prev_m,
            "yoy":         yoy,
            "is_vacation": r["month"] in VACATION,
            "is_best":     r["month"] == best_month,
            "top3":        r.get("companies", [])[:3],
        })

    # 경쟁사 데이터
    comp_annual  = {}
    comp_monthly = {}
    for r in results:
        for co in r.get("companies", []):
            nm, amt = co["company"], co["amount"]
            comp_annual[nm]  = comp_annual.get(nm, 0) + amt
            comp_monthly.setdefault(nm, {})[r["month"]] = amt

    sorted_comps = sorted(comp_annual.items(), key=lambda x: -x[1])

    def excl_vac_avg(company):
        months = [comp_monthly.get(company, {}).get(m, 0)
                  for m in range(1, 13)
                  if m not in VACATION and comp_monthly.get(company, {}).get(m, 0) > 0]
        return int(sum(months) / len(months)) if months else 0

    comp_list = [{
        "company":              c,
        "annual":               a,
        "share":                round(a / annual * 100, 1) if annual else 0,
        "monthly_avg_excl_vac": excl_vac_avg(c),
        "monthly":              {str(m): comp_monthly.get(c, {}).get(m, 0) for m in range(1, 13)},
    } for c, a in sorted_comps[:15]]

    # 제품 정보: JSON 우선, 없으면 Excel Sheet4 fallback
    product_info = []
    for pf in sorted(glob.glob(os.path.join(LOG_DIR, f"{keyword}_제품정보_{year}_*.json"))):
        with open(pf, encoding="utf-8") as f:
            product_info = json.load(f)

    if not product_info:
        product_info = read_product_from_excel(keyword, year)

    # annual/bluesis 필드 보완
    if product_info:
        comp_by_name   = {c["company"]: c for c in comp_list}
        excel_info_map = {p["company"]: p for p in read_product_from_excel(keyword, year)}
        for p in product_info:
            co = comp_by_name.get(p["company"], {})
            if co:
                if not p.get("annual"):
                    p["annual"] = co["annual"]
                if not p.get("monthly_avg"):
                    p["monthly_avg"] = co["monthly_avg_excl_vac"]
            ep = excel_info_map.get(p["company"], {})
            # 구버전 JSON → Excel에서 새 필드 보완
            for fld in ("bluesis_kprice", "bluesis_standard", "bluesis_ingredients",
                        "bluesis_image_b64"):
                if not p.get(fld):
                    p[fld] = ep.get(fld, "")
            # 하위 호환: 예전 bluesis_price 필드
            if not p.get("bluesis_price"):
                p["bluesis_price"] = p.get("bluesis_kprice", ep.get("bluesis_price", ""))

    return {
        "keyword":       keyword,
        "year":          year,
        "prev_year":     year - 1,
        "annual":        annual,
        "prev_annual":   prev_annual,
        "best_month":    best_month,
        "collected_at":  curr.get("collected_at", ""),
        "market_curr":   curr_mkt,
        "market_prev":   prev_mkt,
        "monthly":       monthly,
        "competitors":   comp_list,
        "product_info":  product_info,
        "has_prev":      prev_raw is not None,
    }


# ── API ───────────────────────────────────────────────────
@app.route("/")
def index():
    return send_file(os.path.join(SCRIPT_DIR, "dashboard.html"))


@app.route("/api/search")
def api_search():
    keyword = request.args.get("keyword", "").strip()
    if not keyword:
        return jsonify({"error": "keyword_required"}), 400
    data = compute_data(keyword)
    if data is None:
        return jsonify({"error": "not_found",
                        "message": f'"{keyword}" 데이터가 없습니다. 수집을 시작해 주세요.'}), 404

    # 블루시스 상세정보가 없으면 백그라운드 수집 자동 시작
    if _bluesis_needs_update(data.get("product_info", [])):
        _start_bluesis_bg(keyword, data["year"], data["product_info"])
        data["bluesis_collecting"] = True
    else:
        data["bluesis_collecting"] = bluesis_tasks.get(keyword) == "running"

    return jsonify(data)


@app.route("/api/bluesis_status")
def api_bluesis_status():
    """블루시스 백그라운드 수집 상태 확인"""
    keyword = request.args.get("keyword", "").strip()
    status  = bluesis_tasks.get(keyword, "idle")
    # done이면 최신 데이터 반환
    if status == "done":
        data = compute_data(keyword)
        return jsonify({"status": "done", "data": data})
    return jsonify({"status": status})


@app.route("/api/cache")
def api_cache():
    """캐시된 키워드 목록 — ANALYSIS_YEAR(2025) 또는 전년도(2024) 파일만 표시"""
    valid_years = {ANALYSIS_YEAR, ANALYSIS_YEAR - 1}
    files = glob.glob(os.path.join(LOG_DIR, "*_학교급식_*_*.json"))
    seen, result = set(), []
    # ANALYSIS_YEAR 파일 우선 (역순 정렬 → 2025 > 2024 순서)
    for f in sorted(files, reverse=True):
        m = re.match(r"(.+)_학교급식_(\d{4})_", os.path.basename(f))
        if m:
            kw, yr = m.group(1), int(m.group(2))
            if yr not in valid_years:
                continue
            if kw not in seen:
                seen.add(kw)
                # 항상 ANALYSIS_YEAR 기준으로 표시
                result.append({"keyword": kw, "year": ANALYSIS_YEAR})
    return jsonify(result)


@app.route("/api/run")
def api_run():
    keyword = request.args.get("keyword", "").strip()
    if not keyword:
        return jsonify({"error": "keyword_required"}), 400

    if running_tasks.get(keyword) == "running":
        return jsonify({"status": "already_running"})

    # 수집 연도: 항상 ANALYSIS_YEAR(2025) 기준 — 스킬이 2025+2024 자동 수집
    year_str = str(ANALYSIS_YEAR)

    def _run():
        running_tasks[keyword] = "running"
        try:
            subprocess.run(
                [sys.executable, "-X", "utf8", SKILL_PY, keyword, year_str],
                cwd=SCRIPT_DIR
            )
            running_tasks[keyword] = "done"
        except Exception as e:
            running_tasks[keyword] = f"error:{e}"

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"status": "started", "keyword": keyword, "year": year_str})


@app.route("/api/excel")
def api_excel():
    """ANALYSIS_YEAR(2025) 엑셀 파일 우선 다운로드, 없으면 최신 파일"""
    keyword = request.args.get("keyword", "").strip()
    if not keyword:
        return jsonify({"error": "keyword_required"}), 400

    def _files(pattern):
        return [f for f in sorted(glob.glob(pattern))
                if not os.path.basename(f).startswith("~$")]

    # 1순위: ANALYSIS_YEAR(2025) 파일
    preferred = _files(os.path.join(OUTPUT_DIR,
                       f"{keyword}_학교급식규모_{ANALYSIS_YEAR}_*.xlsx"))
    # 2순위: 전년도(2024) 파일
    fallback1 = _files(os.path.join(OUTPUT_DIR,
                       f"{keyword}_학교급식규모_{ANALYSIS_YEAR-1}_*.xlsx"))
    # 3순위: 연도 무관 최신 파일
    fallback2 = _files(os.path.join(OUTPUT_DIR,
                       f"{keyword}_학교급식규모_*.xlsx"))

    files = preferred or fallback1 or fallback2
    if not files:
        return jsonify({"error": "no_excel",
                        "message": "엑셀 파일이 없습니다. 스킬을 먼저 실행해 주세요."}), 404

    latest = files[-1]
    return send_file(
        latest,
        as_attachment=True,
        download_name=os.path.basename(latest),
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


@app.route("/api/status")
def api_status():
    keyword  = request.args.get("keyword", "").strip()
    status   = running_tasks.get(keyword, "idle")
    has_data = find_best_year(keyword) and get_latest_log(keyword, find_best_year(keyword)) is not None
    return jsonify({"status": status, "has_data": bool(has_data)})


# ── 실행 ──────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8765))
    is_local = not IS_CLOUD
    if is_local:
        import webbrowser
        threading.Timer(1.5, lambda: webbrowser.open(f"http://localhost:{port}")).start()
    url = f"http://localhost:{port}"
    print("=" * 55)
    print("  학교급식 시장규모 대시보드")
    print(f"  접속 주소: {url}")
    print("  종료: Ctrl+C")
    print("=" * 55)
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
