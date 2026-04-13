"""
식품안전나라 축산물/식품 생산실적 Playwright 스크래퍼
- 사이트에 직접 접속하여 검색 조건 입력 후 엑셀 자동 다운로드
- 공개 OpenAPI(I1420)보다 최신·완전한 데이터 수집 가능
"""

import asyncio
import logging
import re
import tempfile
from pathlib import Path
from typing import Optional

import pandas as pd
from .base_scraper import BaseScraper

logger = logging.getLogger(__name__)

# 식품안전나라 생산실적 조회 URL (직접 확인 후 수정 가능)
DEFAULT_URL = "https://www.foodsafetykorea.go.kr/portal/healthyfoodlife/productionPerformance.do"

# 엑셀 컬럼 → 한글명 매핑 (실제 다운로드 파일 기준 확인)
COLUMN_KR = {
    "PRDLST_REPORT_NO": "품목제조번호",
    "EVL_YR":           "보고년도",
    "LCNS_NO":          "인허가번호",
    "PRDLST_NM":        "품목명",
    "PRDCTN_QY":        "생산량(kg)",
    "BSSH_NM":          "업소명",
    "H_ITEM_NM":        "대분류품목명",
    "GUBUN":            "구분",
    "FYER_PRDCTN_ABRT_QY": "연간생산중단수량",
}

# 검색 폼 셀렉터 (여러 패턴 시도)
FORM_SELECTORS = {
    "year": [
        "select[name='evlYr']", "select[name='EVL_YR']", "select[name='prdctnYear']",
        "select[name='srchYear']", "#evlYr", "#prdctnYear", "select:has-text('보고년도')",
    ],
    "h_item_nm": [
        "select[name='hItemNm']", "select[name='H_ITEM_NM']", "select[name='srchHItemNm']",
        "input[name='hItemNm']", "input[name='H_ITEM_NM']",
        "#hItemNm", "select:has-text('대분류')",
    ],
    "prdlst_nm": [
        "input[name='prdlstNm']", "input[name='PRDLST_NM']", "input[name='srchPrdlstNm']",
        "input[name='productNm']", "#prdlstNm", "#srchPrdlstNm",
    ],
    "bssh_nm": [
        "input[name='bsshNm']", "input[name='BSSH_NM']", "input[name='srchBsshNm']",
        "#bsshNm", "#srchBsshNm",
    ],
    "search_btn": [
        "button:has-text('검색')", "a:has-text('검색')", "input[value='검색']",
        "button[onclick*='search']", "button[onclick*='Search']",
        ".btn-search", "#btnSearch",
    ],
    "excel_btn": [
        "button:has-text('엑셀')", "a:has-text('엑셀')", "button:has-text('Excel')",
        "button[onclick*='excel']", "button[onclick*='Excel']", "a[onclick*='excel']",
        ".btn-excel", "#btnExcel", "button:has-text('다운로드')",
    ],
}


class FoodSafetyKoreaScraper(BaseScraper):
    """식품안전나라 생산실적 Playwright 스크래퍼"""

    def __init__(self, page_url: str = DEFAULT_URL, headless: bool = True,
                 download_dir: Optional[str] = None):
        super().__init__(headless=headless)
        self.page_url = page_url
        self.download_dir = download_dir or tempfile.mkdtemp()

    async def start(self):
        """다운로드 폴더 지정 후 브라우저 시작"""
        from playwright.async_api import async_playwright
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(
            headless=self.headless,
            args=["--no-sandbox", "--disable-blink-features=AutomationControlled"],
        )
        self._context = await self._browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            locale="ko-KR",
            accept_downloads=True,
        )
        self.page = await self._context.new_page()
        await self.page.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )

    async def fetch_production_data(
        self,
        year: str = "",
        h_item_nm: str = "",     # 대분류품목명 (예: 돈까스류)
        prdlst_nm: str = "",     # 품목명 (예: 돈까스)
        bssh_nm: str = "",       # 업소명
        progress_callback=None,
    ) -> pd.DataFrame:
        """
        식품안전나라 생산실적 페이지에서 검색 후 엑셀 다운로드

        Returns:
            DataFrame (컬럼: 품목제조번호, 보고년도, 인허가번호, 품목명, 생산량(kg), 업소명, 대분류품목명, 구분, 연간생산중단수량)
        """

        def log(msg):
            logger.info(msg)
            if progress_callback:
                progress_callback(msg)

        try:
            log(f"식품안전나라 접속 중... ({self.page_url})")
            await self.page.goto(self.page_url, wait_until="domcontentloaded", timeout=30000)
            await self.page.wait_for_timeout(2000)

            await self.screenshot("debug_fsk_loaded.png")

            # ── 검색 폼 입력 ──
            log("검색 조건 입력 중...")
            await self._fill_search_form(year, h_item_nm, prdlst_nm, bssh_nm)
            await self.page.wait_for_timeout(1000)

            # ── 검색 버튼 클릭 ──
            log("검색 실행 중...")
            clicked = await self._click_by_selectors(FORM_SELECTORS["search_btn"])
            if not clicked:
                logger.warning("검색 버튼 클릭 실패 - Enter 시도")
                await self.page.keyboard.press("Enter")

            await self.page.wait_for_load_state("networkidle", timeout=20000)
            await self.page.wait_for_timeout(1500)
            await self.screenshot("debug_fsk_results.png")

            # ── 엑셀 다운로드 ──
            log("엑셀 다운로드 시도 중...")
            df = await self._try_excel_download()
            if df is not None and not df.empty:
                log(f"엑셀 다운로드 성공: {len(df)}건")
                return self._normalize_df(df, year, h_item_nm, prdlst_nm)

            # ── 폴백: 화면 테이블 직접 스크래핑 ──
            log("엑셀 실패 → 테이블 직접 수집 중...")
            df = await self._scrape_table_all_pages(progress_callback)
            if df is not None and not df.empty:
                log(f"테이블 수집 성공: {len(df)}건")
                return self._normalize_df(df, year, h_item_nm, prdlst_nm)

            logger.warning("데이터 수집 실패 - 스크린샷 확인: debug_fsk_results.png")
            return pd.DataFrame()

        except Exception as e:
            logger.error(f"식품안전나라 수집 오류: {e}")
            await self.screenshot("debug_fsk_error.png")
            return pd.DataFrame()

    # ──────────────────────────────────────────
    # 내부 메서드
    # ──────────────────────────────────────────

    async def _fill_search_form(self, year, h_item_nm, prdlst_nm, bssh_nm):
        """검색 폼 조건 입력"""
        # 보고년도 (select 또는 input)
        if year:
            for sel in FORM_SELECTORS["year"]:
                try:
                    el = self.page.locator(sel).first
                    if await el.count() > 0:
                        tag = await el.evaluate("el => el.tagName.toLowerCase()")
                        if tag == "select":
                            await el.select_option(label=year)
                        else:
                            await el.fill(year)
                        break
                except Exception:
                    continue

        # 대분류품목명
        if h_item_nm:
            for sel in FORM_SELECTORS["h_item_nm"]:
                try:
                    el = self.page.locator(sel).first
                    if await el.count() > 0:
                        tag = await el.evaluate("el => el.tagName.toLowerCase()")
                        if tag == "select":
                            await el.select_option(label=h_item_nm)
                        else:
                            await el.fill(h_item_nm)
                        break
                except Exception:
                    continue

        # 품목명
        if prdlst_nm:
            await self._fill_by_selectors(FORM_SELECTORS["prdlst_nm"], prdlst_nm)

        # 업소명
        if bssh_nm:
            await self._fill_by_selectors(FORM_SELECTORS["bssh_nm"], bssh_nm)

    async def _try_excel_download(self) -> Optional[pd.DataFrame]:
        """엑셀 다운로드 버튼 클릭 후 파일 읽기"""
        for sel in FORM_SELECTORS["excel_btn"]:
            try:
                el = self.page.locator(sel).first
                if await el.count() == 0:
                    continue
                async with self.page.expect_download(timeout=30000) as dl_info:
                    await el.click()
                download = await dl_info.value
                dest = Path(self.download_dir) / download.suggested_filename
                await download.save_as(str(dest))
                logger.info(f"엑셀 저장: {dest}")
                df = pd.read_excel(dest, engine="openpyxl")
                return df
            except Exception as e:
                logger.debug(f"엑셀 다운로드 실패 ({sel}): {e}")
                continue
        return None

    async def _scrape_table_all_pages(self, progress_callback=None) -> Optional[pd.DataFrame]:
        """페이지네이션을 따라가며 테이블 데이터 전체 수집"""
        all_rows = []
        page_num = 1

        while True:
            if progress_callback:
                progress_callback(f"테이블 수집 중... {page_num}페이지")

            rows = await self.extract_table_data("table", "tbody tr")
            if not rows:
                # thead 포함 전체 테이블도 시도
                rows = await self.extract_table_data("table", "tr")

            if not rows:
                break

            # 헤더행 제거 (모두 th인 행)
            for row in rows:
                if any(cell for cell in row):
                    all_rows.append(row)

            # 다음 페이지
            next_sels = [
                "a:has-text('다음')", ".paging .next", ".pagination .next",
                "a[onclick*='next']", "li.next a",
            ]
            went_next = False
            for sel in next_sels:
                try:
                    el = self.page.locator(sel).first
                    if await el.count() > 0:
                        cls = await el.get_attribute("class") or ""
                        if "disabled" in cls or "inactive" in cls:
                            break
                        await el.click()
                        await self.page.wait_for_load_state("networkidle", timeout=15000)
                        await self.page.wait_for_timeout(800)
                        went_next = True
                        page_num += 1
                        break
                except Exception:
                    continue

            if not went_next or page_num > 100:
                break

        if not all_rows:
            return None

        # 헤더 추출 시도
        try:
            headers = await self._get_table_headers()
        except Exception:
            headers = None

        if headers and len(headers) == len(all_rows[0]):
            df = pd.DataFrame(all_rows, columns=headers)
        else:
            df = pd.DataFrame(all_rows)

        return df

    async def _get_table_headers(self) -> list[str]:
        """테이블 헤더 추출"""
        headers = []
        try:
            ths = await self.page.locator("table thead th").all()
            if not ths:
                ths = await self.page.locator("table tr:first-child th").all()
            for th in ths:
                headers.append((await th.inner_text()).strip())
        except Exception:
            pass
        return headers

    def _normalize_df(self, df: pd.DataFrame, year: str,
                      h_item_nm: str, prdlst_nm: str) -> pd.DataFrame:
        """컬럼명 한글 변환 + 클라이언트 필터링"""
        # 영문 컬럼명 → 한글 변환
        df.rename(columns={c: COLUMN_KR.get(c, c) for c in df.columns}, inplace=True)

        # 필터링
        str_cols = df.select_dtypes(include="object").columns.tolist()

        def _kw_filter(df_in, kw):
            mask = pd.Series(False, index=df_in.index)
            for col in str_cols:
                if col in df_in.columns:
                    mask |= df_in[col].str.contains(kw, na=False, case=False)
            return df_in[mask]

        keyword = prdlst_nm or h_item_nm
        if keyword:
            df = _kw_filter(df, keyword)
        if year and "보고년도" in df.columns:
            df = df[df["보고년도"].astype(str).str.contains(year, na=False)]

        return df.reset_index(drop=True)
