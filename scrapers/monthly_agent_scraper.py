"""
FOODnBID 월별예상금액 자동수집기
https://info.foodnbid.com/agent/sellingAgent01.do

검색 필터 설정 → 검색 → 상위품목확인 업체별 클릭 → Excel 다운로드 자동화
"""

import asyncio
import logging
import io
from pathlib import Path
from typing import Optional
import pandas as pd
from playwright.async_api import async_playwright

logger = logging.getLogger(__name__)

MONTHLY_URL = "https://info.foodnbid.com/agent/sellingAgent01.do"
LOGIN_URL_CANDIDATES = [
    "https://info.foodnbid.com/member/memberLogin.do",
    "https://info.foodnbid.com/login.do",
    "https://foodnbid.com",
]


class MonthlyAgentScraper:
    """FOODnBID 월별예상금액 자동수집기"""

    def __init__(self, user_id: str, password: str, headless: bool = True):
        self.user_id = user_id
        self.password = password
        self.headless = headless
        self._playwright = None
        self._browser = None
        self._context = None
        self.page = None
        self.download_dir = Path("downloads_monthly")
        self.download_dir.mkdir(exist_ok=True)

    # ──────────────────────────────────────────────────────────
    # 브라우저 생명주기
    # ──────────────────────────────────────────────────────────

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, *args):
        await self.close()

    async def start(self):
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(
            headless=self.headless,
            args=["--no-sandbox", "--disable-blink-features=AutomationControlled"],
        )
        self._context = await self._browser.new_context(
            viewport={"width": 1400, "height": 900},
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

    async def close(self):
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()

    # ──────────────────────────────────────────────────────────
    # 로그인
    # ──────────────────────────────────────────────────────────

    async def login(self) -> bool:
        """
        info.foodnbid.com 로그인.
        main.do 로 리다이렉트되면 팝업 로그인 버튼 클릭 후 ID/PW 입력.
        """
        try:
            await self.page.goto(MONTHLY_URL, wait_until="domcontentloaded", timeout=30000)
            await self.page.wait_for_timeout(2000)

            page_text = await self.page.inner_text("body")

            # 이미 로그인된 상태
            if "로그아웃" in page_text or "월별예상금액" in page_text:
                logger.info("이미 로그인 상태")
                return True

            logger.info(f"로그인 필요 — 현재 URL: {self.page.url}")
            await self.page.screenshot(path="debug_monthly_before_login.png", full_page=True)

            # ── Step 1: 팝업 로그인 버튼 클릭 (foodnbid.com 패턴) ──
            popup_btn_sels = [
                "a[onclick*='fn_usrLogin']",
                "a[onclick*='login']",
                "a[onclick*='Login']",
                ".btn-login",
                ".login-btn",
                "a:has-text('로그인')",
                "button:has-text('로그인')",
            ]
            for sel in popup_btn_sels:
                try:
                    el = self.page.locator(sel).first
                    if await el.count() > 0:
                        logger.info(f"팝업 로그인 버튼 클릭: {sel}")
                        await el.click()
                        await self.page.wait_for_timeout(1200)
                        break
                except Exception:
                    continue

            await self.page.screenshot(path="debug_monthly_popup.png", full_page=True)

            # ── 진단: 페이지의 모든 input/form/frame 출력 ──
            await self._log_page_inputs("main page after popup click")

            # ── Step 2: iframe 안에 로그인 폼이 있는지 먼저 확인 ──
            frames = self.page.frames
            login_frame = None
            for frame in frames:
                try:
                    frame_inputs = await frame.locator("input[type='password']").count()
                    if frame_inputs > 0:
                        login_frame = frame
                        logger.info(f"iframe에서 로그인 폼 발견: {frame.url}")
                        break
                except Exception:
                    continue

            id_sels = [
                "#cm_id", "input[name='cm_id']",
                "#loginId", "input[name='loginId']",
                "#userid", "input[name='userid']",
                "#id", "input[name='id']",
                "input[type='text']",
            ]
            pw_sels = [
                "#cm_pwd", "input[name='cm_pwd']",
                "#loginPwd", "input[name='loginPwd']",
                "#userpwd", "input[name='userpwd']",
                "#pwd", "input[name='pwd']",
                "input[type='password']",
            ]
            btn_sels = [
                "#loginBtn", ".loginBtn",
                "a[onclick*='fn_login']",
                "a[onclick*='fnLogin']",
                "button[onclick*='login']",
                "input[type='submit']",
                "button[type='submit']",
            ]

            # iframe 또는 메인 페이지에서 ID/PW 입력
            if login_frame:
                id_ok = await self._fill_in_frame(login_frame, id_sels, self.user_id)
                pw_ok = await self._fill_in_frame(login_frame, pw_sels, self.password)
                if id_ok and pw_ok:
                    await self._click_in_frame(login_frame, btn_sels)
                else:
                    logger.error("iframe 로그인 필드 입력 실패")
            else:
                id_ok = await self._fill_by_selectors(id_sels, self.user_id)
                pw_ok = await self._fill_by_selectors(pw_sels, self.password)
                if id_ok and pw_ok:
                    await self._click_by_selectors(btn_sels)
                else:
                    logger.error("로그인 필드를 찾지 못했습니다. debug_monthly_popup.png 확인")
                    return False

            await self._click_by_selectors(btn_sels)
            await self.page.wait_for_load_state("networkidle", timeout=15000)
            await self.page.wait_for_timeout(1500)

            page_text = await self.page.inner_text("body")
            if "로그아웃" in page_text or "마이페이지" in page_text:
                logger.info("로그인 성공")
            else:
                logger.warning("로그인 상태 불확실")
                await self.page.screenshot(path="debug_monthly_after_login.png", full_page=True)

            return True

        except Exception as e:
            logger.error(f"로그인 오류: {e}")
            await self.page.screenshot(path="debug_monthly_error.png", full_page=True)
            return False

    # ──────────────────────────────────────────────────────────
    # 메인 수집 엔트리포인트
    # ──────────────────────────────────────────────────────────

    async def collect(
        self,
        sido: str = "",
        military: str = "2",
        year: str = "",
        month: str = "",
        keyword_include: str = "",
        keyword_exclude: str = "",
        progress_callback=None,
    ) -> tuple[pd.DataFrame, list[dict]]:
        """
        월별예상금액 수집 메인 함수

        Args:
            sido:             시도 선택값 ("" = 전체)
            military:         군부대 옵션 ("" = 전체, "1" = 포함, "2" = 제외)
            year:             연도 문자열 (예: "2026")
            month:            월 문자열 (예: "3")
            keyword_include:  검색포함 키워드
            keyword_exclude:  검색제외 키워드
            progress_callback: 진행 상황 콜백 (str) → None

        Returns:
            (summary_df, company_results)
            company_results: [{"company": str, "df": DataFrame, "excel_bytes": bytes|None}]
        """

        def log(msg: str):
            logger.info(msg)
            if progress_callback:
                progress_callback(msg)

        # 1. 로그인
        log("로그인 중...")
        await self.login()

        # 2. 월별예상금액 페이지로 이동
        log("월별예상금액 페이지 접속 중...")
        await self.page.goto(MONTHLY_URL, wait_until="networkidle", timeout=30000)
        await self.page.wait_for_timeout(1500)

        # 3. 필터 설정
        log("검색 조건 설정 중...")
        await self._set_filters(sido, military, year, month, keyword_include, keyword_exclude)
        await self.page.wait_for_timeout(500)

        # 4. 검색
        log("검색 실행 중...")
        await self._click_search()
        await self.page.wait_for_load_state("networkidle", timeout=20000)
        await self.page.wait_for_timeout(2000)

        # 5. 메인 테이블 탐지 및 요약 추출
        log("목록 데이터 수집 중...")
        main_table_info = await self._find_main_table()
        if not main_table_info:
            log("❌ '상위품목확인' 테이블을 찾지 못했습니다.")
            return pd.DataFrame(), []

        summary_df = main_table_info["df"]
        company_names = main_table_info["company_names"]  # 업체명 컬럼 기준
        top_col_idx = main_table_info["top_col_idx"]      # 상위품목확인 컬럼 인덱스
        log(f"총 {len(company_names)}개 업체 발견")

        # 6. 업체별 상세 수집
        company_results = []
        for i, company_name in enumerate(company_names):
            log(f"[{i+1}/{len(company_names)}] {company_name} 상세 수집 중...")
            try:
                result = await self._collect_one_company(
                    company_name, i, top_col_idx
                )
                if result:
                    company_results.append(result)
                    log(f"  ✓ {company_name} 완료 (행 수: {len(result['df'])})")
                else:
                    log(f"  ✗ {company_name} 수집 실패")
            except Exception as e:
                logger.error(f"{company_name} 수집 오류: {e}")
                log(f"  ✗ {company_name} 오류: {e}")

        log(f"전체 완료: {len(company_results)}/{len(company_names)}개 업체")
        return summary_df, company_results

    # ──────────────────────────────────────────────────────────
    # 필터 & 검색
    # ──────────────────────────────────────────────────────────

    async def _set_filters(
        self, sido: str, military: str, year: str, month: str,
        keyword_include: str, keyword_exclude: str
    ):
        """검색 필터 폼 채우기"""
        # 시도 선택
        if sido is not None:
            await self._select_opt(
                ["#s_sido", "select[name='s_sido']", "select[name='sido']"],
                sido
            )

        # 군부대 선택
        if military:
            await self._select_opt(
                ["#s_mili", "select[name='s_mili']", "select[name='military']",
                 "select[name='s_military']"],
                military
            )

        # 연도 선택
        if year:
            await self._select_opt(
                ["#s_year", "select[name='s_year']", "select[name='year']"],
                year
            )

        # 월 선택 (01, 1 두 가지 형식 모두 시도)
        if month:
            month_padded = month.zfill(2)
            month_sels = ["#s_month", "select[name='s_month']", "select[name='month']"]
            success = await self._select_opt(month_sels, month_padded)
            if not success:
                await self._select_opt(month_sels, month)

        # 검색포함 입력
        if keyword_include:
            await self._fill_by_selectors(
                ["#s_search", "input[name='s_search']",
                 "input[name='searchNm']", "input[name='keyword']"],
                keyword_include
            )

        # 검색제외 입력
        if keyword_exclude:
            await self._fill_by_selectors(
                ["#s_exc_search", "input[name='s_exc_search']",
                 "input[name='excSearchNm']", "input[name='excKeyword']"],
                keyword_exclude
            )

    async def _click_search(self):
        """검색 버튼 클릭"""
        search_sels = [
            "input[type='button'][value='검색']",
            "input[type='submit'][value='검색']",
            "button:has-text('검색')",
            "a:has-text('검색')",
            "input[onclick*='earch']",
            "button[onclick*='earch']",
            "a[onclick*='earch']",
            ".btn-search",
            ".btnSearch",
            "#btnSearch",
        ]
        for sel in search_sels:
            try:
                el = self.page.locator(sel).first
                if await el.count() > 0:
                    logger.info(f"검색 버튼 클릭: {sel}")
                    await el.click()
                    return
            except Exception:
                continue

        # 마지막 수단: 페이지의 모든 버튼/입력 텍스트에서 '검색' 찾기
        try:
            els = await self.page.locator("input[type='button'], input[type='submit'], button, a").all()
            for el in els:
                txt = (await el.inner_text()).strip()
                val = await el.get_attribute("value") or ""
                if "검색" in txt or "검색" in val:
                    logger.info(f"검색 버튼 텍스트 탐색으로 발견: '{txt or val}'")
                    await el.click()
                    return
        except Exception:
            pass

        logger.warning("검색 버튼을 찾지 못했습니다 - 엔터키로 대체")
        await self.page.keyboard.press("Enter")

    # ──────────────────────────────────────────────────────────
    # 테이블 파싱
    # ──────────────────────────────────────────────────────────

    async def _find_main_table(self) -> Optional[dict]:
        """
        '상위품목확인' 헤더를 가진 테이블 탐색.
        th / td 모두 시도, 처음 3행 내에서 헤더 행 탐색.
        """
        try:
            # 검색 결과 테이블이 늦게 렌더링될 수 있으므로 추가 대기
            await self.page.wait_for_timeout(2000)

            tables = await self.page.locator("table").all()
            logger.info(f"페이지 내 테이블 수: {len(tables)}")

            for t_idx, table in enumerate(tables):
                all_rows = await table.locator("tr").all()
                headers = []
                header_row_idx = -1

                # 처음 3행에서 헤더 탐색 (th 또는 td 모두 허용)
                for r_idx, row in enumerate(all_rows[:3]):
                    cells = await row.locator("th, td").all()
                    texts = [c.strip() for c in [await cell.inner_text() for cell in cells]]
                    if "상위품목확인" in texts:
                        headers = texts
                        header_row_idx = r_idx
                        logger.info(f"테이블[{t_idx}] 헤더 발견(행{r_idx}): {headers}")
                        break

                if not headers:
                    continue

                top_col_idx = headers.index("상위품목확인")
                company_col_idx = headers.index("업체명") if "업체명" in headers else 1
                logger.info(f"  업체명 컬럼: {company_col_idx}, 상위품목확인 컬럼: {top_col_idx}")

                # 데이터 행 = 헤더 행 이후 td만 있는 행
                company_names = []
                table_data = []

                for row in all_rows[header_row_idx + 1:]:
                    tds = await row.locator("td").all()
                    if len(tds) < 2:
                        continue
                    row_texts = [r.strip() for r in [await td.inner_text() for td in tds]]
                    first = row_texts[0] if row_texts else ""
                    if first in ("NO", "합계", "총금액", ""):
                        continue
                    table_data.append(row_texts)
                    if company_col_idx < len(tds):
                        name = (await tds[company_col_idx].inner_text()).strip()
                        if name:
                            company_names.append(name)

                cols = headers if (table_data and len(headers) == len(table_data[0])) else None
                df = pd.DataFrame(table_data, columns=cols)

                logger.info(f"수집된 업체 수: {len(company_names)}")
                return {
                    "df": df,
                    "company_names": company_names,
                    "top_col_idx": top_col_idx,
                }

            logger.warning("'상위품목확인' 헤더를 가진 테이블 없음")
            return None
        except Exception as e:
            logger.error(f"메인 테이블 탐색 오류: {e}")
            return None

    # ──────────────────────────────────────────────────────────
    # 업체별 상세 수집
    # ──────────────────────────────────────────────────────────

    async def _collect_one_company(
        self, company_name: str, idx: int, top_col_idx: int
    ) -> Optional[dict]:
        """
        특정 업체의 상위품목 데이터 수집.
        메인 테이블에서 idx번째 데이터 행의 top_col_idx 컬럼 클릭.
        """
        try:
            # 메인 테이블 재탐색 (팝업/뒤로가기 후 DOM이 갱신될 수 있음)
            tables = await self.page.locator("table").all()
            target_table = None
            for table in tables:
                header_cells = await table.locator("th").all()
                headers = [h.strip() for h in [await th.inner_text() for th in header_cells]]
                if "상위품목확인" in headers:
                    target_table = table
                    break

            if not target_table:
                logger.error(f"{company_name}: 메인 테이블을 찾지 못했습니다")
                return None

            # 데이터 행(th가 없는 tr) 에서 idx번째
            data_rows = await target_table.locator("tbody tr").all()
            if not data_rows:
                all_rows = await target_table.locator("tr").all()
                data_rows = [r for r in all_rows[1:]]

            if idx >= len(data_rows):
                logger.warning(f"{company_name}: 행 인덱스 초과 idx={idx} / {len(data_rows)}")
                return None

            row = data_rows[idx]
            tds = await row.locator("td").all()
            if top_col_idx >= len(tds):
                logger.warning(f"{company_name}: 컬럼 인덱스 초과 {top_col_idx} / {len(tds)}")
                return None

            target_td = tds[top_col_idx]

            # td 안의 클릭 가능한 요소 탐색 (a 태그, onclick 있는 요소)
            clickable = None
            for sel in ["a", "span[onclick]", "span", "button"]:
                el = target_td.locator(sel).first
                if await el.count() > 0:
                    clickable = el
                    break

            if not clickable:
                # td 자체에 onclick이 있는 경우
                onclick = await target_td.get_attribute("onclick") or ""
                if onclick:
                    clickable = target_td
                else:
                    logger.error(f"{company_name}: 클릭 가능한 요소 없음")
                    return None

            # ── 팝업 여부 감지 ──
            popup_pages: list = []

            def _on_page(p):
                popup_pages.append(p)

            self._context.on("page", _on_page)
            await clickable.click()
            await self.page.wait_for_timeout(2500)
            self._context.remove_listener("page", _on_page)

            if popup_pages:
                popup = popup_pages[0]
                await popup.wait_for_load_state("networkidle", timeout=15000)
                await popup.wait_for_timeout(1000)
                df = await self._extract_table_from_page(popup)
                excel_bytes = await self._download_excel_from_page(popup, company_name)
                await popup.close()
            else:
                await self.page.wait_for_load_state("networkidle", timeout=10000)
                df = await self._extract_table_from_page(self.page)
                excel_bytes = await self._download_excel_from_page(self.page, company_name)
                await self.page.go_back()
                await self.page.wait_for_load_state("networkidle", timeout=10000)
                await self.page.wait_for_timeout(1000)

            return {"company": company_name, "df": df, "excel_bytes": excel_bytes}

        except Exception as e:
            logger.error(f"{company_name} 상세 수집 오류: {e}")
            return None

    async def _extract_table_from_page(self, page) -> pd.DataFrame:
        """주어진 페이지(또는 팝업)에서 가장 큰 테이블 추출"""
        try:
            tables = await page.locator("table").all()
            best_df = pd.DataFrame()

            for table in tables:
                rows = await table.locator("tr").all()
                headers, data = [], []

                for row in rows:
                    ths = await row.locator("th").all()
                    tds = await row.locator("td").all()

                    if ths and not headers:
                        headers = [h.strip() for h in [await th.inner_text() for th in ths]]
                    elif tds:
                        row_data = [r.strip() for r in [await td.inner_text() for td in tds]]
                        if any(row_data):
                            data.append(row_data)

                if data:
                    cols = headers if (headers and len(headers) == len(data[0])) else None
                    df = pd.DataFrame(data, columns=cols)
                    if len(df) > len(best_df):
                        best_df = df

            return best_df
        except Exception as e:
            logger.error(f"테이블 추출 오류: {e}")
            return pd.DataFrame()

    async def _download_excel_from_page(self, page, company_name: str) -> Optional[bytes]:
        """페이지에서 엑셀 다운로드 버튼 찾아서 클릭 후 bytes 반환"""
        excel_sels = [
            "a:has-text('엑셀')",
            "button:has-text('엑셀')",
            "input[value*='엑셀']",
            "a:has-text('Excel')",
            "a[href*='.xls']",
            "a[onclick*='excel']",
            "a[onclick*='Excel']",
            "button[onclick*='excel']",
            ".btn-excel",
            "img[src*='excel']",
            "a[title*='엑셀']",
        ]
        for sel in excel_sels:
            try:
                el = page.locator(sel).first
                if await el.count() > 0:
                    async with page.expect_download(timeout=30000) as dl_info:
                        await el.click()
                    download = await dl_info.value
                    save_path = self.download_dir / f"{company_name}.xlsx"
                    await download.save_as(str(save_path))
                    with open(save_path, "rb") as f:
                        return f.read()
            except Exception as e:
                logger.debug(f"Excel 다운로드 시도 실패 ({sel}): {e}")
                continue
        logger.warning(f"{company_name}: 엑셀 다운로드 버튼을 찾지 못했습니다")
        return None

    # ──────────────────────────────────────────────────────────
    # 공통 유틸
    # ──────────────────────────────────────────────────────────

    async def _log_page_inputs(self, label: str = ""):
        """디버깅: 페이지의 모든 input/form 요소 로그 출력"""
        try:
            inputs = await self.page.locator("input, select, textarea, button, a[onclick]").all()
            logger.info(f"[진단:{label}] 요소 수={len(inputs)}")
            for el in inputs[:30]:
                try:
                    tag = await el.evaluate("e => e.tagName")
                    typ = await el.get_attribute("type") or ""
                    name = await el.get_attribute("name") or ""
                    id_ = await el.get_attribute("id") or ""
                    val = await el.get_attribute("value") or ""
                    onclick = (await el.get_attribute("onclick") or "")[:60]
                    txt = (await el.inner_text()).strip()[:30]
                    logger.info(f"  {tag} type={typ} name={name} id={id_} value={val} onclick={onclick} text={txt}")
                except Exception:
                    pass
        except Exception as e:
            logger.warning(f"진단 출력 실패: {e}")

    async def _fill_in_frame(self, frame, selectors: list, value: str) -> bool:
        for sel in selectors:
            try:
                el = frame.locator(sel).first
                if await el.count() > 0:
                    await el.fill(value)
                    return True
            except Exception:
                continue
        return False

    async def _click_in_frame(self, frame, selectors: list) -> bool:
        for sel in selectors:
            try:
                el = frame.locator(sel).first
                if await el.count() > 0:
                    await el.click()
                    return True
            except Exception:
                continue
        return False

    async def _fill_by_selectors(self, selectors: list, value: str) -> bool:
        for sel in selectors:
            try:
                el = self.page.locator(sel).first
                if await el.count() > 0:
                    await el.fill(value)
                    return True
            except Exception:
                continue
        return False

    async def _click_by_selectors(self, selectors: list) -> bool:
        for sel in selectors:
            try:
                el = self.page.locator(sel).first
                if await el.count() > 0:
                    await el.click()
                    return True
            except Exception:
                continue
        return False

    async def _select_opt(self, selectors: list, value: str) -> bool:
        """셀렉트박스 옵션 선택 (value / label 두 방식 시도, 타임아웃 2초)"""
        for sel in selectors:
            for method in ("value", "label"):
                try:
                    el = self.page.locator(sel).first
                    if await el.count() > 0:
                        if method == "value":
                            await el.select_option(value=value, timeout=2000)
                        else:
                            await el.select_option(label=value, timeout=2000)
                        return True
                except Exception:
                    continue
        return False


# ──────────────────────────────────────────────────────────────
# Excel 병합 유틸
# ──────────────────────────────────────────────────────────────

def build_combined_excel(company_results: list[dict]) -> bytes:
    """
    업체별 결과를 하나의 Excel 파일로 병합.
    각 업체는 별도 시트, '전체' 시트에는 전부 합쳐서 저장.
    """
    buf = io.BytesIO()
    all_rows = []

    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        for item in company_results:
            company = item["company"]
            df = item["df"].copy()
            if df.empty:
                continue

            # 업체명 컬럼 추가
            df.insert(0, "업체명", company)
            all_rows.append(df)

            sheet_name = company[:31]  # 시트명 31자 제한
            df.to_excel(writer, index=False, sheet_name=sheet_name)

        # 전체 시트
        if all_rows:
            combined = pd.concat(all_rows, ignore_index=True)
            combined.to_excel(writer, index=False, sheet_name="전체")

    return buf.getvalue()
