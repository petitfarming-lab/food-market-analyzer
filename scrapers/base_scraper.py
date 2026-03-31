"""
공통 스크래퍼 베이스 클래스
Playwright 브라우저 세션 관리, 로그인, 페이지 유틸리티 포함
"""

import asyncio
import logging
from typing import Optional
from playwright.async_api import async_playwright, Browser, BrowserContext, Page

logger = logging.getLogger(__name__)


class BaseScraper:
    def __init__(self, headless: bool = True):
        self.headless = headless
        self._playwright = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None

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
            viewport={"width": 1280, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            locale="ko-KR",
        )
        self.page = await self._context.new_page()
        # 봇 감지 우회
        await self.page.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )

    async def close(self):
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()

    async def login(self, login_url: str, id_field: str, pw_field: str,
                    submit_btn: str, user_id: str, password: str) -> bool:
        """공통 로그인 처리 (여러 셀렉터를 순서대로 시도)"""
        try:
            await self.page.goto(login_url, wait_until="networkidle", timeout=30000)
            await self.page.wait_for_timeout(1000)

            # ID 입력 (여러 셀렉터 시도)
            id_filled = await self._fill_by_selectors(id_field.split(", "), user_id)
            if not id_filled:
                logger.error("ID 입력 필드를 찾지 못했습니다.")
                return False

            # PW 입력
            pw_filled = await self._fill_by_selectors(pw_field.split(", "), password)
            if not pw_filled:
                logger.error("비밀번호 입력 필드를 찾지 못했습니다.")
                return False

            # 로그인 버튼 클릭
            clicked = await self._click_by_selectors(submit_btn.split(", "))
            if not clicked:
                logger.error("로그인 버튼을 찾지 못했습니다.")
                return False

            await self.page.wait_for_load_state("networkidle", timeout=15000)
            logger.info(f"로그인 성공: {login_url}")
            return True

        except Exception as e:
            logger.error(f"로그인 실패: {e}")
            return False

    async def _fill_by_selectors(self, selectors: list, value: str) -> bool:
        """여러 셀렉터를 순서대로 시도하여 첫 번째 성공한 것으로 채움"""
        for sel in selectors:
            sel = sel.strip()
            try:
                element = self.page.locator(sel).first
                count = await element.count()
                if count > 0:
                    await element.fill(value)
                    return True
            except Exception:
                continue
        return False

    async def _click_by_selectors(self, selectors: list) -> bool:
        """여러 셀렉터를 순서대로 시도하여 첫 번째 성공한 것을 클릭"""
        for sel in selectors:
            sel = sel.strip()
            try:
                element = self.page.locator(sel).first
                count = await element.count()
                if count > 0:
                    await element.click()
                    return True
            except Exception:
                continue
        return False

    async def safe_text(self, selector: str, default: str = "") -> str:
        """셀렉터로 텍스트 추출 (실패 시 기본값 반환)"""
        try:
            el = self.page.locator(selector).first
            if await el.count() > 0:
                text = await el.inner_text()
                return text.strip()
        except Exception:
            pass
        return default

    async def extract_table_data(self, table_selector: str, row_selector: str) -> list[list[str]]:
        """테이블 데이터 추출"""
        rows = []
        try:
            table = self.page.locator(table_selector).first
            if await table.count() == 0:
                return rows
            all_rows = table.locator(row_selector)
            count = await all_rows.count()
            for i in range(count):
                row = all_rows.nth(i)
                cells = row.locator("td, th")
                cell_count = await cells.count()
                row_data = []
                for j in range(cell_count):
                    text = await cells.nth(j).inner_text()
                    row_data.append(text.strip())
                if row_data:
                    rows.append(row_data)
        except Exception as e:
            logger.error(f"테이블 추출 오류: {e}")
        return rows

    async def find_label_value_in_page(self, label_keywords: list[str]) -> str:
        """
        페이지에서 라벨 키워드를 포함하는 셀 옆의 값을 추출
        (상품 상세페이지의 스펙 테이블에서 주로 사용)
        """
        for keyword in label_keywords:
            try:
                # th 다음 td 패턴
                elements = await self.page.locator(f"th:has-text('{keyword}') + td").all()
                if elements:
                    text = await elements[0].inner_text()
                    return text.strip()

                # td 다음 td 패턴
                elements = await self.page.locator(f"td:has-text('{keyword}') + td").all()
                if elements:
                    text = await elements[0].inner_text()
                    return text.strip()

                # dl/dt/dd 패턴
                elements = await self.page.locator(f"dt:has-text('{keyword}') + dd").all()
                if elements:
                    text = await elements[0].inner_text()
                    return text.strip()

            except Exception:
                continue
        return ""

    async def screenshot(self, path: str = "debug_screenshot.png"):
        """디버깅용 스크린샷"""
        if self.page:
            await self.page.screenshot(path=path, full_page=True)
            logger.info(f"스크린샷 저장: {path}")

    async def get_current_url(self) -> str:
        return self.page.url if self.page else ""
