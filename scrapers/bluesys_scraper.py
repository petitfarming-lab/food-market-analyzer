"""
블루시스마켓 (Bluesys Market) 스크래퍼
상품 상세 정보 추출: 학교매입가, 함량, TIER, 조리법, 제조사, 브랜드
"""

import asyncio
import logging
import re
from typing import Optional
import pandas as pd
from .base_scraper import BaseScraper
from config.settings import BLUESYS

logger = logging.getLogger(__name__)


class BluesysMarketScraper(BaseScraper):
    def __init__(self, user_id: str, password: str, headless: bool = True):
        super().__init__(headless=headless)
        self.user_id = user_id
        self.password = password
        self.cfg = BLUESYS
        self.logged_in = False

    async def login(self) -> bool:
        """블루시스마켓 로그인 - AJAX 기반 처리"""
        try:
            await self.page.goto(self.cfg["login_url"], wait_until="domcontentloaded", timeout=30000)
            await self.page.wait_for_timeout(1500)

            id_sels = self.cfg["login"]["id_field"].split(", ")
            pw_sels = self.cfg["login"]["pw_field"].split(", ")
            btn_sels = self.cfg["login"]["submit_btn"].split(", ")

            id_ok = await self._fill_by_selectors(id_sels, self.user_id)
            pw_ok = await self._fill_by_selectors(pw_sels, self.password)

            if not id_ok or not pw_ok:
                logger.error("블루시스마켓 로그인 필드를 찾지 못했습니다.")
                await self.screenshot("debug_bluesys_login.png")
                return False

            # 버튼 클릭 또는 Enter
            clicked = await self._click_by_selectors(btn_sels)
            if not clicked:
                await self.page.keyboard.press("Enter")

            # AJAX 응답 대기
            await self.page.wait_for_timeout(3000)

            # 로그인 성공 확인
            current_url = self.page.url
            page_text = await self.page.inner_text("body")
            success = (
                "로그아웃" in page_text
                or "마이페이지" in page_text
                or "main" in current_url.lower()
                or "login" not in current_url.lower()
            )

            if success:
                logger.info("블루시스마켓 로그인 성공")
            else:
                logger.warning("블루시스마켓 로그인 상태 불확실")
                await self.screenshot("debug_bluesys_login.png")

            self.logged_in = True
            return True

        except Exception as e:
            logger.error(f"블루시스마켓 로그인 오류: {e}")
            await self.screenshot("debug_bluesys_login_error.png")
            return False

    async def get_product_data(
        self,
        keyword: str,
        max_products: int = 100,
        progress_callback=None,
    ) -> pd.DataFrame:
        """
        키워드로 상품 목록 검색 후 상세 정보 추출

        Args:
            keyword: 검색 키워드
            max_products: 최대 수집 상품 수
            progress_callback: 진행상황 콜백

        Returns:
            DataFrame with columns: [제품명, 학교매입가, 함량, TIER, 조리법, 제조사, 브랜드, 상품URL]
        """
        if not self.logged_in:
            await self.login()

        all_records = []

        try:
            if progress_callback:
                progress_callback("블루시스마켓: 상품 검색 중...")

            # 검색 페이지로 이동
            await self.page.goto(self.cfg["search_url"], wait_until="networkidle", timeout=30000)
            await self.page.wait_for_timeout(1000)

            # 검색어 입력
            await self._search_keyword(keyword)
            await self.page.wait_for_timeout(2000)
            await self.page.wait_for_load_state("networkidle", timeout=20000)

            # 상품 URL 목록 수집
            if progress_callback:
                progress_callback("블루시스마켓: 상품 목록 수집 중...")

            product_urls = await self._collect_product_urls(max_products, progress_callback)
            logger.info(f"상품 URL {len(product_urls)}개 수집")

            # 각 상품 상세 페이지 방문하여 정보 추출
            for idx, url in enumerate(product_urls):
                if progress_callback:
                    progress_callback(f"블루시스마켓: 상품 상세 수집 ({idx+1}/{len(product_urls)})")

                record = await self._extract_product_detail(url)
                if record:
                    record["키워드"] = keyword
                    all_records.append(record)

                await self.page.wait_for_timeout(500)  # 요청 간 딜레이

            logger.info(f"블루시스마켓 수집 완료: {len(all_records)}건")

        except Exception as e:
            logger.error(f"블루시스마켓 수집 오류: {e}")
            await self.screenshot("debug_bluesys_error.png")

        return self._to_dataframe(all_records)

    async def _search_keyword(self, keyword: str):
        """키워드 검색 실행"""
        cfg = self.cfg["search"]

        kw_filled = await self._fill_by_selectors(cfg["keyword_field"].split(", "), keyword)
        if not kw_filled:
            # URL 파라미터로 직접 검색 시도
            search_url = f"{self.cfg['search_url']}?keyword={keyword}"
            await self.page.goto(search_url, wait_until="networkidle", timeout=30000)
            return

        clicked = await self._click_by_selectors(cfg["search_btn"].split(", "))
        if not clicked:
            # Enter 키로 검색 시도
            await self.page.keyboard.press("Enter")

    async def _collect_product_urls(self, max_products: int, progress_callback=None) -> list[str]:
        """전체 페이지에서 상품 상세 URL 목록 수집"""
        urls = []
        cfg_list = self.cfg["product_list"]
        page_num = 1

        while len(urls) < max_products:
            page_urls = await self._get_current_page_urls()
            urls.extend(page_urls)
            logger.info(f"페이지 {page_num}: {len(page_urls)}개 URL 수집 (누적: {len(urls)})")

            if len(urls) >= max_products:
                break

            has_next = await self._go_to_next_list_page()
            if not has_next:
                break
            page_num += 1

            if page_num > 30:
                break

        # 중복 제거 및 상한 적용
        unique_urls = list(dict.fromkeys(urls))[:max_products]
        return unique_urls

    async def _get_current_page_urls(self) -> list[str]:
        """현재 목록 페이지에서 상품 상세 URL 추출"""
        urls = []
        cfg = self.cfg["product_list"]
        base_url = self.cfg["base_url"]

        try:
            # 상품 링크 셀렉터들을 순서대로 시도
            link_selectors = [
                ".product-item a", ".goods-item a", "li.item a",
                ".product-list a[href*='detail']", ".goods-list a[href*='detail']",
                "a[href*='product/detail']", "a[href*='goods/detail']",
                "a[href*='goodsNo']", "a[href*='productId']",
                ".product-name a", ".goods-name a",
            ]

            for sel in link_selectors:
                links = await self.page.locator(sel).all()
                if links:
                    for link in links:
                        href = await link.get_attribute("href")
                        if href:
                            if href.startswith("http"):
                                urls.append(href)
                            elif href.startswith("/"):
                                urls.append(base_url + href)
                    if urls:
                        break

            # 폴백: 모든 링크에서 제품 상세 URL 패턴 검색
            if not urls:
                all_links = await self.page.locator("a[href]").all()
                for link in all_links:
                    href = await link.get_attribute("href") or ""
                    if any(p in href for p in ["detail", "goodsNo", "productId", "goods_no"]):
                        if href.startswith("http"):
                            urls.append(href)
                        elif href.startswith("/"):
                            urls.append(base_url + href)

        except Exception as e:
            logger.error(f"URL 수집 오류: {e}")

        return list(dict.fromkeys(urls))  # 중복 제거

    async def _go_to_next_list_page(self) -> bool:
        """다음 목록 페이지로 이동"""
        next_selectors = [
            ".pagination .next:not(.disabled)",
            ".paging .btn-next:not(.disabled)",
            "a.next:not(.disabled)",
            ".pagination li:last-child a",
            "a[aria-label='다음']",
            "a:has-text('다음')",
            "a:has-text('>')",
        ]
        for sel in next_selectors:
            try:
                el = self.page.locator(sel).first
                if await el.count() > 0:
                    await el.click()
                    await self.page.wait_for_load_state("networkidle", timeout=15000)
                    await self.page.wait_for_timeout(800)
                    return True
            except Exception:
                continue
        return False

    async def _extract_product_detail(self, url: str) -> Optional[dict]:
        """상품 상세 페이지에서 스펙 정보 추출"""
        try:
            await self.page.goto(url, wait_until="networkidle", timeout=30000)
            await self.page.wait_for_timeout(800)

            cfg = self.cfg["product_detail"]

            # 각 항목 추출
            record = {
                "제품명": await self._extract_field(cfg["product_name"]),
                "학교매입가": await self._extract_field(cfg["school_price"]),
                "함량": await self._extract_field(cfg["weight"]),
                "TIER": await self._extract_field(cfg["tier"]),
                "조리법": await self._extract_field(cfg["cooking_method"]),
                "제조사": await self._extract_field(cfg["manufacturer"]),
                "브랜드": await self._extract_field(cfg["brand"]),
                "상품URL": url,
            }

            # 제품명이 없으면 페이지 제목으로 대체
            if not record["제품명"]:
                record["제품명"] = await self.page.title()
                record["제품명"] = record["제품명"].replace(" - 블루시스마켓", "").strip()

            # 학교매입가 숫자 정리
            if record["학교매입가"]:
                record["학교매입가_숫자"] = self._parse_price(record["학교매입가"])
            else:
                record["학교매입가_숫자"] = 0

            # 함량 숫자 정리
            if record["함량"]:
                record["함량_g"] = self._parse_weight(record["함량"])
            else:
                record["함량_g"] = 0

            logger.debug(f"상품 추출: {record['제품명']}")
            return record

        except Exception as e:
            logger.error(f"상품 상세 추출 오류 ({url}): {e}")
            return None

    async def _extract_field(self, field_cfg: dict) -> str:
        """필드 설정에 따라 값 추출 (셀렉터 → 라벨 키워드 순으로 시도)"""
        # 1. CSS 셀렉터로 시도
        for sel in field_cfg.get("selectors", []):
            try:
                el = self.page.locator(sel).first
                if await el.count() > 0:
                    text = await el.inner_text()
                    cleaned = text.strip()
                    if cleaned:
                        return cleaned
            except Exception:
                continue

        # 2. 라벨 키워드로 테이블에서 찾기
        label_keywords = field_cfg.get("label_keywords", [])
        result = await self.find_label_value_in_page(label_keywords)
        return result

    @staticmethod
    def _parse_price(text: str) -> int:
        """'12,500원' → 12500"""
        cleaned = re.sub(r"[^\d]", "", str(text))
        return int(cleaned) if cleaned else 0

    @staticmethod
    def _parse_weight(text: str) -> float:
        """'500g', '1kg', '500ml' → 숫자(g 또는 ml 기준)"""
        text = str(text).lower().replace(",", "")
        kg_match = re.search(r"(\d+\.?\d*)\s*kg", text)
        g_match = re.search(r"(\d+\.?\d*)\s*g", text)
        if kg_match:
            return float(kg_match.group(1)) * 1000
        if g_match:
            return float(g_match.group(1))
        # 단순 숫자만 있으면 그대로
        num_match = re.search(r"(\d+\.?\d*)", text)
        return float(num_match.group(1)) if num_match else 0

    def _to_dataframe(self, records: list[dict]) -> pd.DataFrame:
        if not records:
            return pd.DataFrame(columns=[
                "키워드", "제품명", "학교매입가", "학교매입가_숫자",
                "함량", "함량_g", "TIER", "조리법", "제조사", "브랜드", "상품URL"
            ])
        df = pd.DataFrame(records)
        # 열 순서 정리
        ordered_cols = [
            "키워드", "제품명", "학교매입가", "학교매입가_숫자",
            "함량", "함량_g", "TIER", "조리법", "제조사", "브랜드", "상품URL"
        ]
        for col in ordered_cols:
            if col not in df.columns:
                df[col] = ""
        return df[ordered_cols]
