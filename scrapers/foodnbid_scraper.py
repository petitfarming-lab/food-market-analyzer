"""
푸드앤비드 (FoodNBid) 스크래퍼
수도권 매출 데이터 드릴다운 추출
"""

import asyncio
import logging
import re
from datetime import datetime
from typing import Optional
import pandas as pd
from .base_scraper import BaseScraper
from config.settings import FOODNBID

logger = logging.getLogger(__name__)


class FoodNBidScraper(BaseScraper):
    def __init__(self, user_id: str, password: str, headless: bool = True):
        super().__init__(headless=headless)
        self.user_id = user_id
        self.password = password
        self.cfg = FOODNBID
        self.logged_in = False

    async def login(self) -> bool:
        """푸드앤비드 로그인 - 메인 페이지 팝업 로그인 처리"""
        try:
            await self.page.goto(self.cfg["login_url"], wait_until="domcontentloaded", timeout=30000)
            await self.page.wait_for_timeout(2000)

            # 1) 로그인 팝업 열기 버튼 클릭 시도
            popup_selectors = self.cfg["login"]["open_popup_btn"].split(", ")
            await self._click_by_selectors(popup_selectors)
            await self.page.wait_for_timeout(1000)

            # 2) ID/PW 입력
            id_sels = self.cfg["login"]["id_field"].split(", ")
            pw_sels = self.cfg["login"]["pw_field"].split(", ")
            btn_sels = self.cfg["login"]["submit_btn"].split(", ")

            id_ok = await self._fill_by_selectors(id_sels, self.user_id)
            pw_ok = await self._fill_by_selectors(pw_sels, self.password)

            if not id_ok or not pw_ok:
                logger.error("로그인 필드를 찾지 못했습니다. 스크린샷을 확인하세요.")
                await self.screenshot("debug_foodnbid_login.png")
                return False

            await self._click_by_selectors(btn_sels)
            await self.page.wait_for_load_state("networkidle", timeout=15000)
            await self.page.wait_for_timeout(1500)

            # 3) 로그인 성공 여부 확인 (URL 변경 또는 로그아웃 버튼 존재)
            current_url = self.page.url
            page_text = await self.page.inner_text("body")
            success = (
                "로그아웃" in page_text
                or "mypage" in current_url.lower()
                or "main" in current_url.lower()
            )

            if success:
                logger.info("푸드앤비드 로그인 성공")
            else:
                logger.warning("푸드앤비드 로그인 상태 불확실 - 계속 진행")
                await self.screenshot("debug_foodnbid_login.png")

            self.logged_in = True
            return True

        except Exception as e:
            logger.error(f"푸드앤비드 로그인 오류: {e}")
            await self.screenshot("debug_foodnbid_login_error.png")
            return False

    async def get_sales_data(
        self,
        keyword: str,
        start_date: str,
        end_date: str,
        progress_callback=None,
    ) -> pd.DataFrame:
        """
        키워드 + 기간으로 수도권 매출 데이터 추출

        Args:
            keyword: 검색 키워드 (예: '돈까스', '소시지')
            start_date: 시작일 'YYYY-MM-DD'
            end_date: 종료일 'YYYY-MM-DD'
            progress_callback: 진행상황 콜백 함수 (선택)

        Returns:
            DataFrame with columns: [제품명, 카테고리, 지역, 매출금액, 수량, 단가, 날짜]
        """
        if not self.logged_in:
            await self.login()

        all_records = []

        try:
            if progress_callback:
                progress_callback("푸드앤비드: 통계 페이지 접속 중...")

            # 통계/매출 페이지로 이동
            await self.page.goto(self.cfg["stats_url"], wait_until="networkidle", timeout=30000)
            await self.page.wait_for_timeout(1500)

            # 실제 URL이 다를 경우를 대비한 네비게이션 탐색
            current_url = await self.get_current_url()
            logger.info(f"현재 URL: {current_url}")

            # 페이지 스크린샷 (디버깅용)
            await self.screenshot("debug_foodnbid_stats.png")

            # 검색 조건 입력
            filled = await self._fill_search_form(keyword, start_date, end_date)
            if not filled:
                logger.warning("검색 폼을 찾지 못했습니다. 페이지 구조를 확인하세요.")
                # 페이지 텍스트로 데이터 직접 파싱 시도
                raw_data = await self._parse_page_data()
                return self._to_dataframe(raw_data, keyword)

            if progress_callback:
                progress_callback("푸드앤비드: 데이터 조회 중...")

            await self.page.wait_for_timeout(2000)
            await self.page.wait_for_load_state("networkidle", timeout=20000)

            # 페이지별 데이터 수집
            page_num = 1
            while True:
                if progress_callback:
                    progress_callback(f"푸드앤비드: {page_num}페이지 수집 중...")

                records = await self._extract_current_page_data()
                all_records.extend(records)

                # 다음 페이지 확인
                has_next = await self._go_next_page()
                if not has_next:
                    break
                page_num += 1

                if page_num > 50:  # 안전 상한
                    logger.warning("최대 페이지 수 초과")
                    break

            logger.info(f"푸드앤비드 수집 완료: {len(all_records)}건")

        except Exception as e:
            logger.error(f"푸드앤비드 수집 오류: {e}")
            await self.screenshot("debug_foodnbid_error.png")

        df = self._to_dataframe(all_records, keyword)
        # 수도권 필터링
        df = self._filter_sudogwon(df)
        return df

    async def _fill_search_form(self, keyword: str, start_date: str, end_date: str) -> bool:
        """검색 폼 채우기"""
        cfg = self.cfg["search"]
        success = False

        # 키워드 입력
        kw_filled = await self._fill_by_selectors(cfg["keyword_field"].split(", "), keyword)
        if kw_filled:
            success = True

        # 날짜 입력
        fmt_start = start_date.replace("-", "")  # YYYYMMDD 형식도 시도
        fmt_end = end_date.replace("-", "")

        for date_val, fmt_val, sel in [
            (start_date, fmt_start, cfg["start_date"]),
            (end_date, fmt_end, cfg["end_date"]),
        ]:
            sels = sel.split(", ")
            filled = await self._fill_by_selectors(sels, date_val)
            if not filled:
                await self._fill_by_selectors(sels, fmt_val)

        # 지역 선택 (수도권)
        try:
            region_sel = cfg["region_select"].split(", ")
            for sel in region_sel:
                sel = sel.strip()
                el = self.page.locator(sel).first
                if await el.count() > 0:
                    await el.select_option(value=cfg.get("region_value", ""))
                    break
        except Exception:
            pass

        # 검색 버튼 클릭
        clicked = await self._click_by_selectors(cfg["search_btn"].split(", "))
        return success or clicked

    async def _extract_current_page_data(self) -> list[dict]:
        """현재 페이지의 테이블 데이터 추출"""
        records = []
        cfg = self.cfg["result"]

        try:
            rows = await self.extract_table_data(cfg["table"], cfg["rows"])
            col_map = cfg["columns"]

            for row in rows:
                if len(row) <= max(col_map.values()):
                    continue

                # 숫자 정리
                sales_raw = row[col_map["sales_amount"]] if col_map["sales_amount"] < len(row) else "0"
                qty_raw = row[col_map["quantity"]] if col_map["quantity"] < len(row) else "0"

                record = {
                    "제품명": row[col_map["product_name"]] if col_map["product_name"] < len(row) else "",
                    "카테고리": row[col_map["category"]] if col_map["category"] < len(row) else "",
                    "지역": row[col_map["region"]] if col_map["region"] < len(row) else "",
                    "매출금액": self._parse_number(sales_raw),
                    "수량": self._parse_number(qty_raw),
                }
                if record["제품명"]:
                    records.append(record)

        except Exception as e:
            logger.error(f"페이지 데이터 추출 오류: {e}")

            # 폴백: 페이지 전체 텍스트에서 파싱
            records = await self._parse_page_data()

        return records

    async def _parse_page_data(self) -> list[dict]:
        """
        폴백: 페이지 전체에서 데이터 추출 시도
        실제 사이트 구조에 맞게 커스터마이징 필요
        """
        records = []
        try:
            # 모든 테이블 행 시도
            all_tables = await self.page.locator("table").all()
            for table in all_tables:
                rows = await table.locator("tr").all()
                for row in rows:
                    cells = await row.locator("td").all()
                    if len(cells) >= 3:
                        texts = [await c.inner_text() for c in cells]
                        # 금액 패턴이 있는 행만 수집
                        if any(re.search(r'\d{3,}', t) for t in texts):
                            record = {
                                "제품명": texts[0].strip() if texts else "",
                                "카테고리": texts[1].strip() if len(texts) > 1 else "",
                                "지역": texts[2].strip() if len(texts) > 2 else "",
                                "매출금액": self._parse_number(texts[3]) if len(texts) > 3 else 0,
                                "수량": self._parse_number(texts[4]) if len(texts) > 4 else 0,
                            }
                            if record["제품명"]:
                                records.append(record)
        except Exception as e:
            logger.error(f"폴백 파싱 오류: {e}")
        return records

    async def _go_next_page(self) -> bool:
        """다음 페이지로 이동, 없으면 False 반환"""
        cfg = self.cfg["result"]
        try:
            next_sels = cfg["next_page"].split(", ")
            for sel in next_sels:
                sel = sel.strip()
                el = self.page.locator(sel).first
                if await el.count() > 0:
                    is_disabled = await el.get_attribute("class") or ""
                    if "disabled" in is_disabled or "inactive" in is_disabled:
                        return False
                    await el.click()
                    await self.page.wait_for_load_state("networkidle", timeout=15000)
                    await self.page.wait_for_timeout(800)
                    return True
        except Exception:
            pass
        return False

    def _filter_sudogwon(self, df: pd.DataFrame) -> pd.DataFrame:
        """수도권(서울/경기/인천) 데이터만 필터링"""
        if df.empty:
            return df
        keywords = self.cfg["sudogwon_regions"]
        if "지역" in df.columns:
            mask = df["지역"].str.contains("|".join(keywords), na=False, case=False)
            filtered = df[mask]
            if filtered.empty:
                logger.warning("수도권 필터 결과 없음 - 전체 데이터 반환")
                return df
            return filtered
        return df

    def _to_dataframe(self, records: list[dict], keyword: str) -> pd.DataFrame:
        """레코드 리스트를 DataFrame으로 변환"""
        if not records:
            return pd.DataFrame(columns=["제품명", "카테고리", "지역", "매출금액", "수량", "단가", "키워드"])

        df = pd.DataFrame(records)
        df["키워드"] = keyword

        if "매출금액" in df.columns and "수량" in df.columns:
            df["단가"] = df.apply(
                lambda r: round(r["매출금액"] / r["수량"]) if r["수량"] > 0 else 0,
                axis=1
            )

        # 타입 정리
        for col in ["매출금액", "수량", "단가"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

        return df

    @staticmethod
    def _parse_number(text: str) -> int:
        """'1,234,567원' → 1234567"""
        cleaned = re.sub(r"[^\d]", "", str(text))
        return int(cleaned) if cleaned else 0
