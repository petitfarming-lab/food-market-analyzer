"""
식봄(foodspring.co.kr) 스크래퍼
제품명 + 제조사로 검색하여 판매 제품명 및 판매가 추출
"""

import asyncio
import logging
import re

import pandas as pd
from .base_scraper import BaseScraper

logger = logging.getLogger(__name__)

BASE_URL = "https://www.foodspring.co.kr"
SEARCH_URL = BASE_URL + "/search/all?key={keyword}"
MAX_RESULTS = 5


class FoodspringScraper(BaseScraper):
    def __init__(self, headless: bool = True):
        super().__init__(headless=headless)

    async def search_products(
        self,
        df_input: pd.DataFrame,
        progress_callback=None,
    ) -> pd.DataFrame:
        """
        입력 DataFrame의 제품목록을 식봄에서 검색

        Args:
            df_input: '제품명', '제조사' 컬럼 포함 DataFrame
            progress_callback: 진행 상황 콜백

        Returns:
            DataFrame with columns [No, 제품명, 제조사, 순위, 판매제품명, 판매가, 판매가_숫자, 상품URL, 검색상태]
        """
        rows = []
        total = len(df_input)

        for idx, row in df_input.iterrows():
            product_name = str(row.get("제품명", "")).strip()
            manufacturer = str(row.get("제조사", "")).strip()

            if not product_name:
                continue

            no = idx + 1
            if progress_callback:
                progress_callback(f"식봄 검색 중 [{no}/{total}]: {manufacturer} {product_name}")

            results = await self._search_one(product_name, manufacturer)

            if results:
                for rank, r in enumerate(results, 1):
                    rows.append({
                        "No": no,
                        "제품명": product_name,
                        "제조사": manufacturer,
                        "순위": rank,
                        "판매제품명": r["판매제품명"],
                        "판매가": r["판매가"],
                        "판매가_숫자": r["판매가_숫자"],
                        "상품URL": r["상품URL"],
                        "검색상태": "발견",
                    })
            else:
                rows.append({
                    "No": no,
                    "제품명": product_name,
                    "제조사": manufacturer,
                    "순위": "-",
                    "판매제품명": "검색 결과 없음",
                    "판매가": "-",
                    "판매가_숫자": 0,
                    "상품URL": "",
                    "검색상태": "미발견",
                })

            await asyncio.sleep(0.8)

        if not rows:
            return pd.DataFrame(columns=[
                "No", "제품명", "제조사", "순위",
                "판매제품명", "판매가", "판매가_숫자", "상품URL", "검색상태"
            ])

        return pd.DataFrame(rows)

    async def _search_one(self, product_name: str, manufacturer: str) -> list[dict]:
        """단일 제품 검색"""
        keyword = f"{manufacturer} {product_name}".strip()
        url = SEARCH_URL.format(keyword=keyword)

        results = []
        try:
            await self.page.goto(url, wait_until="domcontentloaded", timeout=30000)
            try:
                await self.page.wait_for_selector(
                    "a[href*='/goods/detail']", timeout=8000
                )
            except Exception:
                await self.page.wait_for_timeout(3000)
            await self.page.wait_for_load_state("networkidle", timeout=12000)

            results = await self._extract_results()

            # 결과 없으면 제조사 제외 재시도
            if not results and manufacturer:
                url2 = SEARCH_URL.format(keyword=product_name)
                await self.page.goto(url2, wait_until="domcontentloaded", timeout=30000)
                await self.page.wait_for_timeout(3000)
                await self.page.wait_for_load_state("networkidle", timeout=12000)
                results = await self._extract_results()

        except Exception as e:
            logger.error(f"식봄 검색 오류 ({product_name}): {e}")

        return results[:MAX_RESULTS]

    async def _extract_results(self) -> list[dict]:
        """현재 페이지에서 상품 목록 추출"""
        results = []
        seen = set()

        product_links = await self.page.locator("a[href*='/goods/detail']").all()

        for link in product_links:
            try:
                href = await link.get_attribute("href") or ""
                if href in seen:
                    continue
                seen.add(href)

                full_url = BASE_URL + href if href.startswith("/") else href
                inner = (await link.inner_text()).strip()

                # 링크 내부에서 가격 추출
                price_match = re.search(r"([\d,]+)\s*원", inner)
                if price_match:
                    price_text = price_match.group(0)
                    name_text = inner[: price_match.start()].strip()
                else:
                    price_text = ""
                    name_text = inner

                # 링크만으론 부족할 때 → 부모 카드 텍스트 탐색
                if not price_text or not name_text or len(name_text) < 2:
                    card_text = await link.evaluate("""
                        (el) => {
                            let node = el;
                            for (let i = 0; i < 6; i++) {
                                node = node.parentElement;
                                if (!node) break;
                                const t = (node.innerText || '').trim();
                                if (t.includes('원') && t.length < 400) return t;
                            }
                            return '';
                        }
                    """)
                    if card_text:
                        pm = re.search(r"([\d,]+)\s*원", card_text)
                        if pm:
                            price_text = pm.group(0)
                            lines = [
                                l.strip()
                                for l in card_text[: pm.start()].splitlines()
                                if l.strip()
                            ]
                            if lines:
                                name_text = lines[-1]

                name_text = re.sub(r"\s+", " ", name_text).strip()

                if name_text and len(name_text) >= 2:
                    price_num = _parse_price(price_text)
                    results.append({
                        "판매제품명": name_text,
                        "판매가": f"{price_num:,}원" if price_num else price_text or "-",
                        "판매가_숫자": price_num,
                        "상품URL": full_url,
                    })
            except Exception:
                continue

        # 폴백: 구조적 셀렉터
        if not results:
            results = await self._extract_fallback()

        return results

    async def _extract_fallback(self) -> list[dict]:
        """CSS 클래스 패턴 기반 폴백 추출"""
        results = []
        card_selectors = [
            "[class*='ProductCard']", "[class*='product-card']",
            "[class*='GoodsItem']", "[class*='goods-item']",
            "[class*='SearchItem']", "[class*='item-wrap']",
        ]
        for card_sel in card_selectors:
            cards = await self.page.locator(card_sel).all()
            if not cards:
                continue
            for card in cards[:MAX_RESULTS]:
                try:
                    name_text, price_text, card_url = "", "", ""
                    for ns in ["[class*='name']", "[class*='title']", "strong", "h3", "h4"]:
                        el = card.locator(ns).first
                        if await el.count() > 0:
                            t = (await el.inner_text()).strip()
                            if t and len(t) >= 2:
                                name_text = t
                                break
                    for ps in ["[class*='price']", "[class*='Price']", "strong"]:
                        el = card.locator(ps).first
                        if await el.count() > 0:
                            t = (await el.inner_text()).strip()
                            if "원" in t:
                                price_text = t
                                break
                    link_el = card.locator("a[href*='/goods/detail']").first
                    if await link_el.count() > 0:
                        href = await link_el.get_attribute("href") or ""
                        card_url = BASE_URL + href if href.startswith("/") else href
                    if name_text:
                        price_num = _parse_price(price_text)
                        results.append({
                            "판매제품명": name_text,
                            "판매가": f"{price_num:,}원" if price_num else price_text or "-",
                            "판매가_숫자": price_num,
                            "상품URL": card_url,
                        })
                except Exception:
                    continue
            if results:
                break
        return results


def _parse_price(text: str) -> int:
    cleaned = re.sub(r"[^\d]", "", str(text))
    return int(cleaned) if cleaned else 0
