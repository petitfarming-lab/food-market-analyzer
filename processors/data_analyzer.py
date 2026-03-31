"""
데이터 분석 모듈
- 푸드앤비드 매출 + 블루시스마켓 상품정보 통합
- 매출 타입별 분석
- 경쟁력 있는 제품 스펙/가격 도출
"""

import re
import logging
import numpy as np
import pandas as pd
from config.settings import ANALYSIS

logger = logging.getLogger(__name__)


class DataAnalyzer:
    def __init__(self):
        self.cfg = ANALYSIS

    # ============================================================
    # 1. 데이터 통합
    # ============================================================

    def merge_datasets(
        self,
        sales_df: pd.DataFrame,
        product_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        푸드앤비드 매출 + 블루시스마켓 상품 데이터 통합
        제품명 유사도 기반 매칭 (fuzzy join)
        """
        if sales_df.empty or product_df.empty:
            logger.warning("통합할 데이터가 부족합니다.")
            return pd.DataFrame()

        # 제품명 정규화
        sales_df = sales_df.copy()
        product_df = product_df.copy()
        sales_df["_제품명_norm"] = sales_df["제품명"].apply(self._normalize_name)
        product_df["_제품명_norm"] = product_df["제품명"].apply(self._normalize_name)

        # 제조사/브랜드 기반 매칭 (exact match 우선)
        merged = pd.merge(
            sales_df,
            product_df,
            on="_제품명_norm",
            how="outer",
            suffixes=("_매출", "_상품"),
        )

        # exact match 실패한 것들은 부분 문자열 매칭 시도
        unmatched_sales = sales_df[~sales_df["_제품명_norm"].isin(merged["_제품명_norm"].dropna())]
        if not unmatched_sales.empty:
            fuzzy_matches = self._fuzzy_match(unmatched_sales, product_df)
            if not fuzzy_matches.empty:
                merged = pd.concat([merged, fuzzy_matches], ignore_index=True)

        merged.drop(columns=["_제품명_norm"], errors="ignore", inplace=True)
        return merged

    def _fuzzy_match(self, sales_df: pd.DataFrame, product_df: pd.DataFrame) -> pd.DataFrame:
        """제품명 부분 일치로 매칭"""
        results = []
        for _, s_row in sales_df.iterrows():
            s_name = s_row.get("_제품명_norm", "")
            best_match = None
            best_score = 0
            for _, p_row in product_df.iterrows():
                p_name = p_row.get("_제품명_norm", "")
                score = self._name_similarity(s_name, p_name)
                if score > best_score:
                    best_score = score
                    best_match = p_row
            if best_score > 0.5 and best_match is not None:
                combined = {**s_row.to_dict(), **best_match.to_dict()}
                results.append(combined)
        return pd.DataFrame(results) if results else pd.DataFrame()

    # ============================================================
    # 2. 매출 분석
    # ============================================================

    def analyze_sales_by_type(self, sales_df: pd.DataFrame) -> pd.DataFrame:
        """
        제품 타입별 매출 분석
        (예: 냉동돈까스, 냉장소시지 등 타입 분류)
        """
        if sales_df.empty:
            return pd.DataFrame()

        df = sales_df.copy()
        df["제품타입"] = df["제품명"].apply(self._classify_product_type)

        summary = (
            df.groupby("제품타입")
            .agg(
                총매출금액=("매출금액", "sum"),
                총수량=("수량", "sum"),
                평균단가=("단가", "mean"),
                제품수=("제품명", "nunique"),
            )
            .reset_index()
            .sort_values("총매출금액", ascending=False)
        )

        summary["매출비중(%)"] = (
            summary["총매출금액"] / summary["총매출금액"].sum() * 100
        ).round(2)
        summary["평균단가"] = summary["평균단가"].round(0).astype(int)

        return summary.head(self.cfg["top_n_types"])

    def get_top_products(self, sales_df: pd.DataFrame, top_n: int = 20) -> pd.DataFrame:
        """매출 상위 제품 목록"""
        if sales_df.empty:
            return pd.DataFrame()

        return (
            sales_df.groupby("제품명")
            .agg(
                총매출금액=("매출금액", "sum"),
                총수량=("수량", "sum"),
                평균단가=("단가", "mean"),
            )
            .reset_index()
            .sort_values("총매출금액", ascending=False)
            .head(top_n)
        )

    def get_regional_breakdown(self, sales_df: pd.DataFrame) -> pd.DataFrame:
        """지역별 매출 드릴다운"""
        if sales_df.empty or "지역" not in sales_df.columns:
            return pd.DataFrame()

        return (
            sales_df.groupby("지역")
            .agg(
                총매출금액=("매출금액", "sum"),
                총수량=("수량", "sum"),
                제품수=("제품명", "nunique"),
            )
            .reset_index()
            .sort_values("총매출금액", ascending=False)
        )

    # ============================================================
    # 3. 경쟁력 분석
    # ============================================================

    def analyze_competitive_spec(self, product_df: pd.DataFrame, sales_df: pd.DataFrame) -> dict:
        """
        경쟁력 있는 제품 스펙/가격 도출

        Returns:
            {
                "recommended_price_range": (min, max),
                "recommended_weight": "XXXg",
                "dominant_tier": "TIER명",
                "dominant_cooking_method": "조리법",
                "top_brands": [...],
                "top_manufacturers": [...],
                "competitive_products": DataFrame,
                "insight_text": "분석 결과 텍스트"
            }
        """
        result = {}

        if product_df.empty:
            return {"insight_text": "블루시스마켓 상품 데이터가 없습니다."}

        df = product_df.copy()

        # 학교매입가 분석
        price_data = df["학교매입가_숫자"].replace(0, np.nan).dropna()
        if not price_data.empty:
            low = price_data.quantile(self.cfg["competitive_price_percentile"])
            high = price_data.quantile(1 - self.cfg["competitive_price_percentile"])
            result["recommended_price_range"] = (int(low), int(high))
            result["median_price"] = int(price_data.median())
        else:
            result["recommended_price_range"] = (0, 0)
            result["median_price"] = 0

        # 함량 분석
        weight_data = df["함량_g"].replace(0, np.nan).dropna()
        if not weight_data.empty:
            result["recommended_weight_g"] = round(weight_data.median(), 0)
            result["weight_distribution"] = {
                "최솟값": int(weight_data.min()),
                "중앙값": int(weight_data.median()),
                "최댓값": int(weight_data.max()),
            }
        else:
            result["recommended_weight_g"] = 0
            result["weight_distribution"] = {}

        # TIER 분포
        if "TIER" in df.columns:
            tier_counts = df["TIER"].value_counts()
            result["tier_distribution"] = tier_counts.to_dict()
            result["dominant_tier"] = tier_counts.index[0] if not tier_counts.empty else ""
        else:
            result["dominant_tier"] = ""
            result["tier_distribution"] = {}

        # 조리법 분포
        if "조리법" in df.columns:
            cook_counts = df["조리법"].value_counts()
            result["cooking_method_distribution"] = cook_counts.to_dict()
            result["dominant_cooking_method"] = cook_counts.index[0] if not cook_counts.empty else ""
        else:
            result["dominant_cooking_method"] = ""
            result["cooking_method_distribution"] = {}

        # 브랜드/제조사 순위
        if "브랜드" in df.columns:
            result["top_brands"] = df["브랜드"].value_counts().head(5).index.tolist()
        else:
            result["top_brands"] = []

        if "제조사" in df.columns:
            result["top_manufacturers"] = df["제조사"].value_counts().head(5).index.tolist()
        else:
            result["top_manufacturers"] = []

        # 매출 상위 제품과 가격 교차 분석
        if not sales_df.empty and "제품명" in sales_df.columns:
            top_sales = self.get_top_products(sales_df, top_n=10)
            comp = pd.merge(
                top_sales,
                df[["제품명", "학교매입가_숫자", "함량_g", "TIER", "조리법", "브랜드", "제조사"]],
                on="제품명",
                how="inner"
            )
            result["competitive_products"] = comp
        else:
            result["competitive_products"] = pd.DataFrame()

        # 인사이트 텍스트 생성
        result["insight_text"] = self._generate_insight_text(result, product_df, sales_df)

        return result

    def _generate_insight_text(
        self, result: dict, product_df: pd.DataFrame, sales_df: pd.DataFrame
    ) -> str:
        """분석 결과를 자연어 인사이트로 변환"""
        lines = []

        # 가격 인사이트
        low, high = result.get("recommended_price_range", (0, 0))
        median = result.get("median_price", 0)
        if median > 0:
            lines.append(
                f"📌 **가격 경쟁력**: 학교매입가 중앙값은 **{median:,}원**이며, "
                f"경쟁력 있는 가격대는 **{low:,}원 ~ {high:,}원** 범위입니다."
            )

        # 함량 인사이트
        weight = result.get("recommended_weight_g", 0)
        if weight > 0:
            lines.append(f"📦 **적정 함량**: 중앙값 기준 **{int(weight)}g** 내외 제품이 주류입니다.")

        # TIER 인사이트
        tier = result.get("dominant_tier", "")
        tier_dist = result.get("tier_distribution", {})
        if tier:
            tier_pct = round(tier_dist.get(tier, 0) / max(sum(tier_dist.values()), 1) * 100, 1)
            lines.append(f"🏷 **주력 TIER**: **{tier}** 제품이 {tier_pct}%로 가장 많습니다.")

        # 조리법 인사이트
        cook = result.get("dominant_cooking_method", "")
        if cook:
            lines.append(f"🍳 **주요 조리법**: **{cook}** 방식이 시장 주류입니다.")

        # 브랜드 인사이트
        brands = result.get("top_brands", [])
        if brands:
            lines.append(f"🏢 **주요 브랜드**: {', '.join(brands[:3])}")

        # 매출 인사이트
        if not sales_df.empty and "매출금액" in sales_df.columns:
            total = sales_df["매출금액"].sum()
            top_prod = sales_df.groupby("제품명")["매출금액"].sum().idxmax() if not sales_df.empty else ""
            lines.append(f"💰 **수도권 총매출**: {total:,}원")
            if top_prod:
                lines.append(f"🥇 **매출 1위 제품**: {top_prod}")

        # 기획 제안
        if median > 0 and weight > 0:
            lines.append("")
            lines.append("---")
            lines.append("### 💡 기획 제안")
            lines.append(
                f"경쟁력 있는 신제품 기준: "
                f"**함량 {int(weight)}g**, **가격 {low:,}~{high:,}원**, "
                f"**{tier or 'Standard'} TIER**, **{cook or '다용도'} 조리법** "
                f"→ 시장 평균 대비 차별화 포인트 필요"
            )

        return "\n".join(lines)

    # ============================================================
    # 유틸리티
    # ============================================================

    @staticmethod
    def _normalize_name(name: str) -> str:
        """제품명 정규화 (비교용)"""
        if not name:
            return ""
        name = str(name).lower()
        # 특수문자, 공백 제거
        name = re.sub(r"[^\w가-힣]", "", name)
        return name

    @staticmethod
    def _name_similarity(a: str, b: str) -> float:
        """두 문자열의 유사도 (0~1)"""
        if not a or not b:
            return 0.0
        # 짧은 문자열이 긴 문자열에 포함되는지 확인
        if a in b or b in a:
            return 0.8
        # 공통 글자 비율
        set_a, set_b = set(a), set(b)
        if not set_a or not set_b:
            return 0.0
        intersection = len(set_a & set_b)
        union = len(set_a | set_b)
        return intersection / union

    @staticmethod
    def _classify_product_type(product_name: str) -> str:
        """제품명으로 타입 분류"""
        name = str(product_name).lower()

        # 온도 상태
        temp = "냉동" if "냉동" in name else ("냉장" if "냉장" in name else "")

        # 제품 종류
        categories = {
            "돈까스": ["돈까스", "돈가스", "돈카츠", "포크커틀릿"],
            "소시지": ["소시지", "sausage"],
            "비엔나": ["비엔나", "비너", "vienna"],
            "후랑크": ["후랑크", "프랑크", "frank", "frankfurter"],
            "햄": ["햄", "ham"],
            "너겟": ["너겟", "nugget"],
            "탕수육": ["탕수육"],
            "만두": ["만두"],
            "치킨": ["치킨", "닭", "chicken"],
            "떡갈비": ["떡갈비"],
        }

        for cat, keywords in categories.items():
            if any(kw in name for kw in keywords):
                return f"{temp}{cat}".strip() if temp else cat

        return temp + "기타" if temp else "기타"
