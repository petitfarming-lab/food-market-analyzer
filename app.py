"""
식품 시장 분석 대시보드
푸드앤비드 수도권 매출 + 블루시스마켓 상품 데이터 통합 분석
"""

import asyncio
import logging
import os
import sys
import threading
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# 프로젝트 루트를 sys.path에 추가
ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))

import io
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

from exporters.excel_exporter import create_excel_report
from processors.data_analyzer import DataAnalyzer
from scrapers.bluesys_scraper import BluesysMarketScraper
from scrapers.foodnbid_scraper import FoodNBidScraper
from scrapers.foodspring_scraper import FoodspringScraper

# ============================================================
# 로깅 설정
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# ============================================================
# 페이지 설정
# ============================================================
st.set_page_config(
    page_title="식품 시장 분석 대시보드",
    page_icon="🍖",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ============================================================
# 커스텀 CSS
# ============================================================
st.markdown("""
<style>
    .main-title {
        font-size: 2rem;
        font-weight: 800;
        color: #1F4E79;
        margin-bottom: 0.2rem;
    }
    .sub-title {
        font-size: 1rem;
        color: #595959;
        margin-bottom: 1.5rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #1F4E79, #2E86AB);
        color: white;
        border-radius: 12px;
        padding: 1rem 1.5rem;
        text-align: center;
    }
    .metric-label { font-size: 0.85rem; opacity: 0.85; }
    .metric-value { font-size: 1.6rem; font-weight: 800; }
    .insight-box {
        background: #FFFBEA;
        border-left: 5px solid #F4A300;
        border-radius: 4px;
        padding: 1rem 1.5rem;
        margin: 1rem 0;
        font-size: 0.95rem;
    }
    .stProgress > div > div > div { background-color: #1F4E79; }
    div[data-testid="stSidebarContent"] { background: #F0F4FA; }
</style>
""", unsafe_allow_html=True)


# ============================================================
# 세션 상태 초기화
# ============================================================
def init_session():
    defaults = {
        "sales_df": pd.DataFrame(),
        "product_df": pd.DataFrame(),
        "analysis_done": False,
        "competitive": {},
        "sales_type_df": pd.DataFrame(),
        "top_products_df": pd.DataFrame(),
        "regional_df": pd.DataFrame(),
        "excel_bytes": None,
        "last_keyword": "",
        "last_dates": ("", ""),
        # 식봄 전용
        "foodspring_df": pd.DataFrame(),
        "foodspring_excel_bytes": None,
        "foodspring_done": False,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


init_session()


# ============================================================
# 사이드바 - 설정
# ============================================================
with st.sidebar:
    st.markdown("## ⚙️ 설정")
    st.divider()

    # --- 로그인 정보 ---
    st.markdown("### 🔐 로그인 정보")
    with st.expander("푸드앤비드 계정", expanded=True):
        fnb_id = st.text_input("아이디", key="fnb_id", placeholder="아이디 입력")
        fnb_pw = st.text_input("비밀번호", key="fnb_pw", type="password", placeholder="비밀번호 입력")

    with st.expander("블루시스마켓 계정", expanded=True):
        bls_id = st.text_input("아이디", key="bls_id", placeholder="아이디 입력")
        bls_pw = st.text_input("비밀번호", key="bls_pw", type="password", placeholder="비밀번호 입력")

    st.divider()

    # --- 검색 조건 ---
    st.markdown("### 🔍 검색 조건")
    keyword = st.text_input(
        "제품 키워드",
        placeholder="예: 돈까스, 소시지, 비엔나, 후랑크",
        help="검색할 제품의 대표 키워드를 입력하세요"
    )

    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input(
            "시작일",
            value=date.today() - timedelta(days=365),
            format="YYYY-MM-DD",
        )
    with col2:
        end_date = st.date_input(
            "종료일",
            value=date.today(),
            format="YYYY-MM-DD",
        )

    max_products = st.slider(
        "블루시스마켓 최대 수집 상품 수",
        min_value=10,
        max_value=200,
        value=50,
        step=10,
        help="많을수록 정확하지만 시간이 오래 걸립니다"
    )

    headless_mode = st.toggle(
        "백그라운드 실행 (헤드리스)",
        value=True,
        help="OFF 시 브라우저 창이 보입니다 (디버깅용)"
    )

    st.divider()

    # --- 실행 버튼 ---
    run_btn = st.button(
        "🚀 데이터 수집 & 분석 시작",
        type="primary",
        use_container_width=True,
        disabled=not (keyword and fnb_id and fnb_pw and bls_id and bls_pw),
    )

    if not keyword:
        st.caption("⚠️ 키워드를 입력해주세요")
    if not (fnb_id and fnb_pw):
        st.caption("⚠️ 푸드앤비드 계정을 입력해주세요")
    if not (bls_id and bls_pw):
        st.caption("⚠️ 블루시스마켓 계정을 입력해주세요")


# ============================================================
# 메인 콘텐츠
# ============================================================
st.markdown('<div class="main-title">🍖 식품 시장 분석 대시보드</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="sub-title">푸드앤비드 수도권 매출 + 블루시스마켓 상품 데이터 통합 분석</div>',
    unsafe_allow_html=True
)

# ============================================================
# 메인 탭 분리
# ============================================================
main_tab1, main_tab2 = st.tabs(["📊 시장 분석 (푸드앤비드 + 블루시스)", "🛒 식봄 가격 조회"])


# ============================================================
# 데이터 수집 실행 (공통 함수)
# ============================================================
async def run_scraping(keyword, start_date, end_date, max_products, headless):
    """비동기 스크래핑 실행"""
    results = {"sales_df": pd.DataFrame(), "product_df": pd.DataFrame()}

    progress_placeholder = st.empty()
    status_placeholder = st.empty()

    def update_progress(msg: str):
        status_placeholder.info(f"⏳ {msg}")

    # 푸드앤비드 수집
    try:
        update_progress("푸드앤비드 접속 중...")
        async with FoodNBidScraper(fnb_id, fnb_pw, headless=headless) as scraper:
            results["sales_df"] = await scraper.get_sales_data(
                keyword=keyword,
                start_date=str(start_date),
                end_date=str(end_date),
                progress_callback=update_progress,
            )
    except Exception as e:
        st.warning(f"⚠️ 푸드앤비드 수집 오류: {e}\n스크린샷: debug_foodnbid_error.png 확인")
        logger.error(f"FoodNBid 오류: {e}")

    # 블루시스마켓 수집
    try:
        update_progress("블루시스마켓 접속 중...")
        async with BluesysMarketScraper(bls_id, bls_pw, headless=headless) as scraper:
            results["product_df"] = await scraper.get_product_data(
                keyword=keyword,
                max_products=max_products,
                progress_callback=update_progress,
            )
    except Exception as e:
        st.warning(f"⚠️ 블루시스마켓 수집 오류: {e}\n스크린샷: debug_bluesys_error.png 확인")
        logger.error(f"Bluesys 오류: {e}")

    status_placeholder.success("✅ 데이터 수집 완료!")
    return results


# ============================================================
# 식봄 스크래핑 함수
# ============================================================
async def run_foodspring_scraping(df_input: pd.DataFrame, headless: bool):
    result_holder = {"df": pd.DataFrame()}
    status = st.empty()

    def update_progress(msg: str):
        status.info(f"⏳ {msg}")

    try:
        async with FoodspringScraper(headless=headless) as scraper:
            result_holder["df"] = await scraper.search_products(
                df_input=df_input,
                progress_callback=update_progress,
            )
        status.success("✅ 식봄 검색 완료!")
    except Exception as e:
        logger.error(f"식봄 스크래핑 오류: {e}")
        status.error(f"오류 발생: {e}")

    return result_holder["df"]


def build_foodspring_excel(df: pd.DataFrame) -> bytes:
    """식봄 검색 결과 엑셀 생성 후 bytes 반환"""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "식봄_검색결과"

    header_font = Font(name="맑은 고딕", bold=True, color="FFFFFF", size=11)
    header_fill = PatternFill("solid", start_color="2F75B6")
    cell_font = Font(name="맑은 고딕", size=10)
    found_fill = PatternFill("solid", start_color="E8F4FD")
    notfound_fill = PatternFill("solid", start_color="FFF2CC")
    center = Alignment(horizontal="center", vertical="center", wrap_text=True)
    left = Alignment(horizontal="left", vertical="center", wrap_text=True)
    thin = Side(style="thin", color="BFBFBF")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)

    headers = ["No.", "입력_제품명", "입력_제조사", "순위", "식봄_판매제품명", "식봄_판매가", "상품URL", "검색상태"]
    col_widths = [5, 28, 18, 6, 42, 14, 50, 10]

    for c_idx, (h, w) in enumerate(zip(headers, col_widths), 1):
        cell = ws.cell(row=1, column=c_idx, value=h)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = center
        cell.border = border
        ws.column_dimensions[get_column_letter(c_idx)].width = w

    ws.row_dimensions[1].height = 22

    for _, row in df.iterrows():
        r_idx = ws.max_row + 1
        status = row.get("검색상태", "")
        row_fill = found_fill if status == "발견" else notfound_fill
        data = [
            row.get("No", ""),
            row.get("제품명", ""),
            row.get("제조사", ""),
            row.get("순위", "-"),
            row.get("판매제품명", ""),
            row.get("판매가", "-"),
            row.get("상품URL", ""),
            status,
        ]
        for c_idx, val in enumerate(data, 1):
            cell = ws.cell(row=r_idx, column=c_idx, value=val)
            cell.font = cell_font
            cell.fill = row_fill
            cell.border = border
            cell.alignment = center if c_idx in (1, 4, 6, 8) else left

    # 같은 No. 묶음 병합
    if not df.empty and "No" in df.columns:
        groups = df.groupby("No", sort=False)
        for no, grp in groups:
            if len(grp) > 1:
                start_r = grp.index[0] + 2
                end_r = grp.index[-1] + 2
                for mc in (1, 2, 3):
                    ws.merge_cells(start_row=start_r, start_column=mc, end_row=end_r, end_column=mc)
                    ws.cell(start_r, mc).alignment = center

    ws.freeze_panes = "A2"

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.read()


# ============================================================
# main_tab1: 기존 시장 분석 UI
# ============================================================
with main_tab1:
    if run_btn:
        with st.spinner("데이터 수집 중... (수분 소요될 수 있습니다)"):
            result_container = {}

            def scrape_in_thread():
                if sys.platform == "win32":
                    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    result_container["results"] = loop.run_until_complete(
                        run_scraping(keyword, start_date, end_date, max_products, headless_mode)
                    )
                except Exception as e:
                    logger.error(f"스크래핑 스레드 오류: {e}")
                    result_container["results"] = {
                        "sales_df": pd.DataFrame(),
                        "product_df": pd.DataFrame(),
                    }
                finally:
                    loop.close()

            t = threading.Thread(target=scrape_in_thread, daemon=True)
            t.start()
            t.join()

            results = result_container.get(
                "results",
                {"sales_df": pd.DataFrame(), "product_df": pd.DataFrame()}
            )

        st.session_state["sales_df"] = results["sales_df"]
        st.session_state["product_df"] = results["product_df"]
        st.session_state["last_keyword"] = keyword
        st.session_state["last_dates"] = (str(start_date), str(end_date))

        analyzer = DataAnalyzer()
        if not results["sales_df"].empty:
            st.session_state["sales_type_df"] = analyzer.analyze_sales_by_type(results["sales_df"])
            st.session_state["top_products_df"] = analyzer.get_top_products(results["sales_df"])
            st.session_state["regional_df"] = analyzer.get_regional_breakdown(results["sales_df"])
        else:
            st.session_state["sales_type_df"] = pd.DataFrame()
            st.session_state["top_products_df"] = pd.DataFrame()
            st.session_state["regional_df"] = pd.DataFrame()

        st.session_state["competitive"] = analyzer.analyze_competitive_spec(
            results["product_df"], results["sales_df"]
        )

        try:
            excel_bytes = create_excel_report(
                sales_df=results["sales_df"],
                product_df=results["product_df"],
                sales_type_df=st.session_state["sales_type_df"],
                top_products_df=st.session_state["top_products_df"],
                regional_df=st.session_state["regional_df"],
                competitive_analysis=st.session_state["competitive"],
                keyword=keyword,
                start_date=str(start_date),
                end_date=str(end_date),
            )
            st.session_state["excel_bytes"] = excel_bytes
        except Exception as e:
            st.warning(f"엑셀 생성 오류: {e}")
            logger.error(f"Excel 오류: {e}")

        st.session_state["analysis_done"] = True
        st.rerun()

    if st.session_state["analysis_done"]:
        kw = st.session_state["last_keyword"]
        s_date, e_date = st.session_state["last_dates"]
        sales_df = st.session_state["sales_df"]
        product_df = st.session_state["product_df"]
        sales_type_df = st.session_state["sales_type_df"]
        top_products_df = st.session_state["top_products_df"]
        regional_df = st.session_state["regional_df"]
        competitive = st.session_state["competitive"]

        st.info(f"**분석 대상**: [{kw}] | **기간**: {s_date} ~ {e_date} | **수도권 기준**")

        kpi1, kpi2, kpi3, kpi4 = st.columns(4)
        total_sales = int(sales_df["매출금액"].sum()) if not sales_df.empty and "매출금액" in sales_df.columns else 0
        total_qty = int(sales_df["수량"].sum()) if not sales_df.empty and "수량" in sales_df.columns else 0
        total_products = len(product_df) if not product_df.empty else 0
        median_price = competitive.get("median_price", 0)

        with kpi1:
            st.metric("💰 수도권 총매출", f"{total_sales:,}원" if total_sales else "수집중")
        with kpi2:
            st.metric("📦 총 판매 수량", f"{total_qty:,}개" if total_qty else "수집중")
        with kpi3:
            st.metric("🛒 분석 상품 수", f"{total_products:,}개")
        with kpi4:
            st.metric("💲 학교매입가 중앙값", f"{median_price:,}원" if median_price else "-")

        st.divider()

        tab1, tab2, tab3, tab4, tab5 = st.tabs([
            "📊 매출 타입 분석", "🏆 TOP 제품", "🗺 지역별 분석", "🛒 상품 상세", "💡 경쟁력 분석",
        ])

        with tab1:
            if not sales_type_df.empty:
                col_chart, col_table = st.columns([1, 1])
                with col_chart:
                    st.subheader("제품 타입별 매출 비중")
                    fig_pie = px.pie(
                        sales_type_df, names="제품타입", values="총매출금액",
                        color_discrete_sequence=px.colors.qualitative.Set2, hole=0.4,
                    )
                    fig_pie.update_traces(textposition="inside", textinfo="percent+label")
                    fig_pie.update_layout(height=380, margin=dict(t=20, b=20))
                    st.plotly_chart(fig_pie, use_container_width=True)
                with col_table:
                    st.subheader("제품 타입별 매출금액")
                    fig_bar = px.bar(
                        sales_type_df.sort_values("총매출금액"), x="총매출금액", y="제품타입",
                        orientation="h", color="총매출금액", color_continuous_scale="Blues", text="총매출금액",
                    )
                    fig_bar.update_traces(texttemplate="%{text:,.0f}원", textposition="outside")
                    fig_bar.update_layout(height=380, margin=dict(t=20, b=20), showlegend=False)
                    st.plotly_chart(fig_bar, use_container_width=True)
                st.subheader("타입별 상세 수치")
                st.dataframe(
                    sales_type_df.style.format({
                        "총매출금액": "{:,}원", "총수량": "{:,}개",
                        "평균단가": "{:,}원", "매출비중(%)": "{:.2f}%",
                    }),
                    use_container_width=True,
                )
            else:
                st.info("푸드앤비드 매출 데이터를 수집하면 타입별 분석이 표시됩니다.")

        with tab2:
            if not top_products_df.empty:
                st.subheader(f"매출 상위 제품 TOP {len(top_products_df)}")
                fig_top = px.bar(
                    top_products_df.head(15).sort_values("총매출금액"), x="총매출금액", y="제품명",
                    orientation="h", color="평균단가", color_continuous_scale="RdYlGn_r",
                    title="매출 TOP 15 제품 (색상: 평균단가)",
                )
                fig_top.update_layout(height=480, margin=dict(t=40, b=20))
                st.plotly_chart(fig_top, use_container_width=True)
                st.dataframe(
                    top_products_df.style.format({
                        "총매출금액": "{:,}원", "총수량": "{:,}개", "평균단가": "{:,.0f}원",
                    }),
                    use_container_width=True,
                )
            else:
                st.info("푸드앤비드 매출 데이터를 수집하면 TOP 제품이 표시됩니다.")

        with tab3:
            if not regional_df.empty:
                col_map, col_tbl = st.columns([1, 1])
                with col_map:
                    st.subheader("지역별 매출금액")
                    fig_region = px.bar(
                        regional_df, x="지역", y="총매출금액", color="총매출금액",
                        color_continuous_scale="Blues", text="총매출금액",
                    )
                    fig_region.update_traces(texttemplate="%{text:,.0f}원", textposition="outside")
                    fig_region.update_layout(height=360, margin=dict(t=20, b=20))
                    st.plotly_chart(fig_region, use_container_width=True)
                with col_tbl:
                    st.subheader("지역별 상세")
                    st.dataframe(
                        regional_df.style.format({"총매출금액": "{:,}원", "총수량": "{:,}개"}),
                        use_container_width=True,
                    )
            else:
                st.info("지역 데이터가 없거나 수집이 필요합니다.")

        with tab4:
            if not product_df.empty:
                st.subheader(f"블루시스마켓 상품 목록 ({len(product_df)}개)")
                f_col1, f_col2, f_col3 = st.columns(3)
                with f_col1:
                    tier_filter = st.multiselect(
                        "TIER 필터",
                        options=product_df["TIER"].dropna().unique().tolist() if "TIER" in product_df.columns else [],
                    )
                with f_col2:
                    cook_filter = st.multiselect(
                        "조리법 필터",
                        options=product_df["조리법"].dropna().unique().tolist() if "조리법" in product_df.columns else [],
                    )
                with f_col3:
                    if "학교매입가_숫자" in product_df.columns:
                        price_range = st.slider(
                            "학교매입가 범위", min_value=0,
                            max_value=int(product_df["학교매입가_숫자"].max() or 50000),
                            value=(0, int(product_df["학교매입가_숫자"].max() or 50000)),
                        )
                    else:
                        price_range = (0, 999999)
                filtered_df = product_df.copy()
                if tier_filter and "TIER" in filtered_df.columns:
                    filtered_df = filtered_df[filtered_df["TIER"].isin(tier_filter)]
                if cook_filter and "조리법" in filtered_df.columns:
                    filtered_df = filtered_df[filtered_df["조리법"].isin(cook_filter)]
                if "학교매입가_숫자" in filtered_df.columns:
                    filtered_df = filtered_df[filtered_df["학교매입가_숫자"].between(price_range[0], price_range[1])]
                display_cols = ["제품명", "학교매입가", "함량", "TIER", "조리법", "제조사", "브랜드"]
                disp_cols = [c for c in display_cols if c in filtered_df.columns]
                st.dataframe(filtered_df[disp_cols], use_container_width=True, height=400)
                if "학교매입가_숫자" in filtered_df.columns:
                    price_data = filtered_df["학교매입가_숫자"].replace(0, pd.NA).dropna()
                    if not price_data.empty:
                        fig_hist = px.histogram(
                            price_data, nbins=20, title="학교매입가 분포",
                            color_discrete_sequence=["#1F4E79"],
                        )
                        fig_hist.update_layout(height=280, margin=dict(t=40, b=20))
                        st.plotly_chart(fig_hist, use_container_width=True)
            else:
                st.info("블루시스마켓 상품 데이터를 수집하면 표시됩니다.")

        with tab5:
            insight_text = competitive.get("insight_text", "")
            if insight_text:
                st.markdown(f'<div class="insight-box">{insight_text}</div>', unsafe_allow_html=True)
            col_a, col_b = st.columns(2)
            with col_a:
                tier_dist = competitive.get("tier_distribution", {})
                if tier_dist:
                    st.subheader("TIER 분포")
                    fig_tier = px.pie(
                        names=list(tier_dist.keys()), values=list(tier_dist.values()),
                        color_discrete_sequence=px.colors.qualitative.Pastel, hole=0.35,
                    )
                    fig_tier.update_layout(height=300, margin=dict(t=10, b=10))
                    st.plotly_chart(fig_tier, use_container_width=True)
            with col_b:
                cook_dist = competitive.get("cooking_method_distribution", {})
                if cook_dist:
                    st.subheader("조리법 분포")
                    fig_cook = px.bar(
                        x=list(cook_dist.keys()), y=list(cook_dist.values()),
                        color_discrete_sequence=["#2E86AB"],
                    )
                    fig_cook.update_layout(height=300, margin=dict(t=10, b=10))
                    st.plotly_chart(fig_cook, use_container_width=True)
            comp_products = competitive.get("competitive_products", pd.DataFrame())
            if not comp_products.empty:
                st.subheader("매출 TOP & 상품 스펙 교차 분석")
                st.dataframe(comp_products, use_container_width=True)

        st.divider()
        dl_col1, dl_col2, dl_col3 = st.columns([1, 2, 1])
        with dl_col2:
            if st.session_state.get("excel_bytes"):
                filename = f"식품시장분석_{kw}_{s_date}_{e_date}.xlsx"
                st.download_button(
                    label="📥 엑셀 보고서 다운로드",
                    data=st.session_state["excel_bytes"],
                    file_name=filename,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                    type="primary",
                )
                st.caption(f"파일명: {filename}")
    else:
        st.markdown("""
        ### 사용 방법

        1. **왼쪽 사이드바**에 푸드앤비드 / 블루시스마켓 로그인 정보를 입력합니다.
        2. **제품 키워드**를 입력합니다. (예: `돈까스`, `소시지`, `비엔나`, `후랑크`)
        3. **조회 기간**을 선택합니다.
        4. **데이터 수집 & 분석 시작** 버튼을 클릭합니다.

        ---

        ### 분석 결과
        | 탭 | 내용 |
        |---|---|
        | 📊 매출 타입 분석 | 제품 타입별 수도권 매출 규모 및 비중 |
        | 🏆 TOP 제품 | 매출 상위 20개 제품 |
        | 🗺 지역별 분석 | 서울/경기/인천 지역별 드릴다운 |
        | 🛒 상품 상세 | 블루시스마켓 상품 목록 및 필터 |
        | 💡 경쟁력 분석 | 가격/함량/TIER/조리법 기반 기획 제안 |

        ---

        > **참고**: 처음 실행 시 브라우저 자동화로 각 사이트에 로그인 후 데이터를 수집합니다.
        """)


# ============================================================
# main_tab2: 식봄 가격 조회 UI
# ============================================================
with main_tab2:
    st.markdown("### 🛒 식봄(foodspring.co.kr) 제품 가격 조회")
    st.info("아래 표에 **엑셀에서 복사(Ctrl+C)한 데이터를 바로 붙여넣기(Ctrl+V)** 하세요. 직접 입력도 가능합니다.")

    # ── 입력 테이블 초기화 ──
    if "fs_input_df" not in st.session_state:
        st.session_state["fs_input_df"] = pd.DataFrame(
            {"제품명": [""] * 10, "제조사": [""] * 10}
        )

    fs_top_col1, fs_top_col2 = st.columns([4, 1])
    with fs_top_col2:
        if st.button("🗑️ 초기화", key="fs_clear", use_container_width=True):
            st.session_state["fs_input_df"] = pd.DataFrame(
                {"제품명": [""] * 10, "제조사": [""] * 10}
            )
            st.session_state["foodspring_done"] = False
            st.session_state["foodspring_df"] = pd.DataFrame()
            st.rerun()
        fs_headless = st.toggle("백그라운드 실행", value=True, key="fs_headless",
                                help="OFF 시 브라우저 창이 보입니다")

    with fs_top_col1:
        st.caption("📋 엑셀에서 복사 → 첫 번째 셀 클릭 → Ctrl+V 로 붙여넣기 | 행 추가: 표 하단 + 버튼")

    # ── data_editor: 붙여넣기/직접입력 가능한 표 ──
    edited_df = st.data_editor(
        st.session_state["fs_input_df"],
        num_rows="dynamic",
        use_container_width=True,
        height=320,
        key="fs_editor",
        column_config={
            "제품명": st.column_config.TextColumn("제품명 ✱ (필수)", width="large"),
            "제조사": st.column_config.TextColumn("제조사 (선택)", width="medium"),
        },
    )

    # 유효 행만 추출
    df_input = edited_df.copy()
    df_input.columns = df_input.columns.str.strip()
    df_input = df_input.fillna("").astype(str)
    df_input = df_input[df_input["제품명"].str.strip() != ""].reset_index(drop=True)

    valid_count = len(df_input)
    st.caption(f"유효 제품 수: **{valid_count}개**")

    fs_run_btn = st.button(
        f"🔍 식봄 가격 검색 시작  ({valid_count}개)",
        type="primary",
        disabled=(valid_count == 0),
        key="fs_run",
    )

    if fs_run_btn:
        # 현재 입력 저장
        st.session_state["fs_input_df"] = edited_df
        st.session_state["foodspring_done"] = False

        with st.spinner("식봄에서 제품 검색 중... 잠시 기다려 주세요."):
            fs_result_container = {}

            def fs_scrape_thread():
                if sys.platform == "win32":
                    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    fs_result_container["df"] = loop.run_until_complete(
                        run_foodspring_scraping(df_input, fs_headless)
                    )
                except Exception as e:
                    logger.error(f"식봄 스레드 오류: {e}")
                    fs_result_container["df"] = pd.DataFrame()
                finally:
                    loop.close()

            fs_t = threading.Thread(target=fs_scrape_thread, daemon=True)
            fs_t.start()
            fs_t.join()

        fs_df = fs_result_container.get("df", pd.DataFrame())
        st.session_state["foodspring_df"] = fs_df
        st.session_state["foodspring_done"] = True

        if not fs_df.empty:
            try:
                st.session_state["foodspring_excel_bytes"] = build_foodspring_excel(fs_df)
            except Exception as e:
                logger.error(f"식봄 엑셀 생성 오류: {e}")

        st.rerun()

    # ── 결과 표시 ──
    if st.session_state.get("foodspring_done") and not st.session_state["foodspring_df"].empty:
        fs_df = st.session_state["foodspring_df"]
        total = fs_df["No"].nunique() if "No" in fs_df.columns else len(fs_df)
        found = fs_df[fs_df["검색상태"] == "발견"]["No"].nunique() if "검색상태" in fs_df.columns else 0

        st.divider()
        st.markdown("#### 검색 결과")

        m1, m2, m3 = st.columns(3)
        m1.metric("총 검색 제품", f"{total}개")
        m2.metric("✅ 발견", f"{found}개")
        m3.metric("❌ 미발견", f"{total - found}개")

        st.divider()

        display_cols = ["No", "제품명", "제조사", "순위", "판매제품명", "판매가", "검색상태"]
        show_cols = [c for c in display_cols if c in fs_df.columns]

        def highlight_status(row):
            if row.get("검색상태") == "미발견":
                return ["background-color: #FFF9C4"] * len(row)
            return [""] * len(row)

        st.dataframe(
            fs_df[show_cols].style.apply(highlight_status, axis=1),
            use_container_width=True,
            height=450,
        )

        if st.session_state.get("foodspring_excel_bytes"):
            from datetime import datetime as _dt
            fname = f"식봄_가격조회_{_dt.now().strftime('%Y%m%d_%H%M')}.xlsx"
            dl_c1, dl_c2, dl_c3 = st.columns([1, 2, 1])
            with dl_c2:
                st.download_button(
                    label="📥 엑셀 다운로드",
                    data=st.session_state["foodspring_excel_bytes"],
                    file_name=fname,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    type="primary",
                    use_container_width=True,
                )
