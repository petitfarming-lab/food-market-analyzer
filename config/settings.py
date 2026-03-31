"""
사이트별 URL, 셀렉터, 설정값 관리
실제 확인된 URL 및 필드명 기준
"""

# ============================================================
# 푸드앤비드 (FoodNBid) 설정  https://foodnbid.com
# ============================================================
FOODNBID = {
    "base_url": "https://foodnbid.com",
    "login_url": "https://foodnbid.com",           # 메인 페이지에 로그인 팝업
    "stats_url": "https://foodnbid.com/agent/sellingAgentResult.do",

    # 로그인 셀렉터 (메인 페이지 팝업 로그인)
    "login": {
        "id_field": "#cm_id, input[name='cm_id']",
        "pw_field": "#cm_pwd, input[name='cm_pwd']",
        "submit_btn": "#loginBtn, .loginBtn, button[onclick*='fn_login']",
        # 로그인 팝업을 여는 버튼 (메인 페이지)
        "open_popup_btn": "a[onclick*='fn_usrLogin'], .login-btn, a:has-text('로그인')",
    },

    # 검색/필터 셀렉터
    "search": {
        "keyword_field": "#s_school_nm, input[name='s_school_nm']",
        "start_date": "#sDate, input[name='sDate'], #sDate1",
        "end_date": "#eDate, input[name='eDate'], #eDate1",
        "region_select": "#s_charge_area, select[name='s_charge_area'], select[name='area']",
        "region_value": "",  # 로그인 후 직접 확인 필요
        "search_btn": "button[onclick*='fn_bidSearch'], .btn-search, input[type='submit']",
    },

    # 결과 테이블 셀렉터
    "result": {
        "table": "table",
        "rows": "tbody tr",
        "columns": {
            "product_name": 0,
            "category": 1,
            "sales_amount": 2,
            "quantity": 3,
            "region": 4,
        },
        "pagination": ".pagination a, .paging a",
        "next_page": ".pagination .next, a:has-text('다음')",
    },

    # 수도권 지역 코드/키워드
    "sudogwon_regions": ["서울", "경기", "인천", "수도권"],
}

# ============================================================
# 블루시스마켓 설정  https://market.bluesis.com
# ============================================================
BLUESYS = {
    "base_url": "https://market.bluesis.com",
    "login_url": "https://market.bluesis.com/web/pc/login.php",
    "search_url": "https://market.bluesis.com/web/pc/product.php",

    # 로그인 셀렉터 (AJAX 기반)
    "login": {
        "id_field": "#blue_uid, input[name='uid']",
        "pw_field": "#pwd, input[name='pwd']",
        "submit_btn": "button[onclick*='id_login'], .login-btn, a:has-text('로그인'), button:has-text('로그인')",
    },

    # 검색 셀렉터
    "search": {
        "keyword_field": "input[name='keyword'], input[name='search'], .search-input, input[type='search']",
        "search_btn": "button[type='submit'], .btn-search, button:has-text('검색'), a:has-text('검색')",
        "category_select": "select[name='category'], select[name='fn_cate_2']",
    },

    # 상품 목록 셀렉터
    "product_list": {
        "items": ".goods-item, .product-item, li.item, .goods_list li",
        "detail_link": "a[href*='product'], a[href*='goods'], a[href*='wr_id']",
        "pagination": ".pagination a, .paging a",
        "next_page": ".pagination .next, a:has-text('다음'), a[aria-label='다음']",
    },

    # 상품 상세 페이지 셀렉터
    "product_detail": {
        "product_name": {
            "selectors": ["h1", "h2", ".goods-name", ".product-title", ".subject"],
            "label_keywords": ["상품명", "제품명"],
        },
        "school_price": {
            "selectors": [".school-price", ".price-school", "[class*='school']"],
            "label_keywords": ["학교매입가", "학교가", "매입가", "학교납품가"],
        },
        "weight": {
            "selectors": [".weight", ".volume", "[class*='weight']"],
            "label_keywords": ["함량", "용량", "중량", "내용량", "규격"],
        },
        "tier": {
            "selectors": ["[class*='tier']", "[class*='grade']"],
            "label_keywords": ["TIER", "티어", "등급", "tier"],
        },
        "cooking_method": {
            "selectors": ["[class*='cook']", "[class*='method']"],
            "label_keywords": ["조리법", "조리방법", "가열방법", "취식방법"],
        },
        "manufacturer": {
            "selectors": ["[class*='manufacturer']", "[class*='maker']"],
            "label_keywords": ["제조사", "제조업체", "생산자", "제조원"],
        },
        "brand": {
            "selectors": ["[class*='brand']"],
            "label_keywords": ["브랜드", "상표", "브랜드명"],
        },
    },
}

# ============================================================
# 분석 설정
# ============================================================
ANALYSIS = {
    "competitive_price_percentile": 0.25,
    "top_n_types": 10,
    "sudogwon_keywords": ["서울", "경기", "인천", "수도권"],
}
