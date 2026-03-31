# 사이트 셀렉터 설정 가이드

처음 실행 시 사이트 HTML 구조에 따라 `config/settings.py`의 셀렉터를 맞춰야 합니다.

---

## 셀렉터 확인 방법

1. **브라우저 개발자 도구** (F12) 열기
2. Elements 탭에서 원하는 요소 우클릭 → "Copy selector" 또는 "Copy XPath"
3. `config/settings.py`에서 해당 항목 수정

---

## 헤드리스 OFF로 디버깅

`app.py` 사이드바에서 "백그라운드 실행" 토글을 **OFF**로 설정하면
브라우저 창이 열리면서 수집 과정을 실시간으로 확인할 수 있습니다.

---

## 오류 발생 시 스크린샷 확인

스크래핑 오류 시 프로젝트 루트에 다음 파일이 생성됩니다:
- `debug_foodnbid_stats.png` - 푸드앤비드 통계 페이지
- `debug_foodnbid_error.png` - 푸드앤비드 오류 시점
- `debug_bluesys_error.png` - 블루시스마켓 오류 시점

---

## 푸드앤비드 주요 수정 항목 (config/settings.py)

```python
FOODNBID = {
    "stats_url": "실제 매출통계 페이지 URL",  # ← 로그인 후 매출 통계 메뉴 URL 확인
    "search": {
        "keyword_field": "input[name='키워드필드명']",   # ← 실제 필드명
        "start_date": "input[name='시작일필드명']",
        "end_date": "input[name='종료일필드명']",
        "region_select": "select[name='지역필드명']",
        "region_value": "수도권 옵션 값",               # ← select option value 확인
    },
    "result": {
        "table": "실제 테이블 CSS 셀렉터",
        "columns": {
            "product_name": 0,  # ← 열 순서 (0부터 시작)
            "sales_amount": 2,
            ...
        }
    }
}
```

## 블루시스마켓 주요 수정 항목

```python
BLUESYS = {
    "search_url": "실제 상품검색 URL",
    "product_detail": {
        "school_price": {
            "label_keywords": ["학교매입가", "학교가격"],  # ← 실제 라벨 텍스트
        },
        ...
    }
}
```

---

## 자주 있는 패턴

| 사이트 패턴 | 셀렉터 예시 |
|---|---|
| 로그인 ID 필드 | `input[name='userId']`, `#id`, `#loginId` |
| 날짜 입력 | `input[name='stDate']`, `.datepicker` |
| 검색 버튼 | `.btn-search`, `button[onclick*='search']` |
| 테이블 | `#resultTable`, `table.list`, `.board-list table` |
| 상품 라벨-값 | `th:has-text('학교매입가') + td` |
