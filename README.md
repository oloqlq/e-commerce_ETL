# e-commerce_ETL

# 목표
```
1. Medallion Architecture의 배치 기반  ETL 파이프라인 구축
    - 이벤트 데이터 & 판매 데이터 
2. BI 대시보드를 최종 산출물로 생성 - 자동화 Daily Report
```

# ETL 파이프라인 구조

## Extract
- faker 라이브러리를 활용한 가짜 로그 생성 ( 로컬 실행 )
- 예상 raw데이터 크기 : n x 1000 row
```
- product_master.csv    : 상품 마스터 테이블. 이벤트&판매 로그 데이터 생성 시 참조
- event_log_gen.py      : 유저 행동 (이벤트) 로그데이터 생성
- sales_log_gen.py      : 판매 로그데이터 생성
```

## Transform
- Medallion Architecture
```
- silver
    - silver_event.py
    - silver_sales.py
- gold
    - gold_event.py
    - gold_sales.py
```
## Load
- s3에 단계별 적재
```
- s3/bucket/silver/event & sales
    - silver_event_tbl.parquet,
    - silver_sales_tbl.parquet
```
```
- s3/bucket/gold/event & sales
    - gold_event_tbl.parquet
    - gold_sales_tbl.parquet
```

## Analysis
- tableau와 s3를 연동, daily 판매 분석 대시보드 제작