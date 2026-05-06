# e-commerce_ETL

# 목표
```
1. Medallion Architecture의 배치 기반  ETL 파이프라인 구축
    - 유저 행동로그 데이터 & 판매 데이터 
2. BI 대시보드를 최종 산출물로 생성 - 자동화 Daily Report
```

# ETL 파이프라인 구조

## Extract
- log_gen.py ( 로컬 실행 )
- 예상 raw데이터 크기 : 50,000row ~
```
- product_master.csv    : 상품 마스터 테이블. 이벤트&판매 로그 데이터 생성 시 참조
- log_gen.py            : 유저 행동로그 + 판매데이터 생성
```

## Transform
- Medallion Architecture
```
- bronze
    - athena query 사용
    - event & sales 각각의 External Table 생성
- silver
    - silver_event.py
    - silver_sales.py
- gold
    - event, sales 분석 테이블 생성
    - silver_to_gold.py
```
## Load
- s3에 단계별 적재
```
- s3/bucket/silver/event & sales/dt=YYYY-MM-DD
    - Parquet 형식 저장
    - 100KB ~ 
```
```
- s3/bucket/gold/
    - event : campaign, device, funnel, item, search
    - sales : category, sales_daily, payment, product, region, user
```

## Analysis
- tableau와 s3를 연동, daily 판매 분석 대시보드 제작
```
- tableau desktop
    - BI Dashboard

- tableau cloud
    - daily schedule
    - send to target
```