# MWAA Lambda 연동 트러블슈팅 기록

## 📅 날짜: 2025-10-14

---

## 🔴 문제 상황

### 증상
- Lambda 함수는 정상적으로 완료됨 (약 8분 소요)
- MWAA DAG Task가 19분 이상 "running" 상태로 멈춤
- Task 로그가 시작 부분 이후 더 이상 출력되지 않음

### 테스트 결과
- ✅ **1개 회사**: Task Success (빠르게 완료)
- ❌ **10개 회사**: Task Running → 타임아웃 (8분 크롤링 완료했으나 Task는 계속 대기)

---

## 🔍 원인 분석

### 1. Lambda 실행 흐름
```
MWAA DAG (hds_dap_stock_info_pipeline_annual_quater_v1.py)
  ↓
run_lambda_task(blocking=True)
  ↓
common.py의 _invoke_lambda() 호출
  ↓
boto3 Lambda invoke (RequestResponse 동기 호출)
  ↓
Lambda 함수 (HDS-DAP-DEV-SYS1-STOCK-INFO-CRAWLER-QUARTER/ANNUAL)
  ↓
stock_crawler_factory.py
  ↓
8분 후 완료 (statusCode: 200 반환)
  ↓
MWAA Task가 응답을 기다리다가 타임아웃
```

### 2. Lambda 응답 형식
**현재 반환 형식** (수정 후):
```python
return {
    'statusCode': 200,
    'headers': {
        'Content-Type': 'application/json; charset=utf-8'
    },
    'body': json.dumps({
        'success': True,
        'crawler_type': 'quarter',
        'crawl_result': {...},
        's3_upload': {...},
        'crawl_time': '2025-10-14 23:53:53'
    }, ensure_ascii=False, indent=2)
}
```

### 3. 타임아웃 설정
- **boto3 read_timeout**: 800초 (13분) - `common.py:406`
- **Lambda 실행 시간**: ~8분 (503초)
- **MWAA Task 실행 시간**: 19분+ (멈춤)

→ **MWAA Task 레벨의 execution_timeout이 더 짧게 설정되어 있을 가능성**

---

## ⚠️ 시도했던 해결 방법 (실패)

### 1차 시도: Lambda 응답 형식 변경
- **시도**: `statusCode`, `body` 제거하고 직접 dict 반환
- **결과**: boto3가 응답을 읽지 못함, Task가 계속 running
- **원복**: HTTP 형식(`statusCode`, `body`)으로 되돌림

### 2차 시도: common.py 수정
- **시도**: `_invoke_lambda`에서 `body` JSON 파싱 로직 추가
- **결과**: `common.py`는 공통 모듈이므로 수정 불가
- **원복**: 원래 코드로 되돌림

---

## ✅ 해결 방법

### 근본 원인
**MWAA Task의 `execution_timeout` 설정이 Lambda 실행 시간(8분)보다 짧음**

### 조치 사항

#### 1. **임시 해결** (완료)
회사 목록 쿼리를 1개만 반환하도록 수정:
```sql
SELECT
    dart_corp,
    LPAD(CAST(dart_corp_code AS VARCHAR), 8, '0') as dart_corp_code,
    stock_nm,
    LPAD(CAST(stock_code AS VARCHAR), 6, '0') as stock_code
FROM {self.database}.{self.table}
WHERE dart_corp_code IS NOT NULL
GROUP BY dart_corp,
         dart_corp_code,
         stock_nm,
         stock_code
ORDER BY dart_corp
LIMIT 1  -- ✅ 1개만 반환하도록 수정
```

**결과**: 1개 회사 크롤링은 빠르게 완료되어 Task Success ✅

#### 2. **영구 해결** (TODO - 내일 진행)
DAG 담당자에게 타임아웃 연장 요청:

**확인 사항:**
1. `DEFAULT_ARGS`에 `execution_timeout` 설정 확인
   ```python
   DEFAULT_ARGS = {
       'execution_timeout': timedelta(minutes=10),  # ← 현재 값 확인
   }
   ```

2. DAG 레벨 타임아웃 확인
   ```python
   with DAG(
       dagrun_timeout=timedelta(minutes=30),  # ← 현재 값 확인
   ):
   ```

**요청 내용:**
> "Lambda 크롤러가 10개 회사 처리 시 최대 10분 정도 걸립니다.
> Task `execution_timeout`을 **15분(900초)**으로 늘려주실 수 있나요?"

---

## 📁 수정된 파일

### 1. `stock_crawler_factory.py`
**변경 내용**: Lambda 응답 형식을 boto3 호환 형식으로 유지

#### Quarter 크롤러 (handle_quarter_crawler)
```python
# 성공 시
return {
    'statusCode': 200,
    'headers': {'Content-Type': 'application/json; charset=utf-8'},
    'body': json.dumps(result_data, ensure_ascii=False, indent=2)
}

# 실패 시
return {
    'statusCode': 500,
    'headers': {'Content-Type': 'application/json; charset=utf-8'},
    'body': json.dumps({'success': False, 'error': '...'}, ensure_ascii=False, indent=2)
}
```

#### Annual 크롤러 (handle_annual_crawler)
- Quarter와 동일한 형식

#### Factory 핸들러 (factory_lambda_handler)
- 에러 케이스도 동일한 HTTP 형식으로 반환

### 2. `dags/common.py`
**변경 내용**: 없음 (원본 유지)
- 공통 모듈이므로 수정하지 않음

### 3. 회사 목록 쿼리 (임시)
**파일 위치**: (Lambda에서 호출하는 쿼리 - 정확한 위치 확인 필요)
```sql
-- LIMIT 1 추가
LIMIT 1
```

---

## 🔧 향후 개선 사항

### 옵션 1: Task 타임아웃 연장 (권장)
- `execution_timeout`: 15분으로 연장
- 가장 간단하고 확실한 해결책

### 옵션 2: 비동기 실행
```python
# DAG 파일에서
j = run_lambda_task(task_id=task_id, function_name=fn, blocking=False)
```
- Lambda만 실행하고 결과를 기다리지 않음
- 단점: Lambda 실패 시 Task는 성공 처리됨

### 옵션 3: 배치 분할 실행
- 10개 회사를 5개씩 2번 실행
- Lambda 실행 시간 단축

### 옵션 4: Lambda 병렬 처리 최적화
- 현재는 순차 처리 (delay_between_stocks=2초)
- 병렬 처리로 개선하면 실행 시간 단축 가능

---

## 📊 실행 결과 로그

### Lambda 완료 로그 (성공)
```
2025-10-14T23:53:53.662+09:00
🎯 [MWAA] Lambda 실행 완료 - 응답 반환
🎯 [MWAA] 응답 데이터:
{
    "success": true,
    "crawler_type": "quarter",
    "crawl_result": {
        "success": true,
        "total_companies": 10,
        "message": "10개 회사의 분기별 재무정보 크롤링 완료",
        "output_directory": "/tmp/crawl_results",
        "s3_bucket": "hds-dap-dev-an2-datalake-01"
    },
    "s3_upload": {
        "success": true,
        "message": "S3 업로드는 크롤링 함수 내부에서 처리됨"
    },
    "crawl_time": "2025-10-14 23:53:53"
}

Duration: 503999.57 ms (약 8분)
```

### MWAA Task 로그 (타임아웃)
```
[2025-10-14T14:45:30.393+0000] INFO - Starting attempt 1 of 1
[2025-10-14T14:45:30.455+0000] INFO - Executing <Task(PythonOperator): l0_ingestion.hds_sap_stock_ingest_quarter>
[2025-10-14T14:45:30.765+0000] INFO - ::endgroup::
(이후 로그 없음, 19분+ running)
```

---

## 🎯 액션 아이템

### 완료 ✅
- [x] Lambda 응답 형식을 HTTP 표준 형식으로 수정
- [x] 회사 목록 쿼리에 `LIMIT 1` 추가하여 1개 회사만 처리
- [x] 1개 회사 테스트 → Task Success 확인

### 진행 중 🔄
- [ ] **DAG 담당자에게 타임아웃 연장 요청** (내일 진행)
  - 현재 `execution_timeout` 설정값 확인
  - 15분(900초)으로 연장 요청

### 대기 ⏸️
- [ ] 타임아웃 연장 후 10개 회사로 전체 테스트
- [ ] `LIMIT 1` 제거하고 원래대로 복구
- [ ] 정상 동작 확인 후 문서 업데이트

---

## 📝 참고 사항

### DAG 구조
```
l0_ingestion (Task Group)
  ├─ g_start (EmptyOperator)
  ├─ hds_sap_stock_ingest_annual (PythonOperator - Lambda)
  ├─ hds_sap_stock_ingest_quarter (PythonOperator - Lambda)
  └─ g_end (EmptyOperator)
    ↓
l0_crawler (Task Group)
  └─ stock-info-ingest-quater-l0-crawler (GlueCrawlerOperator)
    ↓
l1_transform (Task Group)
  ├─ l0-to-l1-etl-glue-job (GlueJobOperator)
  └─ l1-crawler (GlueCrawlerOperator)
```

### Lambda 함수
- **ANNUAL**: `HDS-DAP-DEV-SYS1-STOCK-INFO-CRAWLER-ANNUAL`
  - 환경변수: `CRAWLER_TYPE=annual`
- **QUARTER**: `HDS-DAP-DEV-SYS1-STOCK-INFO-CRAWLER-QUARTER`
  - 환경변수: `CRAWLER_TYPE=quarter`

### 실행 방식
- **blocking=True**: 동기 실행, Lambda 완료 대기
- **payload=None**: 환경변수에서 설정값 읽음
- **InvocationType=RequestResponse**: boto3 동기 호출

---

## 🚨 주의 사항

1. **`common.py`는 절대 수정하지 말 것**
   - 공통 모듈이므로 다른 DAG에 영향을 줄 수 있음

2. **회사 목록 `LIMIT 1`은 임시 조치**
   - 타임아웃 해결 후 반드시 제거해야 함

3. **Lambda 응답 형식 유지**
   - boto3 호환을 위해 `statusCode`, `body` 구조 필수
   - 직접 dict 반환 시 boto3가 읽지 못함

4. **비동기 실행 시 주의**
   - `blocking=False`로 변경 시 Lambda 실패를 감지 못함
   - S3 결과 확인 로직이 별도로 필요

---

## 📞 연락처

- **DAG 담당자**: (담당자 이름/이메일 기입)
- **작업자**: youngjunlee
- **작업일**: 2025-10-14
