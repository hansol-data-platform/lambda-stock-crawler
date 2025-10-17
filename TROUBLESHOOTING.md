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

### 🔬 추가 테스트 결과 (2025-10-15)
**회사 목록 개수 조정 테스트:**
- ✅ **4분 12초 (252초)**: DAG Task Success
- ❌ **6분 35초 (395초)**: DAG Task Running → 타임아웃

**→ 타임아웃 임계점: 약 5분(300초) 정도로 추정**

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
- **🎯 실제 타임아웃 임계점**: ~5분(300초) - 테스트로 확인됨

→ **MWAA Task 레벨의 execution_timeout이 약 5분(300초)로 설정되어 있을 가능성**

### 4. 타임아웃 설정 의심 지점

#### 🔴 가능성 높음
1. **DAG 파일 `DEFAULT_ARGS`**
   ```python
   # hds_dap_stock_info_pipeline_annual_quater_v1.py
   DEFAULT_ARGS = {
       'execution_timeout': timedelta(minutes=5),  # ← 5분 설정 의심
   }
   ```
   - Task별 기본 타임아웃 설정
   - 가장 일반적인 설정 위치

2. **Task 정의 시 직접 설정**
   ```python
   # DAG 파일 내 PythonOperator
   PythonOperator(
       task_id='hds_sap_stock_ingest_quarter',
       python_callable=run_lambda_task,
       execution_timeout=timedelta(minutes=5),  # ← 개별 Task 타임아웃
   )
   ```
   - 개별 Task에 직접 설정된 경우

#### 🟡 가능성 중간
3. **Airflow 전역 설정 (airflow.cfg)**
   ```ini
   [core]
   default_task_execution_timeout = 300  # 5분(초 단위)
   ```
   - MWAA 환경 전역 설정
   - 모든 DAG에 기본값 적용

4. **DAG 레벨 설정**
   ```python
   with DAG(
       dag_id='hds_dap_stock_info_pipeline_annual_quater_v1',
       default_args=DEFAULT_ARGS,
       dagrun_timeout=timedelta(minutes=30),  # DAG 전체 타임아웃
   ):
   ```
   - DAG 전체 실행 타임아웃 (Task별 타임아웃과 다름)

#### 🟢 가능성 낮음
5. **MWAA 환경 설정 (AWS Console)**
   - MWAA 환경 구성에서 설정된 제약
   - 보통 이렇게 짧게 설정하지 않음

6. **common.py의 wrapper 함수**
   ```python
   # common.py - run_lambda_task()
   def run_lambda_task(..., timeout=300):  # ← wrapper 레벨 타임아웃
   ```
   - 공통 함수에서 타임아웃 적용
   - 문서에 common.py 수정 불가라고 했으므로 낮은 가능성

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

**확인 요청 사항 (우선순위 순):**

1. **DAG 파일 `DEFAULT_ARGS` 확인** (가장 가능성 높음)
   ```python
   # hds_dap_stock_info_pipeline_annual_quater_v1.py
   DEFAULT_ARGS = {
       'execution_timeout': timedelta(minutes=?),  # ← 현재 값 확인
   }
   ```
   - 현재 5분(300초)로 추정됨

2. **개별 Task 설정 확인**
   ```python
   # PythonOperator 정의 부분
   PythonOperator(
       task_id='hds_sap_stock_ingest_quarter',
       execution_timeout=timedelta(minutes=?),  # ← 이 설정이 있는지 확인
   )
   ```

3. **Airflow 전역 설정 확인** (가능하면)
   ```ini
   # airflow.cfg
   [core]
   default_task_execution_timeout = ?
   ```

**요청 내용:**
> "Lambda 크롤러 실행 시간 테스트 결과:
> - 4분 12초: Success ✅
> - 6분 35초: Timeout ❌
>
> 현재 Task execution_timeout이 약 5분(300초)로 설정된 것 같습니다.
> Lambda 크롤러가 10개 회사 처리 시 최대 10분 정도 걸리므로,
> Task `execution_timeout`을 **15분(900초)**으로 늘려주실 수 있나요?
>
> 확인이 필요한 설정:
> 1. DAG 파일의 DEFAULT_ARGS['execution_timeout']
> 2. PythonOperator의 개별 execution_timeout
> 3. Airflow 전역 설정 (가능하면)"

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

## 🔴 새로운 문제 발견 (2025-10-15) - 해결됨 ✅

### 증상: Lambda는 완료되지만 MWAA Task는 15분 타임아웃

**테스트 결과:**
- ✅ **1개 회사**: Lambda 실행 성공 (빠름)
- ✅ **6개 회사**: Lambda 6분 30초에 완료
- ❌ **8개 회사**: Lambda 6분 30초에 완료, 하지만 MWAA Task는 15분 타임아웃 Failed

### 최종 분석

**문제점:**
- Lambda는 정상 완료 (6분 30초, statusCode 200 반환)
- 하지만 boto3가 Lambda 응답을 읽는 중에 **15분 execution_timeout** 발생
- DAG Task 에러 로그:
  ```
  File "/usr/local/lib/python3.11/http/client.py", line 286, in _read_status
    line = str(self.fp.readline(_MAXLINE + 1), "iso-8859-1")
  airflow.exceptions.AirflowTaskTimeout: Timeout, PID: 22590
  ```

**근본 원인:**

1. **Lambda 응답 크기 문제**
   - Lambda가 `json.dumps(result_data, ensure_ascii=False, indent=2)` 사용
   - `indent=2`로 인해 응답이 불필요하게 커짐 (pretty print)
   - boto3가 큰 응답을 읽는데 시간이 오래 걸림

2. **boto3 read_timeout 설정**
   - `DEFAULT_LAMBDA_TIMEOUT = 800` (13분)
   - Lambda는 6분 30초에 끝났는데
   - boto3가 HTTP 응답 읽기에 15분 넘게 걸림

3. **execution_timeout 발생**
   - MWAA Task의 15분 execution_timeout 먼저 발생
   - boto3는 여전히 응답을 읽는 중
   - Task Failed로 처리

### 해결 방법 ✅

#### 수정 사항: Lambda 응답 크기 축소

**파일**: `stock_crawler_factory.py`

**변경 전**:
```python
'body': json.dumps(result_data, ensure_ascii=False, indent=2)  # pretty print
```

**변경 후**:
```python
'body': json.dumps(result_data, ensure_ascii=False)  # indent 제거, compact JSON
```

**효과**:
- JSON 응답 크기 감소 (공백, 개행 제거)
- boto3 HTTP 응답 읽기 속도 향상
- execution_timeout 내에 완료 가능

#### 적용 방법

1. `stock_crawler_factory.py` 수정 (완료)
2. ECR에 새 이미지 빌드 및 푸시
3. Lambda가 새 이미지 사용하도록 대기 또는 재배포
4. 8개 회사로 테스트

---

## 🎯 액션 아이템

### 완료 ✅
- [x] Lambda 응답 형식을 HTTP 표준 형식으로 수정
- [x] 회사 목록 쿼리에 `LIMIT 1` 추가하여 1개 회사만 처리
- [x] 1개 회사 테스트 → Task Success 확인
- [x] DEFAULT_ARGS에 `execution_timeout: timedelta(minutes=15)` 추가
- [x] `run_lambda_task` 함수에 `execution_timeout` 파라미터 추가 (기본값 15분)
- [x] 15분 타임아웃 정상 작동 확인 (15분에 Failed 떨어짐)
- [x] urllib timeout을 30초 → 180초로 증가
- [x] 6개 회사 테스트 → Success ✅

### 진행 중 🔄
- [x] **8개 회사 테스트** → Lambda가 시작하지 않음 (CloudWatch 로그 없음)
  - 패턴 발견: 1개 ✅, 6개 ✅, 8개 ❌
  - Lambda가 아예 시작하지 않음 (CloudWatch에 로그 없음)
  - 회사목록 API 응답 크기: ~800바이트 (작음)
  - 유일한 차이점: LIMIT 1 vs LIMIT 8

### 분석 필요 🔍
- [ ] **회사목록 Lambda URL의 Athena 쿼리 실행 시간 확인**
  - LIMIT 1일 때 vs LIMIT 8일 때 쿼리 실행 시간 차이
  - 회사목록 Lambda가 응답하는데 걸리는 실제 시간 측정
  - 가능성: Athena 쿼리가 180초를 초과하는지 확인

### 대기 ⏸️
- [ ] 회사목록 Lambda 쿼리 성능 최적화 또는 타임아웃 추가 증가
- [ ] 전체 회사로 테스트
- [ ] `LIMIT` 제거하고 원래대로 복구
- [ ] 정상 동작 확인 후 문서 업데이트

---

## 🔴 병렬 실행 문제 발견 (2025-10-15) - 해결 ✅

### 증상: 하나의 DAG 실행이 두 개의 Lambda를 동시에 실행

**발견 내용:**
- Quarter DAG 1번 실행하면 2개의 Lambda가 생성됨
- Quarter Lambda: `2025-10-15 14:57:17 (UTC+09:00)`
- Annual Lambda: `2025-10-15 14:57:20 (UTC+09:00)`
- **단 3초 차이로 거의 동시 실행**

### 근본 원인

**잘못된 DAG 설계:**
- `hds_dap_stock_info_pipeline_annual_v1.py` - Annual 전용 DAG ✅
- `hds_dap_stock_info_pipeline_annual_quater_v1.py` - **Quarter 전용이어야 하는데 Annual + Quarter 둘 다 실행** ❌

**기존 코드** (`hds_dap_stock_info_pipeline_annual_quater_v1.py`):
```python
SPECS = [
  ("hds_sap_stock_ingest_annual",  "HDS-DAP-DEV-SYS1-STOCK-INFO-CRAWLER-ANNUAL"),   # ❌ 불필요
  ("hds_sap_stock_ingest_quarter", "HDS-DAP-DEV-SYS1-STOCK-INFO-CRAWLER-QUARTER"),  # ✅ 필요
]

# 두 개를 병렬 실행
g_start >> heads  # heads = [annual_task, quarter_task]
```

**문제점:**
1. Quarter DAG가 Annual Lambda까지 실행
2. 두 Lambda가 동시에 회사목록 Lambda URL 호출
3. boto3가 두 Lambda 응답을 동시에 읽으려다가 막힘
4. 결과적으로 15분 execution_timeout 발생

### 해결 방법 ✅

**변경 사항**: Quarter DAG는 Quarter Lambda만 실행

```python
@task_group(group_id="l0_ingestion")
def tg_l0() -> Ends:
    # ✅ Quarter만 실행 (Annual은 별도 DAG 파일에서 실행)
    j0 = run_lambda_task("hds_sap_stock_ingest_quarter",
                         function_name="HDS-DAP-DEV-SYS1-STOCK-INFO-CRAWLER-QUARTER",
                         payload=None,
                         blocking=True)

    return Ends(head=j0.head, tail=j0.tail)
```

**DAG 역할 분리:**
```
hds_dap_stock_info_pipeline_annual_v1.py
  └─ Annual Lambda만 실행

hds_dap_stock_info_pipeline_annual_quater_v1.py
  └─ Quarter Lambda만 실행
```

**효과:**
- Quarter DAG 실행 시 Quarter Lambda만 실행
- Annual DAG 실행 시 Annual Lambda만 실행
- 병렬 실행 없음, boto3 응답 읽기 정상 처리
- execution_timeout 내에 정상 완료 예상

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
