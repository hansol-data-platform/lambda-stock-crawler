# 🚀 주식 크롤러 - AWS Lambda 컨테이너 이미지

네이버 금융에서 주식 투자정보를 자동으로 크롤링하여 S3에 저장하는 AWS Lambda 함수입니다.

## 📋 목차

- [기능 개요](#-기능-개요)
- [아키텍처](#-아키텍처)
- [프로젝트 구조](#-프로젝트-구조)
- [환경변수 설정](#-환경변수-설정)
- [로컬 개발](#-로컬-개발)
- [Docker 빌드 및 배포](#-docker-빌드-및-배포)
- [Lambda 함수 호출](#-lambda-함수-호출)
- [S3 데이터 구조](#-s3-데이터-구조)
- [문제 해결](#-문제-해결)

## 🎯 기능 개요

### **지원하는 크롤러 타입**

| 크롤러 타입 | 설명 | 데이터 | S3 경로 |
|------------|------|--------|---------|
| `daily_info` | 일간 투자정보 (PER/EPS) | PER, EPS, PBR, BPS, 배당수익률 | `period=daily/` |
| `quarter` | 분기별 재무정보 | 수익성, 성장성, 안정성, 활동성 지표 | `period=quarter/` |
| `annual` | 연간 재무정보 | 수익성, 성장성, 안정성, 활동성 지표 | `period=annual/` |

### **주요 특징**

- ✅ **단일 컨테이너 이미지**로 3가지 크롤러 지원
- ✅ **환경변수 기반 설정** 관리
- ✅ **S3 자동 업로드** (UTF-8 BOM 포함)
- ✅ **동적 S3 경로** 생성 (년/월/일 기준)
- ✅ **Playwright 1.40** 안정 버전 사용
- ✅ **Lambda 최적화** 브라우저 설정

## 🏗️ 아키텍처

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Lambda Event  │───▶│  Factory Handler │───▶│  Crawler Type   │
│                 │    │                  │    │                 │
│ crawler_type    │    │ stock_crawler_   │    │ daily_info      │
│ s3_bucket       │    │ factory.py       │    │ quarter         │
│ delay_between   │    │                  │    │ annual          │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                │
                                ▼
                       ┌──────────────────┐
                       │   S3 Storage     │
                       │                  │
                       │ l0/ver=1/sys=    │
                       │ naver/loc=common/│
                       │ period={type}/   │
                       │ year={YYYY}/     │
                       │ mmdd={mmdd}/     │
                       └──────────────────┘
```

## 📁 프로젝트 구조

```
stock-crawler/
├── 📄 stock_crawler_factory.py          # 메인 팩토리 핸들러
├── 📄 naver_stock_invest_info_crawler.py # 일간 투자정보 크롤러
├── 📄 naver_stock_invest_index_crawler.py # 분기/연간 재무정보 크롤러
├── 📄 s3_utils.py                       # S3 업로드 공통 유틸리티
├── 📄 stocks.json                       # 크롤링 대상 종목 리스트
├── 📄 requirements.txt                  # Python 의존성
├── 📄 Dockerfile.pw                     # Docker 이미지 정의
├── 📄 .dockerignore                     # Docker 빌드 제외 파일
├── 📄 ecr-lifecycle-policy.json         # ECR 라이프사이클 정책
└── 📄 README.md                         # 이 파일
```

## ⚙️ 환경변수 설정

### **환경변수 목록**

| 변수명 | 설명 | 기본값 | 예시 |
|--------|------|--------|------|
| `CRAWLER_TYPE` | 크롤러 타입 | `daily_info` | `daily_info`, `quarter`, `annual` |
| `S3_BUCKET` | S3 버킷명 | `test-stock-info-bucket` | `my-production-bucket` |
| `DELAY_BETWEEN_STOCKS` | 종목 간 대기시간(초) | `2` | `3` |
| `HEADLESS` | 브라우저 헤드리스 모드 | `true` | `true`, `false` |
| `WAIT_TIMEOUT` | 요소 대기시간(ms) | `15000` | `20000` |
| `LOG_LEVEL` | 로그 레벨 | `INFO` | `DEBUG`, `INFO`, `WARNING` |

### **우선순위**

1. **Lambda 환경변수** (최우선)
2. **이벤트 파라미터**
3. **.env 파일** (로컬에서만)
4. **기본값** (코드 내 하드코딩)

### **로컬 개발 설정**

```bash
# .env 파일 생성
cp env.example .env

# .env 파일 편집
CRAWLER_TYPE=daily_info
S3_BUCKET=my-local-bucket
DELAY_BETWEEN_STOCKS=3
HEADLESS=true
WAIT_TIMEOUT=15000
```

## 🛠️ 로컬 개발

### **1. 환경 설정**

```bash
# 저장소 클론
git clone <repository-url>
cd stock-crawler

# Python 가상환경 생성
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 의존성 설치
pip install -r requirements.txt

# Playwright 브라우저 설치
python -m playwright install

# 환경변수 설정
cp env.example .env
# .env 파일 편집
```

### **2. 로컬 테스트**

```bash
# 팩토리 핸들러 테스트
python stock_crawler_factory.py

# 개별 크롤러 테스트
python naver_stock_invest_info_crawler.py
python naver_stock_invest_index_crawler.py
```

### **3. 테스트 결과**

- ✅ **브라우저 정상 실행**
- ✅ **크롤링 데이터 수집**
- ✅ **S3 업로드** (AWS CLI 설정된 경우)
- ✅ **UTF-8 한글 지원**

## 🐳 Docker 빌드 및 배포

### **1. ECR 리포지토리 생성**

```bash
# ECR 리포지토리 생성
aws ecr create-repository \
  --repository-name stock-crawler \
  --region ap-northeast-2

# 로그인
aws ecr get-login-password --region ap-northeast-2 | \
  docker login --username AWS --password-stdin \
  <account-id>.dkr.ecr.ap-northeast-2.amazonaws.com
```

### **2. Docker 이미지 빌드**

```bash
# 이미지 빌드
docker build -f Dockerfile.pw -t stock-crawler .

# 태그 설정
docker tag stock-crawler:latest \
  <account-id>.dkr.ecr.ap-northeast-2.amazonaws.com/stock-crawler:latest

# ECR에 푸시
docker push <account-id>.dkr.ecr.ap-northeast-2.amazonaws.com/stock-crawler:latest
```

### **3. Lambda 함수 생성**

```bash
# Lambda 함수 생성
aws lambda create-function \
  --function-name stock-crawler-lambda \
  --package-type Image \
  --code ImageUri=<account-id>.dkr.ecr.ap-northeast-2.amazonaws.com/stock-crawler:latest \
  --role arn:aws:iam::<account-id>:role/lambda-execution-role \
  --timeout 900 \
  --memory-size 3008 \
  --environment Variables='{
    "CRAWLER_TYPE":"daily_info",
    "S3_BUCKET":"my-production-bucket",
    "DELAY_BETWEEN_STOCKS":"2"
  }'
```

## 📞 Lambda 함수 호출

### **1. AWS CLI로 호출**

```bash
# 일간 투자정보 크롤러
aws lambda invoke \
  --function-name stock-crawler-lambda \
  --payload '{"crawler_type": "daily_info"}' \
  response.json

# 분기별 재무정보 크롤러
aws lambda invoke \
  --function-name stock-crawler-lambda \
  --payload '{"crawler_type": "quarter"}' \
  response.json

# 연간 재무정보 크롤러
aws lambda invoke \
  --function-name stock-crawler-lambda \
  --payload '{"crawler_type": "annual"}' \
  response.json
```

### **2. 이벤트 파라미터로 오버라이드**

```json
{
  "crawler_type": "daily_info",
  "s3_bucket": "custom-bucket",
  "delay_between_stocks": 5
}
```

### **3. EventBridge 스케줄링**

```json
{
  "crawler_type": "daily_info",
  "s3_bucket": "my-production-bucket"
}
```

## 📊 S3 데이터 구조

### **S3 경로 패턴**

```
s3://{bucket-name}/l0/ver=1/sys=naver/loc=common/period={type}/year={YYYY}/mmdd={mmdd}/{filename}
```

### **파일명**

| 크롤러 타입 | 파일명 | 설명 |
|------------|--------|------|
| `daily_info` | `stock_invest_info.csv` | PER/EPS 데이터 |
| `quarter` | `financial_data_transformed.csv` | 분기별 재무정보 |
| `annual` | `financial_data_transformed.csv` | 연간 재무정보 |

### **CSV 형식**

- ✅ **UTF-8 BOM** 포함 (한글 깨짐 방지)
- ✅ **Excel 호환** 형식
- ✅ **구조화된 데이터** (행 기반)

### **예시 S3 경로**

```
s3://my-bucket/l0/ver=1/sys=naver/loc=common/period=daily/year=2025/mmdd=0909/stock_invest_info.csv
s3://my-bucket/l0/ver=1/sys=naver/loc=common/period=quarter/year=2025/mmdd=0909/financial_data_transformed.csv
s3://my-bucket/l0/ver=1/sys=naver/loc=common/period=annual/year=2025/mmdd=0909/financial_data_transformed.csv
```

## 🔧 문제 해결

### **1. 브라우저 실행 오류**

```bash
# Playwright 버전 확인
python -c "import playwright; print(playwright.__version__)"

# 브라우저 재설치
python -m playwright install --force
```

### **2. S3 업로드 실패**

```bash
# AWS CLI 설정 확인
aws configure list

# S3 버킷 권한 확인
aws s3 ls s3://your-bucket-name
```

### **3. 한글 깨짐 문제**

- ✅ **UTF-8 BOM** 자동 적용
- ✅ **Content-Type** 올바른 설정
- ✅ **Excel 호환** 형식

### **4. Lambda 타임아웃**

```bash
# Lambda 함수 설정 업데이트
aws lambda update-function-configuration \
  --function-name stock-crawler-lambda \
  --timeout 900 \
  --memory-size 3008
```

## 📈 성능 최적화

### **메모리 사용량**

- **권장 메모리**: 3008MB
- **브라우저 최적화**: `--single-process`, `--no-zygote`
- **이미지 크기**: ~2GB

### **실행 시간**

- **일간 크롤러**: ~5-10분 (8개 종목)
- **분기/연간 크롤러**: ~15-30분 (8개 종목)
- **타임아웃 설정**: 900초 (15분)

## 🤝 기여하기

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📄 라이선스

이 프로젝트는 MIT 라이선스 하에 배포됩니다. 자세한 내용은 `LICENSE` 파일을 참조하세요.

## 📞 지원

문제가 발생하거나 질문이 있으시면 이슈를 생성해 주세요.

---

**Made with ❤️ for automated stock data collection**
