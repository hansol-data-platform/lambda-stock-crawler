"""
주식 크롤러 팩토리 - AWS Lambda용 메인 진입점
타입에 따라 다른 크롤러를 실행하는 팩토리 메서드
"""

import json
import os
import asyncio
from datetime import datetime, timezone, timedelta
import boto3
from dotenv import load_dotenv

# .env 파일 로드 (로컬 환경에서만)
if os.path.exists('.env'):
    load_dotenv()


def factory_lambda_handler(event, context):
    """
    주식 크롤러 팩토리 Lambda 핸들러
    
    Args:
        event (dict): Lambda 이벤트 데이터
            - crawler_type: 'daily', 'quarter', 'annual'
            - s3_bucket: S3 버킷명 (선택사항)
            - delay_between_stocks: 종목 간 대기시간 (선택사항)
        context: Lambda 컨텍스트
        
    Returns:
        dict: HTTP 응답
    """
    try:
        # 환경변수 우선순위로 설정값 읽기 (환경변수 > 이벤트 파라미터 > 기본값)
        crawler_type = os.environ.get('CRAWLER_TYPE') or event.get('crawler_type') or 'daily'
        s3_bucket = os.environ.get('S3_BUCKET') or event.get('s3_bucket') or 'test-stock-info-bucket'
        delay_between_stocks = int(os.environ.get('DELAY_BETWEEN_STOCKS') or event.get('delay_between_stocks') or '2')
        
        # 디버깅을 위한 환경변수 값 출력
        print(f"🔍 환경변수 디버깅:")
        print(f"   CRAWLER_TYPE: {os.environ.get('CRAWLER_TYPE', 'None')}")
        print(f"   S3_BUCKET: {os.environ.get('S3_BUCKET', 'None')}")
        print(f"   DELAY_BETWEEN_STOCKS: {os.environ.get('DELAY_BETWEEN_STOCKS', 'None')}")
        print(f"🔍 이벤트 파라미터:")
        print(f"   crawler_type: {event.get('crawler_type', 'None')}")
        print(f"   s3_bucket: {event.get('s3_bucket', 'None')}")
        print(f"   delay_between_stocks: {event.get('delay_between_stocks', 'None')}")
        print(f"🚀 주식 크롤러 팩토리 시작 - 타입: {crawler_type}, 버킷: {s3_bucket}, 딜레이: {delay_between_stocks}초")
        
        # 이벤트에 환경변수 값들 추가
        event_with_env = event.copy()
        event_with_env['s3_bucket'] = s3_bucket
        event_with_env['delay_between_stocks'] = delay_between_stocks
        
        # 타입에 따라 분기
        if crawler_type == 'daily':
            return handle_daily_crawler(event_with_env, context)
        elif crawler_type == 'quarter':
            return handle_quarter_crawler(event_with_env, context)
        elif crawler_type == 'annual':
            return handle_annual_crawler(event_with_env, context)
        else:
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json; charset=utf-8'
                },
                'body': json.dumps({
                    'success': False,
                    'error': f'지원하지 않는 크롤러 타입: {crawler_type}',
                    'supported_types': ['daily', 'quarter', 'annual']
                }, ensure_ascii=False, indent=2)
            }
            
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json; charset=utf-8'
            },
            'body': json.dumps({
                'success': False,
                'error': f'팩토리 실행 중 오류: {str(e)}'
            }, ensure_ascii=False, indent=2)
        }


def handle_daily_crawler(event, context):
    """
    일간 투자정보 크롤러 실행 (PER/EPS)
    """
    print("📊 일간 투자정보(PER/EPS) 크롤러 실행")
    
    # 기존 naver_stock_invest_info_crawler 모듈 import
    from naver_stock_invest_info_crawler import lambda_handler
    
    # 해당 핸들러 실행
    return lambda_handler(event, context)


def handle_quarter_crawler(event, context):
    """
    분기별 재무정보 크롤러 실행
    """
    print("📈 분기별 재무정보 크롤러 실행")

    try:
        # naver_stock_invest_index_crawler 모듈 import
        from naver_stock_invest_index_crawler import crawl_multiple_stocks_direct

        # 람다 펑션에서 종목 목록 가져오기
        try:
            import urllib.request
            import urllib.error

            # 환경변수에서 람다 URL 가져오기
            lambda_url = os.environ.get('STOCK_LAMBDA_URL', 'https://rbtvqk5rybgcl63umd5skjnc4i0tqjpl.lambda-url.ap-northeast-2.on.aws/')
            print(f"📋 람다 펑션에서 종목 목록 가져오기: {lambda_url}")

            with urllib.request.urlopen(lambda_url, timeout=30) as response:
                response_data = response.read().decode('utf-8')

            print(f"🔍 람다 응답 데이터: {response_data[:500]}...")

            api_response = json.loads(response_data)
            print(f"🔍 API 응답 구조: {type(api_response)}")

            # API 응답 검증
            if not api_response.get('success'):
                error_msg = api_response.get('error', 'Unknown error')
                raise Exception(f"람다 API 호출 실패: {error_msg}")

            # 데이터 추출
            raw_stocks = api_response.get('data', [])
            print(f"📋 람다 API에서 {len(raw_stocks)}개 회사 데이터 수신")

            # stock_code가 null이 아닌 값만 필터링하고 변환
            stocks = []
            for stock in raw_stocks:
                stock_code = stock.get('stock_code')
                stock_nm = stock.get('stock_nm')

                # stock_code가 있는 경우만 (이 프로젝트용)
                if stock_code and stock_nm:
                    stocks.append({
                        'code': stock_code,
                        'name': stock_nm
                    })

            print(f"📋 람다 펑션에서 {len(stocks)}개 유효한 종목 로드 완료")
        except Exception as e:
            return {
                'statusCode': 500,
                'headers': {
                    'Content-Type': 'application/json; charset=utf-8'
                },
                'body': json.dumps({
                    'success': False,
                    'error': f'람다 펑션에서 종목 목록 로드 실패: {str(e)}'
                }, ensure_ascii=False, indent=2)
            }
        
        # 분기별 크롤링 실행
        print("🚀 분기별 재무정보 크롤링 시작")

        # 임시 출력 디렉토리 생성
        output_dir = "/tmp/crawl_results"
        os.makedirs(output_dir, exist_ok=True)

        # s3_bucket 설정
        s3_bucket = os.environ.get('S3_BUCKET') or event.get('s3_bucket') or 'test-stock-info-bucket'

        # crawl_multiple_stocks_direct 실행 (분기) - 종목 목록을 직접 전달
        import asyncio
        if os.name == 'nt':
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

        # MWAA/Airflow 환경에서 이벤트 루프 충돌 방지
        try:
            # 기존 이벤트 루프 확인
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # MWAA 환경: 이미 실행 중인 루프가 있음
                print("🔄 [MWAA] 기존 이벤트 루프 감지 - nest_asyncio 적용")
                try:
                    import nest_asyncio
                    nest_asyncio.apply()
                    crawl_result = asyncio.run(crawl_multiple_stocks_direct(stocks, output_dir, "분기", s3_bucket))
                except ImportError:
                    print("⚠️ nest_asyncio 미설치 - run_until_complete 사용")
                    # nest_asyncio 없으면 새 태스크로 실행
                    crawl_result = loop.run_until_complete(crawl_multiple_stocks_direct(stocks, output_dir, "분기", s3_bucket))
            else:
                # 일반 환경: 새 이벤트 루프 생성
                crawl_result = asyncio.run(crawl_multiple_stocks_direct(stocks, output_dir, "분기", s3_bucket))
        except RuntimeError as e:
            # 이벤트 루프 관련 오류 시 새 루프 생성
            print(f"⚠️ 이벤트 루프 오류 ({e}) - 새 루프 생성")
            new_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(new_loop)
            try:
                crawl_result = new_loop.run_until_complete(crawl_multiple_stocks_direct(stocks, output_dir, "분기", s3_bucket))
            finally:
                new_loop.close()

        # 크롤링 결과를 JSON 직렬화 가능한 형태로 요약
        if crawl_result:
            summary_result = {
                "success": True,
                "total_companies": len(stocks),
                "message": f"{len(stocks)}개 회사의 분기별 재무정보 크롤링 완료",
                "output_directory": output_dir,
                "s3_bucket": s3_bucket
            }
        else:
            summary_result = {
                "success": False,
                "message": "크롤링 실행 중 오류 발생"
            }

        # S3 업로드 결과
        s3_upload_result = {
            "success": True,
            "message": "S3 업로드는 크롤링 함수 내부에서 처리됨"
        }

        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json; charset=utf-8'
            },
            'body': json.dumps({
                'success': True,
                'crawler_type': 'quarter',
                'crawl_result': summary_result,
                's3_upload': s3_upload_result,
                'crawl_time': datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%d %H:%M:%S")
            }, ensure_ascii=False, indent=2)
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json; charset=utf-8'
            },
            'body': json.dumps({
                'success': False,
                'error': f'분기별 크롤러 실행 중 오류: {str(e)}'
            }, ensure_ascii=False, indent=2)
        }


def handle_annual_crawler(event, context):
    """
    연간 재무정보 크롤러 실행
    """
    print("📊 연간 재무정보 크롤러 실행")

    try:
        # naver_stock_invest_index_crawler 모듈 import
        from naver_stock_invest_index_crawler import crawl_multiple_stocks_direct

        # 람다 펑션에서 종목 목록 가져오기
        try:
            import urllib.request
            import urllib.error

            # 환경변수에서 람다 URL 가져오기
            lambda_url = os.environ.get('STOCK_LAMBDA_URL', 'https://rbtvqk5rybgcl63umd5skjnc4i0tqjpl.lambda-url.ap-northeast-2.on.aws/')
            print(f"📋 람다 펑션에서 종목 목록 가져오기: {lambda_url}")

            with urllib.request.urlopen(lambda_url, timeout=30) as response:
                response_data = response.read().decode('utf-8')

            print(f"🔍 람다 응답 데이터: {response_data[:500]}...")

            api_response = json.loads(response_data)
            print(f"🔍 API 응답 구조: {type(api_response)}")

            # API 응답 검증
            if not api_response.get('success'):
                error_msg = api_response.get('error', 'Unknown error')
                raise Exception(f"람다 API 호출 실패: {error_msg}")

            # 데이터 추출
            raw_stocks = api_response.get('data', [])
            print(f"📋 람다 API에서 {len(raw_stocks)}개 회사 데이터 수신")

            # stock_code가 null이 아닌 값만 필터링하고 변환
            stocks = []
            for stock in raw_stocks:
                stock_code = stock.get('stock_code')
                stock_nm = stock.get('stock_nm')

                # stock_code가 있는 경우만 (이 프로젝트용)
                if stock_code and stock_nm:
                    stocks.append({
                        'code': stock_code,
                        'name': stock_nm
                    })

            print(f"📋 람다 펑션에서 {len(stocks)}개 유효한 종목 로드 완료")
        except Exception as e:
            return {
                'statusCode': 500,
                'headers': {
                    'Content-Type': 'application/json; charset=utf-8'
                },
                'body': json.dumps({
                    'success': False,
                    'error': f'람다 펑션에서 종목 목록 로드 실패: {str(e)}'
                }, ensure_ascii=False, indent=2)
            }
        
        # 연간 크롤링 실행
        print("🚀 연간 재무정보 크롤링 시작")

        # 임시 출력 디렉토리 생성
        output_dir = "/tmp/crawl_results"
        os.makedirs(output_dir, exist_ok=True)

        # s3_bucket 설정
        s3_bucket = os.environ.get('S3_BUCKET') or event.get('s3_bucket') or 'test-stock-info-bucket'

        # crawl_multiple_stocks_direct 실행 (연간) - 종목 목록을 직접 전달
        import asyncio
        if os.name == 'nt':
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

        # MWAA/Airflow 환경에서 이벤트 루프 충돌 방지
        try:
            # 기존 이벤트 루프 확인
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # MWAA 환경: 이미 실행 중인 루프가 있음
                print("🔄 [MWAA] 기존 이벤트 루프 감지 - nest_asyncio 적용")
                try:
                    import nest_asyncio
                    nest_asyncio.apply()
                    crawl_result = asyncio.run(crawl_multiple_stocks_direct(stocks, output_dir, "연간", s3_bucket))
                except ImportError:
                    print("⚠️ nest_asyncio 미설치 - run_until_complete 사용")
                    # nest_asyncio 없으면 새 태스크로 실행
                    crawl_result = loop.run_until_complete(crawl_multiple_stocks_direct(stocks, output_dir, "연간", s3_bucket))
            else:
                # 일반 환경: 새 이벤트 루프 생성
                crawl_result = asyncio.run(crawl_multiple_stocks_direct(stocks, output_dir, "연간", s3_bucket))
        except RuntimeError as e:
            # 이벤트 루프 관련 오류 시 새 루프 생성
            print(f"⚠️ 이벤트 루프 오류 ({e}) - 새 루프 생성")
            new_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(new_loop)
            try:
                crawl_result = new_loop.run_until_complete(crawl_multiple_stocks_direct(stocks, output_dir, "연간", s3_bucket))
            finally:
                new_loop.close()

        # 크롤링 결과를 JSON 직렬화 가능한 형태로 요약
        if crawl_result:
            summary_result = {
                "success": True,
                "total_companies": len(stocks),
                "message": f"{len(stocks)}개 회사의 연간 재무정보 크롤링 완료",
                "output_directory": output_dir,
                "s3_bucket": s3_bucket
            }
        else:
            summary_result = {
                "success": False,
                "message": "크롤링 실행 중 오류 발생"
            }

        # S3 업로드 결과
        s3_upload_result = {
            "success": True,
            "message": "S3 업로드는 크롤링 함수 내부에서 처리됨"
        }

        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json; charset=utf-8'
            },
            'body': json.dumps({
                'success': True,
                'crawler_type': 'annual',
                'crawl_result': summary_result,
                's3_upload': s3_upload_result,
                'crawl_time': datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%d %H:%M:%S")
            }, ensure_ascii=False, indent=2)
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json; charset=utf-8'
            },
            'body': json.dumps({
                'success': False,
                'error': f'연간 크롤러 실행 중 오류: {str(e)}'
            }, ensure_ascii=False, indent=2)
        }


if __name__ == "__main__":
    print("🏭 주식 크롤러 팩토리 - 로컬 테스트")
    print("=" * 50)
    
    # 테스트 이벤트 (환경변수 사용)
    test_events = [
        # {
        #     'crawler_type': os.environ.get('CRAWLER_TYPE', 'daily'),
        #     's3_bucket': os.environ.get('S3_BUCKET', 'test-stock-info-bucket'),
        #     'delay_between_stocks': int(os.environ.get('DELAY_BETWEEN_STOCKS', '2'))
        # },
        {
           'crawler_type': 'quarter',
           's3_bucket': os.environ.get('S3_BUCKET', 'test-stock-info-bucket'),
           'delay_between_stocks': int(os.environ.get('DELAY_BETWEEN_STOCKS', '2'))
        }
        # ,
        # {
        #    'crawler_type': 'annual',
        #    's3_bucket': os.environ.get('S3_BUCKET', 'test-stock-info-bucket'),
        #    'delay_between_stocks': int(os.environ.get('DELAY_BETWEEN_STOCKS', '2'))
        # }
    ]
    
    for test_event in test_events:
        print(f"\n🧪 테스트: {test_event['crawler_type']}")
        result = factory_lambda_handler(test_event, None)
        print(f"결과 상태코드: {result['statusCode']}")
        
        if result['statusCode'] == 200:
            body = json.loads(result['body'])
            print(f"✅ 성공: {body.get('success', False)}")
        else:
            print(f"❌ 실패: {result['statusCode']}")
