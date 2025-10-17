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
            print(f"❌ [MWAA] 지원하지 않는 크롤러 타입: {crawler_type}")
            return {'success': False, 'error': f'Unsupported crawler type: {crawler_type}'}

    except Exception as e:
        print(f"❌ [MWAA] 팩토리 실행 중 오류: {str(e)}")
        return {'success': False, 'error': str(e)}


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

            with urllib.request.urlopen(lambda_url, timeout=180) as response:  # 3분으로 증가
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
            print(f"❌ [MWAA] 종목 목록 로드 실패: {str(e)}")
            return {'success': False, 'error': str(e)}

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

        # MWAA boto3 Lambda invoke 호환 형식으로 반환 (최소 데이터만)
        print("================================================================================")
        print("🎯 [MWAA] Lambda 실행 완료 - 응답 반환")

        # DataFrame 객체는 반환하지 않음 (JSON 직렬화 불가)
        # crawl_result는 dict of DataFrames이므로 응답에 포함 불가
        success = crawl_result is not None and len(crawl_result) > 0

        result_data = {
            'success': success,
            'count': len(stocks)
        }

        print(f"🎯 [MWAA] 응답 데이터: {json.dumps(result_data, ensure_ascii=False)}")
        print("================================================================================")

        # 모든 이벤트 루프 정리 (Lambda 종료 보장)
        try:
            loop = asyncio.get_event_loop()
            if loop and not loop.is_closed():
                pending = asyncio.all_tasks(loop)
                if pending:
                    print(f"🔄 [MWAA] {len(pending)}개 미완료 태스크 정리 중...")
                    loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        except Exception as e:
            print(f"⚠️ [MWAA] 이벤트 루프 정리 중 오류 (무시): {e}")

        print("✅ [MWAA] Lambda 핸들러 반환 직전 - Quarter")

        # boto3 SDK invoke 호환: 직접 dict 반환 (statusCode/body 래핑 제거)
        return result_data

    except Exception as e:
        print(f"❌ [MWAA] 분기별 크롤러 실행 중 오류: {str(e)}")
        return {'success': False, 'error': str(e)}


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

            with urllib.request.urlopen(lambda_url, timeout=180) as response:  # 3분으로 증가
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
            print(f"❌ [MWAA] 종목 목록 로드 실패: {str(e)}")
            return {'success': False, 'error': str(e)}

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

        # MWAA boto3 Lambda invoke 호환 형식으로 반환 (최소 데이터만)
        print("================================================================================")
        print("🎯 [MWAA] Lambda 실행 완료 - 응답 반환")

        # DataFrame 객체는 반환하지 않음 (JSON 직렬화 불가)
        # crawl_result는 dict of DataFrames이므로 응답에 포함 불가
        success = crawl_result is not None and len(crawl_result) > 0

        result_data = {
            'success': success,
            'count': len(stocks)
        }

        print(f"🎯 [MWAA] 응답 데이터: {json.dumps(result_data, ensure_ascii=False)}")
        print("================================================================================")

        # 모든 이벤트 루프 정리 (Lambda 종료 보장)
        try:
            loop = asyncio.get_event_loop()
            if loop and not loop.is_closed():
                pending = asyncio.all_tasks(loop)
                if pending:
                    print(f"🔄 [MWAA] {len(pending)}개 미완료 태스크 정리 중...")
                    loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        except Exception as e:
            print(f"⚠️ [MWAA] 이벤트 루프 정리 중 오류 (무시): {e}")

        print("✅ [MWAA] Lambda 핸들러 반환 직전 - Annual")

        # boto3 SDK invoke 호환: 직접 dict 반환 (statusCode/body 래핑 제거)
        return result_data

    except Exception as e:
        print(f"❌ [MWAA] 연간 크롤러 실행 중 오류: {str(e)}")
        return {'success': False, 'error': str(e)}


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
