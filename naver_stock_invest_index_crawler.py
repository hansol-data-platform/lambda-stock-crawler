"""
Playwright를 사용한 한솔홀딩스 투자분석 데이터 크롤러
더 빠르고 안정적인 크롤링을 위해 Playwright 사용
"""

import asyncio
import pandas as pd
from playwright.async_api import async_playwright
import json
from datetime import datetime, timezone, timedelta
import os
import re
from s3_utils import upload_file_to_s3, generate_s3_key
from dotenv import load_dotenv
import sys

# Windows에서 Unicode 인코딩 문제 해결
if os.name == 'nt':
    os.environ['PYTHONIOENCODING'] = 'utf-8'

# .env 파일 로드 (로컬 환경에서만)
if os.path.exists('.env'):
    load_dotenv()

class PlaywrightStockCrawler:
    def __init__(self, headless=None, wait_timeout=None):
        """
        Playwright 크롤러 초기화
        
        Args:
            headless (bool): 브라우저를 백그라운드에서 실행할지 여부 (환경변수 우선)
            wait_timeout (int): 요소 대기 시간 (밀리초) (환경변수 우선)
        """
        # 환경변수에서 설정값 읽기
        self.headless = headless if headless is not None else os.environ.get('HEADLESS', 'true').lower() == 'true'
        self.wait_timeout = wait_timeout or int(os.environ.get('WAIT_TIMEOUT', '10000'))
        self.browser = None
        self.page = None
        self.start_time = None
        self.end_time = None
    
    def start_timer(self):
        """크롤링 시작 시간 기록"""
        self.start_time = datetime.now()
        timestamp = self.start_time.strftime("%Y-%m-%d %H:%M:%S")
        print(f"🕐 크롤링 시작: {timestamp}")
        
    def end_timer(self):
        """크롤링 종료 시간 기록 및 소요시간 계산"""
        self.end_time = datetime.now()
        timestamp = self.end_time.strftime("%Y-%m-%d %H:%M:%S")
        print(f"🕐 크롤링 종료: {timestamp}")
        
        if self.start_time:
            duration = self.end_time - self.start_time
            total_seconds = int(duration.total_seconds())
            hours = total_seconds // 3600
            minutes = (total_seconds % 3600) // 60
            seconds = total_seconds % 60
            
            if hours > 0:
                duration_str = f"{hours}시간 {minutes}분 {seconds}초"
            elif minutes > 0:
                duration_str = f"{minutes}분 {seconds}초"
            else:
                duration_str = f"{seconds}초"
            
            print(f"⏱️ 총 소요시간: {duration_str}")
            return duration
        return None
        
    async def setup_browser(self):
        """브라우저 설정 및 초기화"""
        try:
            self.playwright = await async_playwright().start()
            
            # Chromium 브라우저 실행 (Lambda 최적화)
            browser_args = [
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--disable-blink-features=AutomationControlled',
                '--disable-web-security',
                '--disable-features=VizDisplayCompositor',
                '--disable-background-timer-throttling',
                '--disable-backgrounding-occluded-windows',
                '--disable-renderer-backgrounding',
                '--disable-ipc-flooding-protection',
                '--disable-background-networking'
            ]
            
            # Lambda 환경에서 추가 최적화
            if os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
                browser_args.extend([
                    '--single-process',
                    '--disable-gpu',
                    '--disable-software-rasterizer',
                    '--memory-pressure-off'
                ])
                
            self.browser = await self.playwright.chromium.launch(
                headless=self.headless,
                args=browser_args
            )
            
            # 컨텍스트 생성 (User-Agent 포함)
            self.context = await self.browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            
            # 새 페이지 생성
            self.page = await self.context.new_page()
            
            # 뷰포트 설정
            await self.page.set_viewport_size({"width": 1920, "height": 1080})
            
            print("✅ Playwright 브라우저가 성공적으로 초기화되었습니다.")
            
        except Exception as e:
            print(f"❌ 브라우저 초기화 중 오류 발생: {str(e)}")
            raise
            
    async def navigate_to_page(self, url):
        """페이지로 이동"""
        try:
            print(f"📄 페이지 이동 중: {url}")
            
            # 페이지 로드
            await self.page.goto(url, wait_until='domcontentloaded', timeout=30000)
            
            # 추가 로딩 대기
            await self.page.wait_for_timeout(3000)
            
            print("✅ 페이지 로딩 완료")
            
        except Exception as e:
            print(f"❌ 페이지 이동 중 오류 발생: {str(e)}")
            raise
            
    async def select_finGubun(self, finGubun_type="K-IFRS(연결)"):
        """
        finGubun 콤보박스에서 값 선택
        
        Args:
            finGubun_type (str): "K-IFRS(연결)" 또는 "K-IFRS(별도)"
            
        Returns:
            bool: 성공 여부
        """
        try:
            print(f"📊 '{finGubun_type}' finGubun 선택 시도 중...")
            
            # finGubun 콤보박스 찾기
            selectors = [
                "#finGubun",
                "select[id='finGubun']",
                "select[name='finGubun']",
                "[id*='finGubun']"
            ]
            
            # 각 셀렉터로 시도
            for selector in selectors:
                try:
                    # 콤보박스가 보이는지 확인
                    combo_element = self.page.locator(selector).first
                    if await combo_element.is_visible():
                        # 콤보박스를 화면에 스크롤
                        await combo_element.scroll_into_view_if_needed()
                        await self.page.wait_for_timeout(500)
                        
                        # 콤보박스에서 값 선택
                        await combo_element.select_option(label=finGubun_type)
                        print(f"✅ '{finGubun_type}' finGubun 선택 성공!")
                        
                        # 데이터 로딩 대기
                        await self.page.wait_for_timeout(1500)
                        return True
                        
                except Exception as e:
                    continue
                    
            # JavaScript로 직접 콤보박스 선택 시도
            print(f"🔄 JavaScript로 '{finGubun_type}' finGubun 선택 시도...")
            
            js_select_script = f"""
            () => {{
                const finGubunSelect = document.getElementById('finGubun') || 
                                     document.querySelector('select[id*="finGubun"]') ||
                                     document.querySelector('select[name*="finGubun"]');
                
                if (finGubunSelect) {{
                    const options = Array.from(finGubunSelect.options);
                    const targetOption = options.find(option => 
                        option.text.includes('{finGubun_type}') || 
                        option.value.includes('{finGubun_type}')
                    );
                    
                    if (targetOption) {{
                        finGubunSelect.value = targetOption.value;
                        
                        // change 이벤트 발생
                        const event = new Event('change', {{ bubbles: true }});
                        finGubunSelect.dispatchEvent(event);
                        
                        return true;
                    }}
                }}
                return false;
            }}
            """
            
            result = await self.page.evaluate(js_select_script)
            if result:
                print(f"✅ JavaScript로 '{finGubun_type}' finGubun 선택 성공!")
                await self.page.wait_for_timeout(1500)  # 데이터 로딩 대기
                return True
                
            print(f"❌ '{finGubun_type}' finGubun 콤보박스를 찾을 수 없습니다.")
            
            # 디버깅: 사용 가능한 옵션들 확인
            print(f"🔍 사용 가능한 finGubun 옵션들 확인 중...")
            available_options = await self.page.evaluate("""
            () => {
                const finGubunSelect = document.getElementById('finGubun') || 
                                     document.querySelector('select[id*="finGubun"]') ||
                                     document.querySelector('select[name*="finGubun"]');
                
                if (finGubunSelect) {
                    const options = Array.from(finGubunSelect.options);
                    return options.map(option => ({
                        value: option.value,
                        text: option.text,
                        selected: option.selected
                    }));
                }
                return null;
            }
            """)
            
            if available_options:
                print(f"📋 사용 가능한 finGubun 옵션들:")
                for i, option in enumerate(available_options):
                    selected_mark = " [선택됨]" if option['selected'] else ""
                    print(f"   {i+1}. {option['text']} (value: {option['value']}){selected_mark}")
            else:
                print(f"❌ finGubun 콤보박스 자체를 찾을 수 없습니다.")
                
            return False
            
        except Exception as e:
            print(f"❌ '{finGubun_type}' finGubun 선택 중 오류 발생: {str(e)}")
            return False

    async def select_period_type(self, period_type="연간"):
        """
        연간/분기 라디오 버튼 선택
        
        Args:
            period_type (str): "연간" 또는 "분기"
            
        Returns:
            bool: 성공 여부
        """
        try:
            print(f"📅 '{period_type}' 기간 타입 선택 시도 중...")
            
            # 다양한 셀렉터로 라디오 버튼 찾기
            selectors = [
                f"input[type='radio'][value*='{period_type}']",
                f"input[type='radio'] + label:has-text('{period_type}')",
                f"label:has-text('{period_type}') input[type='radio']",
                f"[for*='{period_type}']",
                f"input[id*='{period_type}']",
                f"input[name*='period'][value*='{period_type}']",
                f"input[name*='term'][value*='{period_type}']"
            ]
            
            # 각 셀렉터로 시도
            for selector in selectors:
                try:
                    element = self.page.locator(selector).first
                    if await element.is_visible():
                        await element.scroll_into_view_if_needed()
                        await self.page.wait_for_timeout(500)
                        await element.click()
                        print(f"✅ '{period_type}' 라디오 버튼 클릭 성공!")
                        await self.page.wait_for_timeout(1500)  # 데이터 로딩 대기
                        return True
                except Exception as e:
                    continue
            
            # JavaScript로 직접 라디오 버튼 클릭 시도
            print(f"🔄 JavaScript로 '{period_type}' 라디오 버튼 클릭 시도...")
            
            js_click_script = f"""
            () => {{
                // 라디오 버튼 찾기
                const radioButtons = Array.from(document.querySelectorAll('input[type="radio"]'));
                let targetRadio = null;
                
                // value 속성에서 찾기
                targetRadio = radioButtons.find(radio => 
                    radio.value && radio.value.includes('{period_type}')
                );
                
                // 라벨 텍스트에서 찾기
                if (!targetRadio) {{
                    for (const radio of radioButtons) {{
                        const label = document.querySelector(`label[for="${{radio.id}}"]`);
                        if (label && label.textContent.includes('{period_type}')) {{
                            targetRadio = radio;
                            break;
                        }}
                        
                        // 부모 라벨 확인
                        const parentLabel = radio.closest('label');
                        if (parentLabel && parentLabel.textContent.includes('{period_type}')) {{
                            targetRadio = radio;
                            break;
                        }}
                    }}
                }}
                
                // 텍스트 주변 라디오 버튼 찾기
                if (!targetRadio) {{
                    const spans = Array.from(document.querySelectorAll('span, div, td'));
                    for (const span of spans) {{
                        if (span.textContent && span.textContent.trim() === '{period_type}') {{
                            const nearbyRadio = span.parentElement?.querySelector('input[type="radio"]') ||
                                              span.querySelector('input[type="radio"]') ||
                                              span.previousElementSibling?.querySelector('input[type="radio"]') ||
                                              span.nextElementSibling?.querySelector('input[type="radio"]');
                            if (nearbyRadio) {{
                                targetRadio = nearbyRadio;
                                break;
                            }}
                        }}
                    }}
                }}
                
                if (targetRadio) {{
                    targetRadio.checked = true;
                    targetRadio.click();
                    
                    // change 이벤트 발생
                    const event = new Event('change', {{ bubbles: true }});
                    targetRadio.dispatchEvent(event);
                    
                    return true;
                }}
                return false;
            }}
            """
            
            result = await self.page.evaluate(js_click_script)
            if result:
                print(f"✅ JavaScript로 '{period_type}' 라디오 버튼 클릭 성공!")
                await self.page.wait_for_timeout(1500)  # 데이터 로딩 대기
                return True
                
            print(f"❌ '{period_type}' 라디오 버튼을 찾을 수 없습니다.")
            return False
            
        except Exception as e:
            print(f"❌ '{period_type}' 라디오 버튼 클릭 중 오류 발생: {str(e)}")
            return False

    async def click_tab(self, tab_name):
        """
        투자분석 탭 클릭
        
        Args:
            tab_name (str): 클릭할 탭 이름
            
        Returns:
            bool: 성공 여부
        """
        try:
            print(f"🖱️ '{tab_name}' 탭 클릭 시도 중...")
            
            # 다양한 셀렉터로 탭 찾기
            selectors = [
                f"text={tab_name}",
                f"a:has-text('{tab_name}')",
                f"span:has-text('{tab_name}')",
                f"button:has-text('{tab_name}')",
                f"td:has-text('{tab_name}')",
                f"div:has-text('{tab_name}')",
                f"[onclick*='{tab_name}']",
                f"[href*='{tab_name}']"
            ]
            
            # 각 셀렉터로 시도
            for selector in selectors:
                try:
                    # 요소가 보이는지 확인
                    element = self.page.locator(selector).first
                    if await element.is_visible():
                        # 요소를 화면에 스크롤
                        await element.scroll_into_view_if_needed()
                        await self.page.wait_for_timeout(500)
                        
                        # 클릭
                        await element.click()
                        print(f"✅ '{tab_name}' 탭 클릭 성공!")
                        
                        # 데이터 로딩 대기
                        await self.page.wait_for_timeout(1500)
                        return True
                        
                except Exception as e:
                    continue
                    
            # JavaScript로 직접 클릭 시도
            print(f"🔄 JavaScript로 '{tab_name}' 탭 클릭 시도...")
            
            js_click_script = f"""
            () => {{
                const elements = Array.from(document.querySelectorAll('*'));
                const targetElement = elements.find(el => 
                    el.textContent && el.textContent.trim() === '{tab_name}' &&
                    (el.tagName === 'A' || el.tagName === 'SPAN' || el.tagName === 'BUTTON' || el.tagName === 'TD')
                );
                
                if (targetElement) {{
                    targetElement.click();
                    return true;
                }}
                return false;
            }}
            """
            
            result = await self.page.evaluate(js_click_script)
            if result:
                print(f"✅ JavaScript로 '{tab_name}' 탭 클릭 성공!")
                await self.page.wait_for_timeout(1500)
                return True
                
            print(f"❌ '{tab_name}' 탭을 찾을 수 없습니다.")
            return False
            
        except Exception as e:
            print(f"❌ '{tab_name}' 탭 클릭 중 오류 발생: {str(e)}")
            return False
            
    async def extract_table_data(self, tab_name, finGubun_type="K-IFRS(연결)"):
        """
        현재 화면의 테이블 데이터 추출 (특정 키워드가 포함된 테이블만)
        
        Args:
            tab_name (str): 현재 탭 이름
            finGubun_type (str): finGubun 구분값 ("K-IFRS(연결)" 또는 "K-IFRS(별도)")
            
        Returns:
            pandas.DataFrame: 추출된 데이터
        """
        try:
            print(f"📊 '{tab_name}' 탭에서 테이블 데이터 추출 중...")
            
            # 테이블 로딩 대기
            await self.page.wait_for_timeout(1500)
            
            # 간단한 테이블 개수 확인
            table_count = await self.page.evaluate("""
            () => {
                const tables = document.querySelectorAll('table.gHead01.all-width.data-list');
                return tables.length;
            }
            """)
            
            print(f"🔍 페이지에서 발견된 target 테이블 수: {table_count}개")
            
            # 탭별 키워드 매핑 (finGubun에 따라 활동성 키워드 변경)
            if tab_name == '활동성':
                if finGubun_type == "K-IFRS(연결)":
                    activity_keyword = '자기자본회전율'
                else:  # K-IFRS(별도)
                    activity_keyword = '총자산회전율'
                    
                tab_keywords = {
                    '수익성': '매출총이익률',
                    '성장성': '매출액증가율',
                    '안정성': '부채비율',
                    '활동성': activity_keyword
                }
            else:
                tab_keywords = {
                    '수익성': '매출총이익률',
                    '성장성': '매출액증가율',
                    '안정성': '부채비율',
                    '활동성': '총자산회전율'  # 기본값
                }
            
            keyword = tab_keywords.get(tab_name, '')
            if not keyword:
                print(f"❌ '{tab_name}' 탭에 대한 키워드가 정의되지 않았습니다.")
                return pd.DataFrame()
            
            print(f"🔍 '{keyword}' 키워드가 포함된 테이블을 찾는 중... (finGubun: {finGubun_type})")
            print(f"📋 현재 탭: {tab_name}, 사용할 키워드: {keyword}")
            
            # JavaScript로 키워드가 포함된 테이블 찾기
            table_data = await self.page.evaluate(f"""
            () => {{
                const keyword = '{keyword}';
                const results = [];
                
                // 모든 테이블 검사
                const allTables = document.querySelectorAll('table.gHead01.all-width.data-list');
                console.log('Found all target tables:', allTables.length);
                
                for (const table of allTables) {{
                    // 테이블 내용에서 키워드 검색
                    const tableText = table.textContent;
                    if (tableText.includes(keyword)) {{
                        console.log('Found table with keyword:', keyword);
                        
                        const rows = table.querySelectorAll('tr');
                        if (rows.length > 1) {{
                            const tableData = [];
                            
                            for (const row of rows) {{
                                const cells = row.querySelectorAll('td, th');
                                if (cells.length > 0) {{
                                    const rowData = [];
                                    for (const cell of cells) {{
                                        const text = cell.textContent.trim();
                                        rowData.push(text);
                                    }}
                                    if (rowData.some(cell => cell)) {{
                                        tableData.push(rowData);
                                    }}
                                }}
                            }}
                            
                            if (tableData.length > 1) {{
                                results.push(tableData);
                                console.log('Extracted table data rows:', tableData.length);
                                break; // 첫 번째 매칭 테이블만 사용
                            }}
                        }}
                    }}
                }}
                
                if (results.length === 0) {{
                    console.log('No table found with keyword:', keyword);
                    // 대체: 첫 번째 유효한 테이블 사용
                    for (const table of allTables) {{
                        const rows = table.querySelectorAll('tr');
                        if (rows.length > 5) {{ // 최소 5행 이상인 테이블
                            const tableData = [];
                            for (const row of rows) {{
                                const cells = row.querySelectorAll('td, th');
                                if (cells.length > 0) {{
                                    const rowData = [];
                                    for (const cell of cells) {{
                                        const text = cell.textContent.trim();
                                        rowData.push(text);
                                    }}
                                    if (rowData.some(cell => cell)) {{
                                        tableData.push(rowData);
                                    }}
                                }}
                            }}
                            if (tableData.length > 1) {{
                                results.push(tableData);
                                console.log('Using fallback table with rows:', tableData.length);
                                break;
                            }}
                        }}
                    }}
                }}
                
                return results;
            }}
            """)
            
            if table_data:
                # 가장 많은 컬럼을 가진 테이블 선택 (투자분석 데이터는 연도별 컬럼이 많음)
                best_table = max(table_data, key=lambda t: len(t[0]) if t else 0)
                
                if len(best_table) > 1:
                    headers = best_table[0]
                    data_rows = best_table[1:]
                    
                    # 컬럼 수 맞추기
                    max_cols = max(len(headers), max(len(row) for row in data_rows))
                    
                    # 헤더 조정 및 중복 제거
                    while len(headers) < max_cols:
                        headers.append(f'Column_{len(headers)+1}')
                    headers = headers[:max_cols]
                    
                    # 중복된 컬럼명 처리
                    seen = {}
                    for i, header in enumerate(headers):
                        if header in seen:
                            seen[header] += 1
                            headers[i] = f"{header}_{seen[header]}"
                        else:
                            seen[header] = 0
                    
                    # 컬럼명 정리
                    headers = [self._clean_column_name(header) for header in headers]
                    
                    # 데이터 행 조정
                    adjusted_data = []
                    for row in data_rows:
                        adjusted_row = row[:max_cols]
                        while len(adjusted_row) < max_cols:
                            adjusted_row.append('')
                        adjusted_data.append(adjusted_row)
                    
                    df = pd.DataFrame(adjusted_data, columns=headers)
                    
                    # 숫자값에서 콤마 제거
                    df = self._clean_numeric_values(df)
                    
                    # 계층 구조 파싱 (id와 parent_id 컬럼 추가)
                    df = self._add_hierarchy_columns(df)
                    
                    print(f"✅ '{tab_name}' 탭에서 {len(df)}행의 데이터를 추출했습니다.")
                    return df
                    
            print(f"❌ '{tab_name}' 탭에서 테이블 데이터를 찾을 수 없습니다.")
            return pd.DataFrame()
            
        except Exception as e:
            print(f"❌ '{tab_name}' 탭에서 데이터 추출 중 오류 발생: {str(e)}")
            return pd.DataFrame()
    
    def _add_hierarchy_columns(self, df):
        """
        데이터프레임에 계층 구조를 나타내는 id와 parent_id 컬럼을 추가
        
        Args:
            df (pandas.DataFrame): 원본 데이터프레임
            
        Returns:
            pandas.DataFrame: id, parent_id 컬럼이 추가된 데이터프레임
        """
        try:
            if df.empty or len(df.columns) == 0:
                return df
                
            # 첫 번째 컬럼을 항목명으로 가정
            item_column = df.columns[0]
            
            # id와 parent_id 컬럼 초기화 (parent_id는 공백으로 설정)
            df['id'] = range(1, len(df) + 1)
            df['parent_id'] = ''
            
            # 계층 구조 파싱 - 간단한 방식
            last_parent_id = None  # 가장 최근의 "펼치기" 항목 ID
            
            for idx, row in df.iterrows():
                item_text = str(row[item_column]).strip()
                current_id = row['id']
                
                # "펼치기"가 있는지 확인 (텍스트 정리 전에)
                is_parent = self._is_parent_item(item_text)
                
                if is_parent:
                    # "펼치기"가 있으면 상위 객체 → 부모 ID 업데이트
                    last_parent_id = current_id
                    # 상위 객체는 parent_id가 공백 (최상위)
                    df.at[idx, 'parent_id'] = ''
                else:
                    # "펼치기"가 없으면 하위 객체 → 가장 최근 부모의 자식
                    if last_parent_id is not None:
                        df.at[idx, 'parent_id'] = last_parent_id
                    else:
                        df.at[idx, 'parent_id'] = ''
                
                # 마지막에 텍스트 정리 (펼치기 제거)
                clean_text = self._clean_item_text(item_text)
                df.at[idx, item_column] = clean_text
                    
            # 컬럼 순서 재정렬 (id, parent_id를 맨 앞으로)
            cols = ['id', 'parent_id'] + [col for col in df.columns if col not in ['id', 'parent_id']]
            df = df[cols]
            
            # 계층 구조 분석 결과 출력
            parent_count = len([x for x in df['parent_id'] if x != ''])
            parent_items = len([True for idx, row in df.iterrows() if self._is_parent_item(str(row[item_column]) + ("펼치기" if row.get('parent_id') != '' else ""))])
            
            print(f"🔗 계층 구조 파싱 완료:")
            print(f"   - 하위 항목: {parent_count}개")
            print(f"   - 상위 항목: {len(df) - parent_count}개")
            
            # 디버깅: 처음 몇 개 항목의 계층 구조 출력
            if len(df) > 0:
                print(f"📋 계층 구조 예시 (처음 5개):")
                for i, (idx, row) in enumerate(df.head(5).iterrows()):
                    item_name = str(row[item_column])[:30] + "..." if len(str(row[item_column])) > 30 else str(row[item_column])
                    parent_info = f"→ 부모ID: {row['parent_id']}" if row['parent_id'] != '' else "→ 최상위"
                    print(f"   {row['id']:2d}. {item_name:35s} {parent_info}")
            
            return df
            
        except Exception as e:
            print(f"❌ 계층 구조 파싱 중 오류: {str(e)}")
            # 오류 발생시 기본 id만 추가하고 반환
            df['id'] = range(1, len(df) + 1)
            df['parent_id'] = ''
            return df
    
    def _clean_numeric_values(self, df):
        """
        DataFrame의 숫자값에서 콤마를 제거
        
        Args:
            df (pd.DataFrame): 원본 DataFrame
            
        Returns:
            pd.DataFrame: 콤마가 제거된 DataFrame
        """
        try:
            # 모든 컬럼에 대해 콤마 제거 (id, parent_id, company_code는 제외)
            for col in df.columns:
                if col not in ['id', 'parent_id', 'company_code'] and len(df.columns) > 1:
                    # 문자열로 변환 후 콤마 제거
                    df[col] = df[col].astype(str).str.replace(',', '', regex=False)
            
            print(f"🧹 숫자값 콤마 제거 완료")
            return df
            
        except Exception as e:
            print(f"⚠️ 숫자값 정리 중 오류: {str(e)}")
            return df
    
    def _clean_column_name(self, column_name):
        """
        컬럼명을 정리하는 함수
        
        Args:
            column_name (str): 원본 컬럼명
            
        Returns:
            str: 정리된 컬럼명
        """
        if not column_name:
            return ""
        
        # 1. 연간컨센서스보기 패턴 제거: "\n...보기" -> ""
        if "연간컨센서스보기" in column_name:
            column_name = re.sub(r'\n.*?보기', '', column_name)
        
        # 2. 연간컨센서스닫기 패턴 변환: "\n...닫기" -> "(연간컨센서스)"
        if "연간컨센서스닫기" in column_name:
            column_name = re.sub(r'\n.*?닫기', '(연간컨센서스)', column_name)
        
        # 3. _1 패턴 변환: "_1" -> "(연간컨센서스)"
        if column_name.endswith('_1'):
            column_name = column_name.replace('_1', '(연간컨센서스)')
        
        # 4. 기타 개행문자와 탭 정리
        column_name = re.sub(r'[\n\t\r]+', ' ', column_name)
        
        # 5. 연속된 공백을 하나로 변환
        column_name = re.sub(r'\s+', ' ', column_name)
        
        return column_name.strip()
    
    def _clean_item_text(self, text):
        """
        항목 텍스트에서 불필요한 문자 제거
        
        Args:
            text (str): 원본 텍스트
            
        Returns:
            str: 정리된 텍스트
        """
        if not text:
            return text
            
        # "펼치기" 텍스트와 관련 문자 제거
        clean_text = text.replace('펼치기', '').strip()
        
        # 탭과 과도한 공백 정리
        clean_text = ' '.join(clean_text.split())
        
        # 접기/펼치기 관련 특수문자 제거
        clean_text = clean_text.replace('▼', '').replace('▲', '').replace('△', '').replace('▽', '')
        clean_text = clean_text.replace('+', '').replace('-', '').strip()
        
        return clean_text
    
    def _is_parent_item(self, text):
        """
        해당 항목이 상위 항목(접기/펼치기가 가능한 항목)인지 판단
        
        Args:
            text (str): 분석할 텍스트
            
        Returns:
            bool: 상위 항목 여부
        """
        if not text:
            return False
            
        # "펼치기" 키워드가 있으면 상위 항목
        if '펼치기' in text:
            return True
            
        # 접기/펼치기 관련 특수문자가 있으면 상위 항목
        expand_collapse_chars = ['▼', '▲', '△', '▽', '+', '-']
        if any(char in text for char in expand_collapse_chars):
            return True
            
        # 특정 패턴의 항목명 (예: "수익성 지표", "성장성 분석" 등)
        parent_keywords = ['지표', '분석', '비율', '현황', '상황', '내역']
        clean_text = self._clean_item_text(text)
        if any(keyword in clean_text for keyword in parent_keywords):
            return True
            
        return False

    def extract_year_month_from_data(self, df):
        """
        크롤링된 데이터에서 실제 연도/월 추출
        
        Args:
            df (pd.DataFrame): 크롤링된 데이터
            
        Returns:
            tuple: (year, month) 또는 (None, None)
        """
        if df.empty or 'yyyy' not in df.columns or 'month' not in df.columns:
            return None, None
        
        # 가장 많이 나타나는 연도/월 찾기
        year_counts = df['yyyy'].value_counts()
        month_counts = df['month'].value_counts()
        
        if not year_counts.empty and not month_counts.empty:
            most_common_year = year_counts.index[0]
            most_common_month = month_counts.index[0]
            
            print(f"📅 크롤링된 데이터에서 추출된 연도/월: {most_common_year}/{most_common_month}")
            return most_common_year, most_common_month
        
        return None, None

    def save_data_by_yyyymm(self, df, output_dir, period_type, s3_bucket=None, save_local=True):
        """
        yyyymm별로 데이터를 분리하여 저장
        
        Args:
            df (pd.DataFrame): 변환된 데이터
            output_dir (str): 출력 디렉토리
            period_type (str): "연간" 또는 "분기"
            s3_bucket (str): S3 버킷명 (선택사항)
            save_local (bool): 로컬 저장 여부 (기본값: True)
        """
        if df.empty or 'yyyy' not in df.columns or 'month' not in df.columns:
            print("❌ yyyy, month 컬럼이 없어 데이터를 분리할 수 없습니다.")
            return
        
        # yyyymm 컬럼 생성
        df['yyyymm'] = df['yyyy'].astype(str) + df['month'].astype(str).str.zfill(2)
        
        # 고유한 yyyymm 값들 찾기
        unique_yyyymm = sorted(df['yyyymm'].unique())
        print(f"📅 발견된 yyyymm: {unique_yyyymm}")
        
        # 가장 큰 yyyymm 찾기 (분석 데이터용)
        max_yyyymm = max(unique_yyyymm)
        print(f"📅 분석 데이터(전년대비/전분기대비)가 포함될 최신 년월: {max_yyyymm}")

        # 각 yyyymm별로 데이터 분리하여 저장
        for yyyymm in unique_yyyymm:
            # 해당 yyyymm 데이터만 필터링
            yyyymm_data = df[df['yyyymm'] == yyyymm].copy()

            # 분석 데이터는 최신 년월에만 포함
            if yyyymm != max_yyyymm and 'column_type' in yyyymm_data.columns:
                # 최신 년월이 아니면 분석 데이터 제외
                filtered_df = yyyymm_data[yyyymm_data['column_type'] != 'analysis_data'].copy()
                excluded_count = len(yyyymm_data) - len(filtered_df)
                if excluded_count > 0:
                    print(f"📊 {yyyymm}: 분석 데이터 {excluded_count}개 제외 (최신 년월 {max_yyyymm}에만 포함)")
            else:
                # 최신 년월이거나 column_type 컬럼이 없으면 모든 데이터 포함
                filtered_df = yyyymm_data.copy()
            
            if not filtered_df.empty:
                # 파일명 생성
                period_suffix = "_annual" if period_type == "연간" else "_quarterly"
                filename = f"{output_dir}/{yyyymm}_all_companies{period_suffix}_transformed.csv"
                
                # 로컬 CSV 저장 (save_local이 True인 경우)
                if save_local:
                    # 저장 전 데이터 타입 조정: value 컬럼만 숫자로, 나머지는 문자열로
                    df_to_save = filtered_df.copy()
                    for col in df_to_save.columns:
                        if col != 'value':
                            df_to_save[col] = df_to_save[col].astype(str)
                    
                    df_to_save.to_csv(filename, index=False, encoding='utf-8-sig')
                    print(f"💾 {yyyymm} 로컬 데이터 저장: {filename} ({len(filtered_df)}행)")
                else:
                    print(f"⏭️ {yyyymm} 로컬 저장 생략 (save_local=False)")
                
                # S3 업로드 (버킷이 지정된 경우)
                if s3_bucket:
                    try:
                        # 기간 타입을 영어로 변환
                        period_type_en = "annual" if period_type == "연간" else "quarter"
                        
                        # yyyymm에서 연도와 월 추출
                        year = yyyymm[:4]
                        month = yyyymm[4:6]
                        
                        # S3 키 생성
                        s3_key = generate_s3_key(period_type_en, year, month)
                        
                        print(f"📤 S3 업로드 준비: s3://{s3_bucket}/{s3_key}")
                        print(f"📅 데이터 연도/월: {year}/{month}")
                        
                        # S3 업로드 (로컬 파일이 있어야 업로드 가능)
                        if save_local:
                            s3_upload_result = upload_file_to_s3(filename, s3_bucket, s3_key)
                            
                            if s3_upload_result.get("success"):
                                print(f"✅ S3 업로드 성공: {s3_upload_result['s3_url']}")
                                print(f"📦 파일 크기: {s3_upload_result['size']} bytes")
                            else:
                                print(f"❌ S3 업로드 실패: {s3_upload_result.get('error', '알 수 없는 오류')}")
                        else:
                            print(f"⚠️ S3 업로드 건너뜀: 로컬 파일이 생성되지 않았습니다 (save_local=False)")
                            
                    except Exception as e:
                        print(f"❌ S3 업로드 과정에서 오류: {str(e)}")
            else:
                print(f"⚠️ {yyyymm} 데이터가 비어있어 저장하지 않았습니다.")

            
    async def cleanup(self):
        """브라우저 종료 및 정리 - 강화된 버전 (타임아웃 및 오류 처리)"""
        cleanup_errors = []

        # Page 정리
        try:
            if self.page:
                try:
                    await asyncio.wait_for(self.page.close(), timeout=5)
                    print("✅ Page closed")
                except asyncio.TimeoutError:
                    cleanup_errors.append("Page close timeout (5s)")
                except Exception as e:
                    cleanup_errors.append(f"Page close error: {e}")
        except Exception as e:
            cleanup_errors.append(f"Page cleanup error: {e}")

        # Context 정리
        try:
            if hasattr(self, 'context') and self.context:
                try:
                    await asyncio.wait_for(self.context.close(), timeout=5)
                    print("✅ Context closed")
                except asyncio.TimeoutError:
                    cleanup_errors.append("Context close timeout (5s)")
                except Exception as e:
                    cleanup_errors.append(f"Context close error: {e}")
        except Exception as e:
            cleanup_errors.append(f"Context cleanup error: {e}")

        # Browser 정리
        try:
            if self.browser:
                try:
                    await asyncio.wait_for(self.browser.close(), timeout=10)
                    print("✅ Browser closed")
                except asyncio.TimeoutError:
                    cleanup_errors.append("Browser close timeout (10s)")
                except Exception as e:
                    cleanup_errors.append(f"Browser close error: {e}")
        except Exception as e:
            cleanup_errors.append(f"Browser cleanup error: {e}")

        # Playwright 정리
        try:
            if hasattr(self, 'playwright'):
                try:
                    await asyncio.wait_for(self.playwright.stop(), timeout=10)
                    print("✅ Playwright stopped")
                except asyncio.TimeoutError:
                    cleanup_errors.append("Playwright stop timeout (10s)")
                except Exception as e:
                    cleanup_errors.append(f"Playwright stop error: {e}")
        except Exception as e:
            cleanup_errors.append(f"Playwright cleanup error: {e}")

        # 최종 결과 출력
        if cleanup_errors:
            print(f"⚠️ Cleanup completed with warnings: {'; '.join(cleanup_errors)}")
        else:
            print("🧹 브라우저를 완전히 종료했습니다.")

        # Lambda/MWAA 환경에서 좀비 프로세스 방지
        import os
        if os.environ.get('AWS_LAMBDA_FUNCTION_NAME') or os.environ.get('AIRFLOW_HOME'):
            print("🔧 Lambda/MWAA 환경: 추가 정리 작업 수행")
            # 추가 대기 시간으로 프로세스 완전 종료 보장
            await asyncio.sleep(1)
    
    async def close_browser(self):
        """브라우저 정리 (별칭 메서드)"""
        await self.cleanup()
            
    async def _crawl_single_company(self, url, company_code, company_name, period_type="연간"):
        """
        단일 회사 크롤링 (브라우저 재사용)
        
        Args:
            url (str): 크롤링할 URL
            company_code (str): 회사 코드
            company_name (str): 회사명
            period_type (str): "연간" 또는 "분기"
            
        Returns:
            dict: 크롤링 결과
        """
        try:
            # 페이지 이동
            await self.navigate_to_page(url)
            
            # 기간 타입 선택 (연간/분기)
            print(f"📅 {company_name}: 기간 타입 '{period_type}' 선택 중...")
            if period_type == "annual":
                period_type = "연간"
            elif period_type == "quarter":
                period_type = "분기"
            
            period_selected = await self.select_period_type(period_type)
            if not period_selected:
                print(f"⚠️ {company_name}: '{period_type}' 기간 타입 선택에 실패했지만 크롤링을 계속 진행합니다.")
            
            # finGubun 구분값들
            finGubun_types = ['K-IFRS(연결)', 'K-IFRS(별도)']
            tabs = ['수익성', '성장성', '안정성', '활동성']
            results = {}
            
            # finGubun별로 크롤링
            for finGubun_idx, finGubun_type in enumerate(finGubun_types):
                print(f"\n{'='*50}")
                print(f"[{finGubun_idx+1}/2] {company_name}: '{finGubun_type}' 크롤링 시작")
                print(f"{'='*50}")
                
                # finGubun 선택
                finGubun_selected = await self.select_finGubun(finGubun_type)
                if not finGubun_selected:
                    print(f"⚠️ {company_name}: '{finGubun_type}' finGubun 선택에 실패했지만 크롤링을 계속 진행합니다.")
                    # finGubun 선택 실패 시에도 탭 크롤링을 시도해보자
                else:
                    print(f"✅ {company_name}: '{finGubun_type}' finGubun 선택 성공!")
                
                # 각 탭별로 크롤링
                for i, tab in enumerate(tabs):
                    print(f"[{i+1}/4] {finGubun_type} - {tab} 탭 크롤링 중...")
                    
                    if await self.click_tab(tab):
                        df = await self.extract_table_data(tab, finGubun_type)
                        if not df.empty:
                            # finGubun 정보를 데이터에 추가 (중복 체크)
                            if 'finGubun' not in df.columns:
                                if len(df.columns) > 0:
                                    df.insert(0, 'finGubun', finGubun_type)
                                else:
                                    df['finGubun'] = finGubun_type
                            
                            # 결과 저장 (키에 finGubun 포함)
                            result_key = f"{finGubun_type}_{tab}"
                            results[result_key] = df
                            print(f"✅ {finGubun_type} - {tab}: {len(df)}행")
                        else:
                            print(f"❌ {finGubun_type} - {tab}: 데이터 없음")
                            print(f"🔍 {finGubun_type} - {tab}: 빈 테이블인지 확인 중...")
                            # 빈 테이블에 대한 추가 정보 출력
                    else:
                        print(f"❌ {finGubun_type} - {tab}: 탭 클릭 실패")
                    
                    # 탭 간 대기
                    await self.page.wait_for_timeout(1000)
                
                # finGubun 구분 간 대기
                if finGubun_idx < len(finGubun_types) - 1:
                    print(f"⏳ 다음 finGubun 크롤링을 위해 1초 대기...")
                    await self.page.wait_for_timeout(1000)
                
            return results
            
        except Exception as e:
            print(f"❌ {company_name} 크롤링 중 오류: {str(e)}")
            return {}
    
    
            
    
    
    def _extract_data_type_from_column(self, column_name):
        """
        컬럼명에서 데이터 타입을 추출하는 함수 (finGubun 값에서 연결/별도만 추출)

        Args:
            column_name (str): 컬럼명 (예: "2020/12(IFRS연결)")

        Returns:
            str: 데이터 타입 (예: "연결", "별도")
        """
        try:
            if not column_name or '(' not in column_name:
                return '연결'  # 기본값

            import re

            # IFRS/GAAP 관련 괄호 내용에서 연결/별도 추출
            ifrs_pattern = r'\(([^()]*(?:IFRS|GAAP)[^()]*)\)'
            matches = re.findall(ifrs_pattern, column_name)

            if matches:
                data_type = matches[0]  # 첫 번째 IFRS 관련 매치

                # 연결/별도만 추출
                if '연결' in data_type:
                    return '연결'
                elif '별도' in data_type:
                    return '별도'
                else:
                    return '연결'  # 기본값

            # IFRS 패턴이 없으면 첫 번째 괄호에서 연결/별도 찾기
            first_paren_content = re.search(r'\((.*?)\)', column_name)
            if first_paren_content:
                content = first_paren_content.group(1)

                # 연결/별도 키워드 검색
                if '연결' in content:
                    return '연결'
                elif '별도' in content:
                    return '별도'

            return '연결'  # 기본값

        except Exception as e:
            print(f"⚠️ 데이터 타입 추출 중 오류: {str(e)}")
            return '연결'  # 기본값
    
    def transform_to_row_format(self, combined_df, period_type="연간"):
        """
        컬럼 기반 데이터를 row 기반으로 변환하는 메소드

        Args:
            combined_df (pd.DataFrame): 크롤링된 원본 데이터
            period_type (str): 조회 기간 타입 ("연간" 또는 "분기")

        Returns:
            pd.DataFrame: 변환된 데이터
        """
        try:
            print(f"📊 데이터 변환 시작: {combined_df.shape}")
            
            # 변환할 컬럼들 식별
            # 1. 연도 컬럼들 (yyyy/mm 패턴이 있는 컬럼 - 연결/별도 구분 없이 모든 재무 데이터)
            import re
            year_pattern = r'\d{4}/\d{2}'
            year_columns = [col for col in combined_df.columns if re.search(year_pattern, col)]
            
            # 2. 분석 컬럼들 (기간 타입에 따라 분기/연간 구분)
            if period_type == "분기":
                # 분기 조회: QoQ, 전분기대비 등 분기 관련 분석 컬럼만
                analysis_keywords = ['QoQ', '전분기대비', '분기증감률']
            else:
                # 연간 조회: YoY, 전년대비 등 연간 관련 분석 컬럼만
                analysis_keywords = ['YoY', '전년대비', '증감률', 'CAGR']

            analysis_columns = [col for col in combined_df.columns if any(keyword in col for keyword in analysis_keywords)]
            
            # 3. 전체 변환 대상 컬럼
            target_columns = year_columns + analysis_columns
            
            print(f"🔍 변환할 연도 컬럼들: {year_columns}")
            print(f"📈 변환할 분석 컬럼들: {analysis_columns}")
            print(f"📊 전체 변환 대상: {len(target_columns)}개 컬럼")
            
            if not target_columns:
                print("❌ 변환할 컬럼을 찾을 수 없습니다.")
                return pd.DataFrame()
            
            # 데이터 변환을 위한 리스트
            transformed_data = []
            
            print("🔄 데이터 변환 중...")
            
            # 가장 큰 연도/월 찾기 (분석 데이터 매핑용) - 정규식으로 안전하게 추출
            max_year = ''
            max_month = ''
            for year_col in year_columns:
                year_match = re.search(year_pattern, year_col)
                if year_match:
                    year_period = year_match.group()  # "2024/09"
                    yy, mm = year_period.split('/')
                    if yy.isdigit() and (not max_year or yy > max_year):
                        max_year = yy
                        max_month = mm
            
            print(f"📅 분석 데이터 매핑 기준: {max_year}/{max_month}")
            
            # 먼저 연도 컬럼들만 처리 (모든 행에서)
            for _, row in combined_df.iterrows():
                for target_col in year_columns:
                    # 연도 컬럼 처리 - 정규식으로 yyyy/mm 패턴 추출
                    year_match = re.search(year_pattern, target_col)
                    if year_match:
                        year_period = year_match.group()  # "2024/09"
                        yy, mm = year_period.split('/')
                    else:
                        yy, mm = '', ''
                    data_type = self._extract_data_type_from_column(target_col)
                    # period_type에 따라 column_type 구분
                    column_type = 'period_data' if period_type == "분기" else 'year_data'
                    # value_type 결정 (E가 있으면 Expected, 없으면 Real)
                    value_type = 'Expected' if '(E)' in target_col else 'Real'

                    # 값이 비어있지 않은 경우만 추가
                    value = row[target_col]
                    if pd.notna(value) and str(value).strip() != '':
                        # 조회구분 정보 가져오기 (없으면 기본값)
                        inquiry_type = row.get('search_type', '연간')

                        # 기본 행 데이터
                        transformed_row = {
                            'tab': row['tab'],
                            'search_type': inquiry_type,
                            'id': row['id'],
                            'parent_id': row['parent_id'],
                            'item': row['항목'],
                            'column_name': target_col,  # 원본 컬럼명 추가
                            'column_type': column_type,  # 컬럼 유형 추가
                            'yyyy': yy,
                            'month': mm,
                            'value': value,
                            'value_type': value_type,  # Expected/Real 구분
                            'data_type': data_type,
                            'crawl_time': datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%d %H:%M:%S") # KST 시간 추가
                        }

                        # company_code가 있으면 추가 (6자리 문자열로 보장)
                        if 'company_code' in combined_df.columns:
                            company_code = str(row['company_code']).zfill(6)
                            transformed_row['company_code'] = company_code
                        else:
                            # 단일 회사 크롤링인 경우 기본값 설정
                            transformed_row['company_code'] = '004150'

                        # company_name이 있으면 추가
                        if 'company_name' in combined_df.columns:
                            transformed_row['company_name'] = row['company_name']
                        else:
                            # 단일 회사 크롤링인 경우 기본값 설정
                            transformed_row['company_name'] = '한솔홀딩스'

                        # finGubun이 있으면 추가
                        if 'finGubun' in combined_df.columns:
                            transformed_row['finGubun'] = row['finGubun']
                        else:
                            # finGubun이 없는 경우 기본값 설정
                            transformed_row['finGubun'] = 'K-IFRS(연결)'

                        transformed_data.append(transformed_row)

            # 분석 컬럼들은 최신 년월에만 추가 (한 번만)
            if analysis_columns:
                print(f"📊 분석 데이터는 최신 년월 {max_year}/{max_month}에만 추가됩니다.")
                for _, row in combined_df.iterrows():
                    for target_col in analysis_columns:
                        # 분석 컬럼 처리 (전년대비 등) - 가장 큰 연도/월에 매핑
                        yy = max_year
                        mm = max_month
                        data_type = self._extract_data_type_from_column(target_col) if '(' in target_col else 'analysis'
                        column_type = 'analysis_data'
                        # 분석 데이터는 모두 Real (실제 계산된 값)
                        value_type = 'Real'

                        # 값이 비어있지 않은 경우만 추가
                        value = row[target_col]
                        if pd.notna(value) and str(value).strip() != '':
                            # 조회구분 정보 가져오기 (없으면 기본값)
                            inquiry_type = row.get('search_type', '연간')

                            # 기본 행 데이터
                            transformed_row = {
                                'tab': row['tab'],
                                'search_type': inquiry_type,
                                'id': row['id'],
                                'parent_id': row['parent_id'],
                                'item': row['항목'],
                                'column_name': target_col,  # 원본 컬럼명 추가
                                'column_type': column_type,  # 컬럼 유형 추가
                                'yyyy': yy,
                                'month': mm,
                                'value': value,
                                'value_type': value_type,  # Expected/Real 구분
                                'data_type': data_type,
                                'crawl_time': datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%d %H:%M:%S") # KST 시간 추가
                            }

                            # company_code가 있으면 추가 (6자리 문자열로 보장)
                            if 'company_code' in combined_df.columns:
                                company_code = str(row['company_code']).zfill(6)
                                transformed_row['company_code'] = company_code
                            else:
                                # 단일 회사 크롤링인 경우 기본값 설정
                                transformed_row['company_code'] = '004150'

                            # company_name이 있으면 추가
                            if 'company_name' in combined_df.columns:
                                transformed_row['company_name'] = row['company_name']
                            else:
                                # 단일 회사 크롤링인 경우 기본값 설정
                                transformed_row['company_name'] = '한솔홀딩스'

                            # finGubun이 있으면 추가
                            if 'finGubun' in combined_df.columns:
                                transformed_row['finGubun'] = row['finGubun']
                            else:
                                # finGubun이 없는 경우 기본값 설정
                                transformed_row['finGubun'] = 'K-IFRS(연결)'

                            transformed_data.append(transformed_row)
            
            if not transformed_data:
                print("❌ 변환할 데이터가 없습니다.")
                return pd.DataFrame()
            
            # 새로운 DataFrame 생성
            transformed_df = pd.DataFrame(transformed_data)
            
            # 컬럼 순서 정리 - 실제 존재하는 컬럼만 선택
            print(f"🔍 변환된 DataFrame의 실제 컬럼들: {list(transformed_df.columns)}")
            
            desired_columns = ['company_code', 'company_name', 'finGubun', 'tab', 'search_type', 'id', 'parent_id', 'item', 'column_name', 'column_type', 'yyyy', 'month', 'value', 'value_type', 'data_type', 'crawl_time']
            available_columns = [col for col in desired_columns if col in transformed_df.columns]
            
            print(f"🎯 사용할 컬럼들: {available_columns}")
            
            if available_columns:
                transformed_df = transformed_df[available_columns]
            else:
                print("⚠️ 원하는 컬럼이 없어 원본 컬럼 순서를 유지합니다.")
            
            print(f"✅ 변환 완료: {transformed_df.shape}")
            print(f"📈 총 회사 수: {transformed_df['company_code'].nunique()}")
            print(f"📊 총 재무 항목 수: {transformed_df['item'].nunique()}")
            print(f"📅 포함된 연도: {sorted([y for y in transformed_df['yyyy'].unique() if y])}")
            print(f"🔍 조회구분: {sorted(transformed_df['search_type'].unique())}")
            print(f"📋 finGubun 구분: {sorted(transformed_df['finGubun'].unique())}" if 'finGubun' in transformed_df.columns else "📋 finGubun 구분: 정보 없음")
            print(f"📋 컬럼 유형: {sorted(transformed_df['column_type'].unique())}")
            print(f"💎 값 유형: {sorted(transformed_df['value_type'].unique())}")
            print(f"🏷️ 데이터 타입: {sorted(transformed_df['data_type'].unique())}")
            print(f"📍 총 데이터 포인트: {len(transformed_df)}")
            
            return transformed_df
            
        except Exception as e:
            print(f"❌ 데이터 변환 중 오류 발생: {str(e)}")
            return pd.DataFrame()
            
            




async def crawl_multiple_stocks(stocks_data, output_dir="./crawl_results", period_type="연간", s3_bucket=None, save_local=True):
    """
    여러 주식 데이터를 순차적으로 크롤링
    
    Args:
        stocks_data (list): 주식 정보 리스트 
            [{"code": "004150", "name": "한솔홀딩스"}, {"code": "005930", "name": "삼성전자"}, ...]
        output_dir (str): 결과 파일 저장 디렉토리
        period_type (str): "연간" 또는 "분기"
        s3_bucket (str): S3 버킷명 (선택사항)
        save_local (bool): 로컬 저장 여부 (기본값: True)
    
    Returns:
        dict: 회사별 크롤링 결과
    """
    print(f"🚀 {len(stocks_data)}개 회사의 투자분석 데이터 크롤링을 시작합니다...")
    
    # 결과 저장 디렉토리 생성
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    all_results = {}
    success_count = 0
    failed_companies = []
    
    # 크롤러 초기화 (한 번만 초기화하여 효율성 증대)
    crawler = PlaywrightStockCrawler(headless=True, wait_timeout=15000)
    crawler.start_timer()  # 타이머 시작
    
    try:
        # 브라우저 설정 (한 번만)
        await crawler.setup_browser()
        
        for i, stock_info in enumerate(stocks_data):
            company_code = stock_info.get('code', '')
            company_name = stock_info.get('name', f'Company_{company_code}')
            
            print(f"\n{'='*60}")
            print(f"[{i+1}/{len(stocks_data)}] {company_name} ({company_code}) 크롤링 시작")
            print(f"{'='*60}")
            
            try:
                # URL 생성
                url = f"https://navercomp.wisereport.co.kr/v2/company/c1040001.aspx?cn=&cmp_cd={company_code}&menuType=block"
                
                # 해당 회사 크롤링 (브라우저 재사용)
                results = await crawler._crawl_single_company(url, company_code, company_name, period_type)
                
                if results:
                    all_results[company_code] = {
                        'company_name': company_name,
                        'company_code': company_code,
                        'data': results,
                        'status': 'success'
                    }
                    
                    # 개별 파일 저장은 하지 않음 (최종 통합 파일만 저장)
                    
                    success_count += 1
                    print(f"✅ {company_name} 크롤링 성공!")
                    
                else:
                    failed_companies.append(f"{company_name}({company_code})")
                    print(f"❌ {company_name} 크롤링 실패 - 데이터 없음")
                    
            except Exception as e:
                failed_companies.append(f"{company_name}({company_code})")
                print(f"❌ {company_name} 크롤링 중 오류: {str(e)}")
                
            # 회사 간 대기 (서버 부하 방지)
            if i < len(stocks_data) - 1:  # 마지막이 아니면
                print("⏳ 다음 회사 크롤링을 위해 1초 대기...")
                await asyncio.sleep(1)
                
    finally:
        await crawler.cleanup()
    
    # Lambda 환경에서는 요약 파일 저장 생략 (메모리 절약 및 오류 방지)
    try:
        # 간단한 요약 정보만 생성 (DataFrame 직렬화 없이)
        summary_data = {
            'timestamp': datetime.now(timezone(timedelta(hours=9))).isoformat(),
            'total_companies': len(stocks_data),
            'success_count': success_count,
            'failed_count': len(failed_companies),
            'failed_companies': failed_companies,
            'message': f'{success_count}개 성공, {len(failed_companies)}개 실패'
        }

        # Lambda 환경이 아닌 경우에만 상세 요약 파일 저장
        if not os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
            print("💾 로컬 환경: 상세 요약 파일 생성 중...")

            # DataFrame을 JSON 직렬화 가능한 형태로 변환 (로컬에서만)
            json_compatible_results = {}
            for company_code, company_data in all_results.items():
                json_compatible_results[company_code] = {
                    'company_name': company_data['company_name'],
                    'company_code': company_data['company_code'],
                    'status': company_data.get('status', 'unknown'),
                    'data_count': len(company_data.get('data', {}))
                }

            summary_data['results'] = json_compatible_results

            # 날짜 접두사 추가하여 JSON 저장
            date_prefix = datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")
            summary_filename = f"{output_dir}/{date_prefix}_crawling_summary.json"
            with open(summary_filename, 'w', encoding='utf-8') as f:
                json.dump(summary_data, f, ensure_ascii=False, indent=2)
            print(f"💾 요약 파일 저장 완료: {summary_filename}")
        else:
            print("☁️ Lambda 환경: 상세 요약 파일 저장 생략")

    except Exception as summary_error:
        print(f"⚠️ 요약 파일 생성 중 오류 (무시하고 계속 진행): {str(summary_error)}")
        summary_data = {
            'timestamp': datetime.now(timezone(timedelta(hours=9))).isoformat(),
            'total_companies': len(stocks_data),
            'success_count': success_count,
            'failed_count': len(failed_companies),
            'message': '요약 파일 생성 실패하였으나 크롤링은 완료됨'
        }
    
    # 모든 회사 데이터를 하나의 CSV 파일로 합치기
    combined_csv_data = []
    for company_code, company_data in all_results.items():
        if company_data.get('status') == 'success' and 'data' in company_data:
            for tab_name, df in company_data['data'].items():
                if isinstance(df, pd.DataFrame) and not df.empty:
                    # 회사 정보와 탭 정보 추가
                    df_copy = df.copy()
                    if len(df_copy.columns) > 0:
                        df_copy.insert(0, 'company_code', str(company_code).zfill(6))  # 6자리 문자열로 변환
                        df_copy.insert(1, 'company_name', company_data['company_name'])
                    else:
                        df_copy['company_code'] = str(company_code).zfill(6)
                        df_copy['company_name'] = company_data['company_name']
                    
                    # tab_name에서 finGubun과 실제 탭명 분리
                    if '_' in tab_name and any(tab_name.endswith(f'_{tab}') for tab in ['수익성', '성장성', '안정성', '활동성']):
                        # "K-IFRS(연결)_수익성" 형태에서 분리
                        finGubun_part, actual_tab = tab_name.rsplit('_', 1)
                        
                        # finGubun 컬럼이 없는 경우에만 추가
                        if 'finGubun' not in df_copy.columns:
                            if len(df_copy.columns) >= 2:
                                df_copy.insert(2, 'finGubun', finGubun_part)
                                df_copy.insert(3, 'tab', actual_tab)
                            else:
                                df_copy['finGubun'] = finGubun_part
                                df_copy['tab'] = actual_tab
                        else:
                            # 이미 finGubun 컬럼이 있으면 tab만 추가
                            if len(df_copy.columns) >= 3:
                                df_copy.insert(3, 'tab', actual_tab)
                            else:
                                df_copy['tab'] = actual_tab
                    else:
                        # 기존 형태 (finGubun 정보가 이미 컬럼에 있는 경우)
                        # 안전한 인덱스 계산
                        insert_idx = max(0, len(df_copy.columns) - 1) if len(df_copy.columns) > 0 else 0
                        df_copy.insert(insert_idx, 'tab', tab_name)
                    
                    # 안전한 인덱스 계산으로 search_type 추가
                    insert_idx = max(0, len(df_copy.columns) - 1) if len(df_copy.columns) > 0 else 0
                    df_copy.insert(insert_idx, 'search_type', period_type)
                    combined_csv_data.append(df_copy)
    
    # 전체 데이터를 하나의 CSV로 저장 후 변환
    if combined_csv_data:
        combined_df = pd.concat(combined_csv_data, ignore_index=True)

        # 데이터 변환 (컬럼 → 행)
        print(f"\n🔄 전체 데이터 변환 중... (기간: {period_type})")

        # 임시 크롤러 객체 생성 (변환 메소드 사용을 위해) - 한 번만 생성
        temp_crawler = PlaywrightStockCrawler()
        transformed_df = temp_crawler.transform_to_row_format(combined_df, period_type)

        if not transformed_df.empty:
            # yyyymm별로 데이터 분리하여 저장 (같은 객체 재사용)
            temp_crawler.save_data_by_yyyymm(transformed_df, output_dir, period_type, s3_bucket, save_local)
        else:
            print("❌ 데이터 변환에 실패했습니다.")
    else:
        print("⚠️ 통합할 데이터가 없어 파일을 생성하지 않았습니다.")
    
    # 결과 출력
    print(f"\n{'='*60}")
    print(f"📊 전체 크롤링 결과 요약")
    print(f"{'='*60}")
    print(f"🏢 총 회사 수: {len(stocks_data)}")
    print(f"✅ 성공: {success_count}개")
    print(f"❌ 실패: {len(failed_companies)}개")
    
    if failed_companies:
        print(f"❌ 실패한 회사들: {', '.join(failed_companies)}")
    
    print(f"💾 결과 파일이 '{output_dir}' 폴더에 저장되었습니다.")
    print(f"📁 yyyymm별 분리된 파일들이 저장되었습니다.")
    print(f"📁 요약 파일: crawling_summary.json")
    
    # 타이머 종료
    crawler.end_timer()
    
    return all_results




async def crawl_multiple_stocks_direct(stocks_data, output_dir="./crawl_results", period_type="연간", s3_bucket=None, save_local=True):
    """
    종목 목록을 직접 받아서 크롤링 (Lambda에서 호출용)

    Args:
        stocks_data (list): 종목 정보 리스트 [{"code": "004150", "name": "한솔홀딩스"}, ...]
        output_dir (str): 결과 저장 디렉토리
        period_type (str): "연간" 또는 "분기"
        s3_bucket (str): S3 버킷명 (선택사항)
        save_local (bool): 로컬 저장 여부 (기본값: True)
    """
    try:
        print(f"[DIRECT] {len(stocks_data)}개 회사 정보로 크롤링을 시작합니다.")
        print(f"[PERIOD] 기간 타입: {period_type}")

        # 전체 크롤링에 타임아웃 설정 (Lambda 최대 15분 고려하여 14분으로 설정)
        # MWAA 환경에서는 더 긴 타임아웃 사용 가능
        timeout_seconds = 840  # 14분 (Lambda 안전 마진)

        # MWAA 환경 감지
        if os.environ.get('AIRFLOW_HOME'):
            timeout_seconds = 3600  # MWAA: 60분
            print(f"[MWAA] MWAA 환경 감지 - 타임아웃: {timeout_seconds}초")
        else:
            print(f"[TIMEOUT] 크롤링 타임아웃: {timeout_seconds}초")

        # 다중 크롤링 실행 (타임아웃 적용)
        return await asyncio.wait_for(
            crawl_multiple_stocks(stocks_data, output_dir, period_type, s3_bucket, save_local),
            timeout=timeout_seconds
        )

    except asyncio.TimeoutError:
        import traceback
        error_msg = f"[TIMEOUT] 크롤링 타임아웃 ({timeout_seconds}초 초과)"
        print(error_msg)
        print(f"[ERROR] {len(stocks_data)}개 종목 중 일부만 완료되었을 수 있습니다.")
        raise Exception(error_msg)
    except Exception as e:
        import traceback
        print(f"[ERROR] 직접 크롤러 실행 중 오류 발생: {str(e)}")
        print(f"[ERROR] 상세 오류 정보:")
        print(traceback.format_exc())
        raise


def run_multiple_crawler(stocks_json_file, output_dir="./crawl_results", period_type="연간", s3_bucket=None, save_local=True):
    """
    JSON 파일에서 주식 목록을 읽어 여러 회사 크롤링

    Args:
        stocks_json_file (str): 주식 목록 JSON 파일 경로
        output_dir (str): 결과 저장 디렉토리
        period_type (str): "연간" 또는 "분기"
        s3_bucket (str): S3 버킷명 (선택사항)
        save_local (bool): 로컬 저장 여부 (기본값: True)
    """
    try:
        # JSON 파일 읽기
        with open(stocks_json_file, 'r', encoding='utf-8') as f:
            stocks_data = json.load(f)

        print(f"[FILE] {stocks_json_file}에서 {len(stocks_data)}개 회사 정보를 로드했습니다.")
        print(f"[PERIOD] 기간 타입: {period_type}")

        # Windows 이벤트 루프 설정
        if os.name == 'nt':
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

        # 다중 크롤링 실행
        asyncio.run(crawl_multiple_stocks(stocks_data, output_dir, period_type, s3_bucket, save_local))

    except FileNotFoundError:
        print(f"[ERROR] 파일을 찾을 수 없습니다: {stocks_json_file}")
        print("[INFO] 예시 JSON 파일 형식:")
        print("""[
  {"code": "004150", "name": "한솔홀딩스"},
  {"code": "005930", "name": "삼성전자"},
  {"code": "000660", "name": "SK하이닉스"}
]""")
    except json.JSONDecodeError:
        print(f"[ERROR] JSON 파일 형식이 올바르지 않습니다: {stocks_json_file}")
    except Exception as e:
        import traceback
        print(f"[ERROR] 다중 크롤러 실행 중 오류 발생: {str(e)}")
        print(f"[ERROR] 상세 오류 정보:")
        print(traceback.format_exc())


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        # 명령줄 인수가 있으면 다중 크롤링
        stocks_file = sys.argv[1]
        output_dir = sys.argv[2] if len(sys.argv) > 2 else "./crawl_results"
        period_type = sys.argv[3] if len(sys.argv) > 3 else "연간"
        run_multiple_crawler(stocks_file, output_dir, period_type)
    else:
        print("[USAGE] 사용법: python naver_stock_invest_index_crawler.py <stocks.json> [output_dir] [period_type]")
        print("        예시: python naver_stock_invest_index_crawler.py stocks.json ./results daily/quarter/annual")
