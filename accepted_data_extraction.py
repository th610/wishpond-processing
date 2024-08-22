# data_extraction.py
import time
import requests
import pandas as pd
import html
from urllib.parse import urlparse

# 설정값
MAX_API_CALLS = 99
API_CALL_LIMIT_DURATION = 60  # 1분
EXCLUDE_KEYWORDS = []

# API 호출 제한을 관리하는 데코레이터
def rate_limit(func):
    def wrapper(*args, **kwargs):
        if wrapper.api_calls_count >= MAX_API_CALLS:
            elapsed_time = time.time() - wrapper.start_time
            if elapsed_time < API_CALL_LIMIT_DURATION:
                sleep_time = API_CALL_LIMIT_DURATION - elapsed_time
                print(f'Rate limit reached. Sleeping for {sleep_time} seconds.')
                time.sleep(sleep_time)
            wrapper.api_calls_count = 0
            wrapper.start_time = time.time()
        wrapper.api_calls_count += 1
        wrapper.total_api_calls += 1
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        wrapper.total_time += (end_time - start_time)
        return result
    
    wrapper.api_calls_count = 0
    wrapper.total_api_calls = 0
    wrapper.start_time = time.time()
    wrapper.total_time = 0
    return wrapper


@rate_limit
def get_list_info(api_key, list_id):
    list_url = f'https://api.wishpond.com/api/v1/lists/{list_id}'
    headers = {
        'X-Api-Token': api_key
    }
    retries = 0
    max_retries = 5
    wait_time = 60  # 1분 대기

    while retries < max_retries:
        try:
            response = requests.get(list_url, headers=headers)
            response.raise_for_status()  # HTTP 오류가 발생하면 예외가 발생합니다.

            if response.status_code == 200:
                return response.json()['list']
            else:
                raise Exception(f'Unexpected error: {response.status_code} - {response.text}')
        
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:
                print(f"429 Rate Limit Exceeded. Waiting {wait_time} seconds before retrying...")
                time.sleep(wait_time)
                retries += 1
            else:
                raise Exception(f'Error fetching list info: {response.status_code} - {response.text}')
    
    raise Exception(f"Failed to fetch list info after {max_retries} attempts due to rate limits.")



@rate_limit
def get_all_visitors_info(api_key, list_id, max_cid_count):
    page = 1
    all_visitors = []
    headers = {
        'X-Api-Token': api_key
    }

    while len(all_visitors) < max_cid_count:
        visitors_url = f'https://api.wishpond.com/api/v1/lists/{list_id}/visitors?page={page}&per_page=10'
        response = requests.get(visitors_url, headers=headers)
        if response.status_code == 200:
            visitors_response = response.json()
            visitors_data = visitors_response['visitors']

            all_visitors.extend(visitors_data)

            total_pages = visitors_response['meta']['total_pages']
            if page >= total_pages or len(all_visitors) >= max_cid_count:
                break
            page += 1
        else:
            raise Exception(f'Error fetching visitors info: {response.status_code} - {response.text}')
    
    return all_visitors[:max_cid_count], len(all_visitors)

@rate_limit
def get_all_visitor_events(api_key, visitor_id, max_events_per_cid):
    page = 1
    all_events = []
    headers = {
        'X-Api-Token': api_key
    }

    while len(all_events) < max_events_per_cid:
        events_url = f'https://api.wishpond.com/api/v1/visitors/{visitor_id}/events?page={page}&per_page=10'
        response = requests.get(events_url, headers=headers)
        if response.status_code == 200:
            events_response = response.json()
            events_data = events_response['events']
            all_events.extend(events_data)
            total_pages = events_response['meta']['total_pages']
            if page >= total_pages or len(all_events) >= max_events_per_cid:
                break
            page += 1
        else:
            raise Exception(f'Error fetching visitor events: {response.status_code} - {response.text}')
    
    return all_events[:max_events_per_cid]

def map_data_to_dataframe_accepted(df, api_key, list_info, visitors_data, total_entries, max_events_per_cid):
    rows = []
    unique_key_mapping = {}
    unique_key_counter = 1  # 고유 번호를 매기기 위한 카운터

    for visitor in visitors_data:
        visitor_id = visitor.get('id')

        visitor_events = get_all_visitor_events(api_key, visitor_id, max_events_per_cid)
        print(f"Visitor ID: {visitor_id}, Events fetched: {len(visitor_events)}")
        dynamic_attributes = visitor.get('dynamic_attributes', {})

        for event in visitor_events:
            if any(keyword in event.get('key', '') for keyword in EXCLUDE_KEYWORDS):
                continue

            row = {col: None for col in df.columns}

            # 변환된 컬럼 이름에 맞게 데이터 할당
            row['customer_id'] = visitor.get('cid')
            row['event_created_timestamp'] = event.get('created_at')
            row['event_id'] = event.get('id')
            row['member_type'] = "visitor"
            row['customer_category'] = '영상'
            row['industry_field'] = '디자인'
            row['ad_objective'] = '고객 모집'
            row['marketing_stage'] = '가망고객모집'
            row['campaign_title'] = '랜딩페이지'
            row['free_offer'] = '할인쿠폰'
            row['interest_field'] = '영상'
            row['key_activity'] = '이벤트'
            row['data_type'] = '사용자이벤트'
            row['item_name'] = '캠페인 방문'


            transaction_key = event.get('key', None)
            row['event_value'] = event.get('value', None)
            row['data_source'] = event.get('source', None)

            # 유니크한 transaction_key에 대해 고유 번호 할당
            if transaction_key not in unique_key_mapping:
                unique_key_mapping[transaction_key] = unique_key_counter
                unique_key_counter += 1

            # 각 행에 고유 번호 매핑
            row['event_key'] = unique_key_mapping[transaction_key]

            # 나머지 필드들에 대한 추가 처리
            row['purchase_category'] = None
            row['purchase_product_name'] = None
            row['purchase_cost'] = None
            row['purchase_timestamp'] = None

            properties = event.get('properties', {})
            row['website_url'] = properties.get('url', properties.get('URL', None))
            row['source_channel'] = properties.get('referrer', properties.get('Referrer', None))

            # UTM 파라미터 매핑
            row['utm_source'] = properties.get('utm_source')  
            row['utm_medium'] = properties.get('utm_medium')
            row['ad_campaign'] = properties.get('utm_campaign')
            row['target_keyword'] = properties.get('utm_term')
            row['content_description'] = properties.get('utm_content')
            row['customer_list_id'] = list_info.get('id')
            row['list_created_timestamp'] = list_info.get('created_at')  # 사용자 정보
            row['score'] = visitor.get('lead_score')

            rows.append(row)
    
    new_df = pd.DataFrame(rows, columns=df.columns)
    df = pd.concat([df, new_df], ignore_index=True)
    
    return df

