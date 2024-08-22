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

def map_data_to_dataframe_frients(df, api_key, list_info, visitors_data, total_entries, max_events_per_cid):
    rows = []
    for visitor in visitors_data:
        visitor_id = visitor.get('id')

        visitor_events = get_all_visitor_events(api_key, visitor_id, max_events_per_cid)
        print(f"Visitor ID: {visitor_id}, Events fetched: {len(visitor_events)}")
        dynamic_attributes = visitor.get('dynamic_attributes', {})

        for event in visitor_events:
            if any(keyword in event.get('key', '') for keyword in EXCLUDE_KEYWORDS):
                continue

            row = {col: None for col in df.columns}


            row['category'] = '서비스'
            row['industry'] = '디자인'
            row['campaign_objective'] = '모객'
            row['campaign_type'] = '잠재고객모집'
            row['campaign_name'] = '랜딩페이지'
            row['campaign_freeoffer'] = '무료웨비나'
            row['interest'] = '마케팅'
            row['objective'] = '캠페인'
            row['type'] = '맞춤이벤트'
            row['name'] = '2-1.맞춤전환-캠페인방문완료'

            
            row['list_id'] = list_info.get('id')
            row['list_created_at'] = list_info.get('created_at')
            row['status'] = list_info.get('status')
            row['lead_count'] = list_info.get('lead_count')
            row['last_lead_activity'] = list_info.get('last_lead_activity')
            row['backmatch_visitors'] = list_info.get('backmatch_visitors')
            
            row['created_at'] = event.get('created_at')
            row['event_id'] = event.get('id')
            row['key'] = event.get('key', None)
            row['value'] = event.get('value', None)
            row['source'] = event.get('source', None)

            
            properties = event.get('properties', {})
            row['url'] = properties.get('url', properties.get('URL', None))
            row['referrer'] = properties.get('referrer', properties.get('Referrer', None))

            # if isinstance(row['value'], str) and ('http://' in row['value'] or 'https://' in row['value']):
            #     row['url'] = row['value']
            #     row['value'] = 1
                
            row['utm_source'] = properties.get('utm_source')  
            row['utm_medium'] = properties.get('utm_medium')
            row['utm_campaign'] = properties.get('utm_campaign')
            row['utm_term'] = properties.get('utm_term')
            row['utm_content'] = properties.get('utm_content')
            
            row['properties'] = event.get('properties', None)

            row['user_type'] = "visitor"
            row['lead_score'] = visitor.get('lead_score') 
            row['cid'] = visitor.get('cid')

            rows.append(row)
    
    new_df = pd.DataFrame(rows, columns=df.columns)
    df = pd.concat([df, new_df], ignore_index=True)
    
    return df