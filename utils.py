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



import time
import requests

# API 호출에 대한 rate limit decorator 제거 또는 수정 필요

def get_all_visitors_info(api_key, list_id, max_cid_count):
    page = 1
    all_visitors = []
    headers = {
        'X-Api-Token': api_key
    }
    max_retries = 5  # 최대 재시도 횟수
    retry_delay = 30  # 재시도 대기 시간 (초)

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
            time.sleep(1)  # 각 페이지 요청마다 1초 대기

        elif response.status_code == 429:
            print("Rate limit exceeded. Waiting before retrying...")
            for _ in range(max_retries):
                time.sleep(retry_delay)  # 대기 후 재시도
                response = requests.get(visitors_url, headers=headers)
                if response.status_code == 200:
                    break
            else:
                raise Exception(f'Error fetching visitors info after retries: {response.status_code} - {response.text}')
        
        else:
            raise Exception(f'Error fetching visitors info: {response.status_code} - {response.text}')
    
    return all_visitors[:max_cid_count], len(all_visitors)

@rate_limit
def get_all_visitor_events(api_key, visitor_id, max_events_per_cid):
    page = 1
    visitor_page=page

    all_events = []
    headers = {
        'X-Api-Token': api_key
    }
    retries = 0
    max_retries = 5
    wait_time = 20  # 재시도 전 대기 시간 (초)

    while len(all_events) < max_events_per_cid and retries < max_retries:
        print("방문자"+str(visitor_page)+"방문자ID"+str(visitor_id)+"이벤트" + str(page) )
        try:
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

            elif response.status_code == 429:
                print(f"429 Rate Limit Exceeded. Waiting {wait_time} seconds before retrying...")
                time.sleep(wait_time)
                retries += 1
                continue
            else:
                raise Exception(f'Error fetching visitor events: {response.status_code} - {response.text}')
        
        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}")
            break
    
    if retries >= max_retries:
        print(f"Max retries reached. Some events may not be fetched.")
    
    return all_events[:max_events_per_cid]

@rate_limit
def get_specific_visitor_info(api_key, list_id, cid_id):
    """특정 CID에 해당하는 방문자 정보를 조회"""
    visitor_url = f'https://api.wishpond.com/api/v1/lists/{list_id}/visitors/{cid_id}'
    headers = {
        'X-Api-Token': api_key
    }
    retries = 0
    max_retries = 5
    wait_time = 60  # 1분 대기

    while retries < max_retries:
        try:
            response = requests.get(visitor_url, headers=headers)
            response.raise_for_status()

            if response.status_code == 200:
                return response.json()['visitor']
            else:
                raise Exception(f'Unexpected error: {response.status_code} - {response.text}')
        
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:
                print(f"429 Rate Limit Exceeded. Waiting {wait_time} seconds before retrying...")
                time.sleep(wait_time)
                retries += 1
            else:
                raise Exception(f'Error fetching visitor info: {response.status_code} - {response.text}')
    
    raise Exception(f"Failed to fetch visitor info after {max_retries} attempts due to rate limits.")

# 특정 cid의 이벤트 정보를 조회하는 함수
@rate_limit
def get_specific_visitor_events(api_key, cid_id, max_events):
    """특정 CID에 해당하는 이벤트를 조회"""
    page = 1
    all_events = []
    headers = {
        'X-Api-Token': api_key
    }
    retries = 0
    max_retries = 5
    wait_time = 20  # 재시도 전 대기 시간 (초)

    while len(all_events) < max_events and retries < max_retries:
        print(f"CID {cid_id} - 이벤트 페이지 {page}")
        try:
            events_url = f'https://api.wishpond.com/api/v1/visitors/{cid_id}/events?page={page}&per_page=10'
            response = requests.get(events_url, headers=headers)

            if response.status_code == 200:
                events_response = response.json()
                events_data = events_response['events']
                all_events.extend(events_data)
                total_pages = events_response['meta']['total_pages']
                if page >= total_pages or len(all_events) >= max_events:
                    break
                page += 1

            elif response.status_code == 429:
                print(f"429 Rate Limit Exceeded. Waiting {wait_time} seconds before retrying...")
                time.sleep(wait_time)
                retries += 1
                continue
            else:
                raise Exception(f'Error fetching visitor events: {response.status_code} - {response.text}')
        
        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}")
            break
    
    if retries >= max_retries:
        print(f"Max retries reached. Some events may not be fetched.")
    
    return all_events[:max_events]

@rate_limit
def get_filtered_visitor_events(api_key, cid_id, max_events):
    """특정 cid에 대한 이벤트만 가져오는 함수"""
    page = 1
    events = []
    headers = {
        'X-Api-Token': api_key
    }
    retries = 0
    max_retries = 5
    wait_time = 20

    while len(events) < max_events and retries < max_retries:
        url = f'https://api.wishpond.com/api/v1/visitors/{cid_id}/events?page={page}&per_page=10'
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            events_data = data.get('events', [])
            events.extend(events_data)
            total_pages = data['meta'].get('total_pages', 1)
            
            if page >= total_pages or len(events) >= max_events:
                break
            page += 1

        elif response.status_code == 429:
            print(f"Rate limit exceeded. Waiting {wait_time} seconds before retrying...")
            time.sleep(wait_time)
            retries += 1
        else:
            raise Exception(f"Error fetching events for cid {cid_id}: {response.status_code} - {response.text}")
    
    return events[:max_events]