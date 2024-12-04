# data_extraction.py
import time
import requests
import pandas as pd
import html
from urllib.parse import urlparse
from utils import rate_limit, get_list_info, get_all_visitors_info, get_all_visitor_events,EXCLUDE_KEYWORDS


def map_data_to_dataframe_data(df, api_key, list_info, visitors_data, total_entries, max_events_per_cid):
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
            row['list_id'] = list_info.get('id')
            row['list_date'] = list_info.get('created_at')  # 사용자 정보
            row['score'] = visitor.get('lead_score')
            row['id'] = visitor.get('cid')
            row['user_type'] = "visitor"
            row['segment'] = '제품'
            row['industry'] = '패션'
            row['marketing_goal'] = '브랜드 인지도'
            row['marketing_funnel'] = '구매 유도'
            row['marketing_title'] = '프로모션 페이지'
            row['special_offer'] = '할인 쿠폰'
            row['interest'] = '여성 의류'
            row['activity'] = '이벤트'
            row['data_type'] = '고객행동데이터'
            row['item_name'] = '프로모션방문완료'

            row['transaction_date'] = event.get('created_at')
            row['transaction_id'] = event.get('id')
            transaction_key = event.get('key', None)
            row['transaction_value'] = event.get('value', None)
            row['source'] = event.get('source', None)

            # 유니크한 transaction_key에 대해 고유 번호 할당
            if transaction_key not in unique_key_mapping:
                unique_key_mapping[transaction_key] = unique_key_counter
                unique_key_counter += 1

            # 각 행에 고유 번호 매핑
            row['transaction_key'] = unique_key_mapping[transaction_key]

            # 나머지 필드들에 대한 추가 처리
            row['payment_category'] = None
            row['payment_product'] = None
            row['payment_price'] = None
            row['payment_date'] = None

            properties = event.get('properties', {})
            row['address'] = properties.get('url', properties.get('URL', None))
            row['referrer'] = properties.get('referrer', properties.get('Referrer', None))

            # UTM 파라미터 매핑
            row['utm_source'] = properties.get('utm_source')  
            row['utm_medium'] = properties.get('utm_medium')
            row['utm_campaign'] = properties.get('utm_campaign')
            row['utm_term'] = properties.get('utm_term')
            row['utm_content'] = properties.get('utm_content')

            #################소스 데이터 추가
            row['m'] = list_info.get('mid')
            row['e-mail'] = list_info.get('email')
            row['position'] = list_info.get('status')
            row['sub'] = list_info.get('subscribed')

            row['event_context'] = event.get('event_context')
            row['title'] = event.get('page_title')
            
            row['good'] = properties.get('product')
            row['ip'] = properties.get('ip_address')
            row['cate'] = properties.get('category')

            rows.append(row)
    
    new_df = pd.DataFrame(rows, columns=df.columns)
    df = pd.concat([df, new_df], ignore_index=True)
    
    return df