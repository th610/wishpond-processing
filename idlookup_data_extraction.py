# data_extraction.py
import time
import requests
import pandas as pd
import html
from urllib.parse import urlparse

from utils import *


def map_data_to_dataframe_idlookup(df, api_key, list_info, visitors_data, cid_id, max_events_per_cid):
    rows = []
    for visitor in visitors_data:
        visitor_id = visitor.get('id')
        cid = visitor.get('cid')  # 각 방문자의 cid를 가져옴

        # 특정 cid의 이벤트만 가져오도록 수정
        visitor_events = get_filtered_visitor_events(api_key, cid_id, max_events_per_cid)
        print(f"Visitor ID: {visitor_id}, CID: {cid}, Events fetched: {len(visitor_events)}")

        dynamic_attributes = visitor.get('dynamic_attributes', {})

        for event in visitor_events:
            if any(keyword in event.get('key', '') for keyword in EXCLUDE_KEYWORDS):
                continue

            row = {col: None for col in df.columns}

            # 리스트 정보
            row['list_id'] = list_info.get('id')
            row['list_created_at'] = list_info.get('created_at')
            row['status'] = list_info.get('status')
            row['lead_count'] = list_info.get('lead_count')
            row['last_lead_activity'] = list_info.get('last_lead_activity')
            row['backmatch_visitors'] = list_info.get('backmatch_visitors')

            # 이벤트 정보
            row['created_at'] = event.get('created_at')
            row['event_id'] = event.get('id')
            row['key'] = event.get('key', None)
            row['value'] = event.get('value', None)
            row['source'] = event.get('source', None)

            # 속성 정보
            properties = event.get('properties', {})
            row['url'] = properties.get('url', properties.get('URL', None))
            row['referrer'] = properties.get('referrer', properties.get('Referrer', None))
            row['utm_source'] = properties.get('utm_source')  
            row['utm_medium'] = properties.get('utm_medium')
            row['utm_campaign'] = properties.get('utm_campaign')
            row['utm_term'] = properties.get('utm_term')
            row['utm_content'] = properties.get('utm_content')
            
            # 추가 정보
            row['user_type'] = "visitor"
            row['lead_score'] = visitor.get('lead_score')
            row['cid'] = cid

            # 소스 데이터 추가
            row['mid'] = list_info.get('mid')
            row['email'] = list_info.get('email')
            row['subscribed'] = list_info.get('subscribed')

            row['event_context'] = event.get('event_context')
            row['page_title'] = event.get('page_title')
            row['product'] = properties.get('product')
            row['ip_address'] = properties.get('ip_address')
            row['category1'] = properties.get('category')

            rows.append(row)
    
    new_df = pd.DataFrame(rows, columns=df.columns)
    df = pd.concat([df, new_df], ignore_index=True)
    
    return df