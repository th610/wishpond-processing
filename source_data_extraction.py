# data_extraction.py
import time
import requests
import pandas as pd
import html
from urllib.parse import urlparse
from utils import rate_limit, get_list_info, get_all_visitors_info, get_all_visitor_events,EXCLUDE_KEYWORDS


def map_data_to_dataframe_source(df, api_key, list_info, visitors_data, total_entries, max_events_per_cid):
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
                
            row['utm_source'] = properties.get('utm_source')  
            row['utm_medium'] = properties.get('utm_medium')
            row['utm_campaign'] = properties.get('utm_campaign')
            row['utm_term'] = properties.get('utm_term')
            row['utm_content'] = properties.get('utm_content')
            
            row['properties'] = event.get('properties', None)

            row['user_type'] = "visitor"
            row['lead_score'] = visitor.get('lead_score') 
            row['cid'] = visitor.get('cid')

            #################소스 데이터 추가
            row['mid'] = list_info.get('mid')
            row['email'] = list_info.get('email')
            row['status'] = list_info.get('status')
            row['subscribed'] = list_info.get('subscribed')

            row['event_context'] = event.get('event_context')
            row['page_title'] = event.get('page_title')
            
            row['product'] = properties.get('product')
            row['ip_address'] = properties.get('ip_address')

            rows.append(row)
    
    new_df = pd.DataFrame(rows, columns=df.columns)
    df = pd.concat([df, new_df], ignore_index=True)
    
    return df