# data_extraction.py
import time
import requests
import pandas as pd
import html
from urllib.parse import urlparse
from utils import rate_limit, get_list_info, get_all_visitors_info, get_all_visitor_events,EXCLUDE_KEYWORDS


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


            # transaction_key = event.get('key', None)
            
            row['event_key'] = event.get('key', None)
            row['event_value'] = event.get('value', None)
            row['data_source'] = event.get('source', None)

            # # 유니크한 transaction_key에 대해 고유 번호 할당
            # if transaction_key not in unique_key_mapping:
            #     unique_key_mapping[transaction_key] = unique_key_counter
            #     unique_key_counter += 1

            # # 각 행에 고유 번호 매핑
            # row['event_key'] = unique_key_mapping[transaction_key]


            # 나머지 필드들에 대한 추가 처리
            row['purchase_category'] = None
            row['purchase_product_name'] = None
            row['purchase_cost'] = None
            row['purchase_timestamp'] = None

            properties = event.get('properties', {})
            row['website_url'] = properties.get('url', properties.get('URL', None))
            row['source_channel'] = properties.get('referrer', properties.get('Referrer', None))

            #################소스 데이터 추가#################
            row['mid_value'] = list_info.get('mid')
            row['email'] = list_info.get('email')
            row['status'] = list_info.get('status')
            row['sub_type'] = list_info.get('subscribed')

            row['properties'] = properties  # properties 전체를 넣음
            row['context'] = event.get('event_context')
            row['page_name'] = event.get('page_title')
            
            row['product'] = properties.get('product')
            row['ip'] = properties.get('ip_address')

            ###################################################

            # UTM 파라미터 매핑
            row['source_type'] = properties.get('utm_source')  
            row['media_type'] = properties.get('utm_medium')
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

