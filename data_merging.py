import pandas as pd
from urllib.parse import urlparse
import random

def map_main_domain_to_id(df):
    # 'url' 컬럼에서 도메인과 첫 번째 경로까지 추출
    def extract_domain_and_path(url):
        parsed_url = urlparse(url)
        netloc = parsed_url.netloc
        path = parsed_url.path.split('/')[1] if len(parsed_url.path.split('/')) > 1 else ''
        return f"{netloc}/{path}"
    
    main_domains_and_paths = df['url'].apply(lambda x: extract_domain_and_path(str(x)) if pd.notna(x) else None)
    
    # 유니크한 도메인과 경로 값들 추출
    unique_domains_and_paths = main_domains_and_paths.dropna().unique()
    # 도메인/경로에 고유한 ID를 부여
    domain_to_id = {domain: idx + 1 for idx, domain in enumerate(unique_domains_and_paths)}
    # 도메인/경로 값을 고유 ID로 매핑
    return main_domains_and_paths.map(domain_to_id)
    
def update_existing_dataset(df_existing, df_new):

    df_new = df_new.rename(columns=str.strip)
    df_new = df_new.dropna(subset=['url'])

    df_existing['domain_id'] = map_main_domain_to_id(df_existing)

    print("Unique domain IDs mapped successfully:")
    print(df_existing['domain_id'].value_counts())

    columns_to_update = ['category', 'industry', 'interest', 'name', 
                         'offer_category', 'offer_product', 'offer_price', 
                         'offer_created_at', 'utm_source', 'utm_medium', 
                         'utm_campaign', 'utm_term', 'utm_content', 'url','refferr']

    # 도메인 ID가 df_new['id']에 존재하지 않는 행들을 처리하기 위한 설정
    domain_id_counts = df_existing['domain_id'].value_counts()
    low_count_domain_ids = domain_id_counts.tail(20).index.tolist()  # 하위 20개의 ID

    for index, row in df_existing.iterrows():
        matching_row = None
        
        if pd.notna(row['domain_id']) and row['domain_id'] in df_new['id'].values:
            matching_rows = df_new[df_new['id'] == row['domain_id']]
            if not matching_rows.empty:
                matching_row = matching_rows.iloc[0]

        elif pd.notna(row['domain_id']) and row['domain_id'] not in df_new['id'].values:
            # df_new['id']에 없는 경우 랜덤한 low_count_domain_ids 중 하나로 매핑
            while matching_row is None:
                random_domain_id = random.choice(low_count_domain_ids)
                random_domain_rows = df_new[df_new['id'] == random_domain_id]
                if not random_domain_rows.empty:
                    matching_row = random_domain_rows.iloc[0]
                else:
                    # 예외 처리: 만약 매칭되는 행이 없는 경우 다른 도메인 ID 시도
                    fallback_domain_id = random.choice(df_new['id'].values)
                    fallback_rows = df_new[df_new['id'] == fallback_domain_id]
                    if not fallback_rows.empty:
                        matching_row = fallback_rows.iloc[0]


            df_existing.at[index, 'domain_id'] = random_domain_id

        if matching_row is not None:
            # 먼저 'value' 값을 처리하여 URL 형식의 값을 'url'로 이동
            if 'value' in matching_row and isinstance(matching_row['value'], str) and ('http://' in matching_row['value'] or 'https://' in matching_row['value']):
                df_existing.at[index, 'url'] = matching_row['value']

            # 그 후 나머지 컬럼들을 처리
            for col in columns_to_update:
                if col in matching_row and pd.notna(matching_row[col]):
                    df_existing.at[index, col] = matching_row[col]

    df_existing.drop(columns=['domain_id'], inplace=True)

    return df_existing

