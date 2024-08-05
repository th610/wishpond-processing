import pandas as pd
from urllib.parse import urlparse

def map_main_domain_to_id(df):
    main_domains = df['url'].apply(lambda x: urlparse(str(x)).netloc if pd.notna(x) else None)
    unique_domains = main_domains.dropna().unique()
    domain_to_id = {domain: idx + 1 for idx, domain in enumerate(unique_domains)}
    return main_domains.map(domain_to_id)

def update_existing_dataset(df_existing, df_new):
    df_new = df_new.rename(columns=str.strip)

    df_new = df_new.dropna(subset=['url'])

    df_existing['domain_id'] = map_main_domain_to_id(df_existing)

    print("Unique domain IDs mapped successfully:")
    print(df_existing['domain_id'].value_counts())

    columns_to_update = ['category', 'industry', 'interest', 'name', 
                         'offer_category', 'offer_product', 'offer_price', 
                         'offer_created_at', 'utm_source', 'utm_medium', 
                         'utm_campaign', 'utm_term', 'utm_content', 'url']

    for index, row in df_existing.iterrows():
        if pd.notna(row['domain_id']) and row['domain_id'] in df_new['id'].values:
            matching_row = df_new[df_new['id'] == row['domain_id']].iloc[0]
            for col in columns_to_update:
                if col in matching_row and pd.notna(matching_row[col]):
                    print(f"Updating row {index} for column {col}: {row[col]} -> {matching_row[col]}")
                    df_existing.at[index, col] = matching_row[col]

            if 'value' in matching_row and isinstance(matching_row['value'], str) and ('http://' in matching_row['value'] or 'https://' in matching_row['value']):
                df_existing.at[index, 'url'] = matching_row['value']
                print(f"Updated 'url' column with value: {matching_row['value']}")
            
            if pd.notna(df_existing.at[index, 'url']) and pd.notna(df_existing.at[index, 'value']):
                if urlparse(df_existing.at[index, 'value']).netloc != "":
                    df_existing.at[index, 'value'] = df_existing.at[index, 'url']
                    print(f"Updated 'value' column to match 'url': {df_existing.at[index, 'url']}")

    df_existing.drop(columns=['domain_id'], inplace=True)

    return df_existing
