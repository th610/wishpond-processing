from flask import Flask, render_template, request, send_file, redirect, url_for, session, jsonify, after_this_request, Response
import os
import webbrowser
from threading import Timer
import pandas as pd
from frients_data_extraction import get_list_info, get_all_visitors_info, map_data_to_dataframe_frients
from data_data_extraction import map_data_to_dataframe_data
from accepted_data_extraction import  map_data_to_dataframe_accepted
from data_merging import update_existing_dataset
import time
import requests 
from requests.exceptions import RequestException, Timeout, ConnectionError
from urllib.parse import urlparse

app = Flask(__name__)
app.secret_key = 'your_secret_key'

current_progress = 0
estimated_time_remaining = 0

@app.route('/')
def index():
    previous_values = {
        'api_key': session.get('api_key', ''),
        'list_id': session.get('list_id', ''),
        'cid_count': session.get('cid_count', 10),
        'max_events_per_cid': session.get('max_events_per_cid', 10)
    }
    return render_template('index.html', previous_values=previous_values)


@app.route('/generate_csv_frients', methods=['GET', 'POST'])
def generate_csv_frients():
    global current_progress, estimated_time_remaining
    if request.method == 'POST':
        api_key = request.form['api_key']
        list_id = request.form['list_id']
        cid_count = int(request.form['cid_count'])
        max_events_per_cid = int(request.form['max_events_per_cid'])

        columns = [
        'category', 'industry', 'campaign_objective', 'campaign_type', 'campaign_name', 'campaign_freeoffer', 'interest', 
        'objective', 'type', 'name', 'list_id', 'list_created_at', 'status', 'lead_count', 'last_lead_activity', 
        'backmatch_visitors', 'created_at', 'event_id', 'key', 'value', 'source', 'purchase_category', 'purchase_product', 
        'purchase_price', 'purchase_created_at', 'url', 'referrer', 'utm_source', 'utm_medium', 
        'utm_campaign', 'utm_term', 'utm_content', 'user_type', 'lead_score', 'cid'
        ]

        df = pd.DataFrame(columns=columns)

        try:
            list_info = get_list_info(api_key, list_id)
            visitors_data, total_entries = get_all_visitors_info(api_key, list_id, cid_count)
            start_time = time.time()

            for i, visitor in enumerate(visitors_data):
                try:
                    df = map_data_to_dataframe_frients(df, api_key, list_info, [visitor], total_entries, max_events_per_cid)
                except Exception as e:
                    if '504' in str(e):
                        print("504 error occurred. Saving progress and retrying after a delay...")
                        # 현재까지의 데이터를 저장
                        if not os.path.exists(os.path.join(app.root_path, 'static')):
                            os.makedirs(os.path.join(app.root_path, 'static'))
                        csv_path = os.path.join(app.root_path, 'static', f'data_partial_{int(time.time())}.csv')
                        df.to_csv(csv_path, index=False)
                        print(f"Partial data saved to {csv_path}. Retrying after delay...")
                        time.sleep(60)  # 60초 대기 후 재시도
                        continue
                    else:
                        raise e

                current_progress = int(((i + 1) / len(visitors_data)) * 100)

                # 예상 완료 시간 계산
                elapsed_time = time.time() - start_time
                estimated_total_time = (elapsed_time / (i + 1)) * len(visitors_data)
                estimated_time_remaining = int(estimated_total_time - elapsed_time)

                time.sleep(0.1)  # 데이터 처리 속도를 조절

            # 최종 데이터를 저장
            if not os.path.exists(os.path.join(app.root_path, 'static')):
                os.makedirs(os.path.join(app.root_path, 'static'))

            csv_path = os.path.join(app.root_path, 'static', f'data.csv')
            df.to_csv(csv_path, index=False)

            current_progress = 100  # 작업 완료
            estimated_time_remaining = 0

            return jsonify({"status": "success"})
        
        except Exception as e:
            return jsonify({"status": "error", "message": str(e)})

    return render_template('generate_csv_frients.html')

@app.route('/generate_csv_data', methods=['GET', 'POST'])
def generate_csv_data():
    global current_progress, estimated_time_remaining
    if request.method == 'POST':
        api_key = request.form['api_key']
        list_id = request.form['list_id']
        cid_count = int(request.form['cid_count'])
        max_events_per_cid = int(request.form['max_events_per_cid'])

        columns = [
            'list_id', 'list_date', 'score','id', 'user_type', 'segment', 'industry', 'marketing_goal', 
            'marketing_funnel', 'marketing_title', 'special_offer', 'interest', 'activity', 
            'data_type', 'item_name', 'transaction_date', 'transaction_id', 'transaction_key', 
            'transaction_value', 'source', 'payment_category', 'payment_product', 
            'payment_price', 'payment_date', 'address', 'referrer', 'utm_source', 
            'utm_medium', 'utm_campaign', 'utm_term', 'utm_content'
        ]   

        df = pd.DataFrame(columns=columns)

        try:
            list_info = get_list_info(api_key, list_id)
            visitors_data, total_entries = get_all_visitors_info(api_key, list_id, cid_count)
            start_time = time.time()

            for i, visitor in enumerate(visitors_data):
                try:
                    df = map_data_to_dataframe_data(df, api_key, list_info, [visitor], total_entries, max_events_per_cid)
                except Exception as e:
                    if '504' in str(e):
                        print("504 error occurred. Saving progress and retrying after a delay...")
                        # 현재까지의 데이터를 저장
                        if not os.path.exists(os.path.join(app.root_path, 'static')):
                            os.makedirs(os.path.join(app.root_path, 'static'))
                        csv_path = os.path.join(app.root_path, 'static', f'data_partial_{int(time.time())}.csv')
                        df.to_csv(csv_path, index=False)
                        print(f"Partial data saved to {csv_path}. Retrying after delay...")
                        time.sleep(60)  # 60초 대기 후 재시도
                        continue
                    else:
                        raise e

                current_progress = int(((i + 1) / len(visitors_data)) * 100)

                # 예상 완료 시간 계산
                elapsed_time = time.time() - start_time
                estimated_total_time = (elapsed_time / (i + 1)) * len(visitors_data)
                estimated_time_remaining = int(estimated_total_time - elapsed_time)

                time.sleep(0.1)  # 데이터 처리 속도를 조절

            # 최종 데이터를 저장
            if not os.path.exists(os.path.join(app.root_path, 'static')):
                os.makedirs(os.path.join(app.root_path, 'static'))

            csv_path = os.path.join(app.root_path, 'static', f'data.csv')
            df.to_csv(csv_path, index=False)

            current_progress = 100  # 작업 완료
            estimated_time_remaining = 0

            return jsonify({"status": "success"})
        
        except Exception as e:
            return jsonify({"status": "error", "message": str(e)})

    return render_template('generate_csv_data.html')


@app.route('/generate_csv_accepted', methods=['GET', 'POST'])
def generate_csv_accepted():
    global current_progress, estimated_time_remaining
    if request.method == 'POST':
        api_key = request.form['api_key']
        list_id = request.form['list_id']
        cid_count = int(request.form['cid_count'])
        max_events_per_cid = int(request.form['max_events_per_cid'])

        columns = [
                'customer_id','event_id','event_created_timestamp', 'member_type', 
                'customer_category', 'industry_field', 'ad_objective', 'marketing_stage', 'campaign_title', 
                'free_offer', 'interest_field', 'key_activity', 'data_type', 'item_name', 
                  'event_key', 'event_value', 'data_source', 
                'purchase_category', 'purchase_product_name', 'purchase_cost', 'purchase_timestamp', 
                'website_url', 'source_channel', 'utm_source', 'utm_medium', 'ad_campaign', 
                'target_keyword', 'content_description', 'customer_list_id', 'list_created_timestamp'
            ]

        df = pd.DataFrame(columns=columns)

        try:
            list_info = get_list_info(api_key, list_id)
            visitors_data, total_entries = get_all_visitors_info(api_key, list_id, cid_count)
            start_time = time.time()

            for i, visitor in enumerate(visitors_data):
                try:
                    df = map_data_to_dataframe_accepted(df, api_key, list_info, [visitor], total_entries, max_events_per_cid)
                except Exception as e:
                    if '504' in str(e):
                        print("504 error occurred. Saving progress and retrying after a delay...")
                        # 현재까지의 데이터를 저장
                        if not os.path.exists(os.path.join(app.root_path, 'static')):
                            os.makedirs(os.path.join(app.root_path, 'static'))
                        csv_path = os.path.join(app.root_path, 'static', f'data_partial_{int(time.time())}.csv')
                        df.to_csv(csv_path, index=False)
                        print(f"Partial data saved to {csv_path}. Retrying after delay...")
                        time.sleep(60)  # 60초 대기 후 재시도
                        continue
                    else:
                        raise e

                current_progress = int(((i + 1) / len(visitors_data)) * 100)

                # 예상 완료 시간 계산
                elapsed_time = time.time() - start_time
                estimated_total_time = (elapsed_time / (i + 1)) * len(visitors_data)
                estimated_time_remaining = int(estimated_total_time - elapsed_time)

                time.sleep(0.1)  # 데이터 처리 속도를 조절

            # 최종 데이터를 저장
            if not os.path.exists(os.path.join(app.root_path, 'static')):
                os.makedirs(os.path.join(app.root_path, 'static'))

            csv_path = os.path.join(app.root_path, 'static', f'data.csv')
            df.to_csv(csv_path, index=False)

            current_progress = 100  # 작업 완료
            estimated_time_remaining = 0

            return jsonify({"status": "success"})
        
        except Exception as e:
            return jsonify({"status": "error", "message": str(e)})

    return render_template('generate_csv_accepted.html')




def make_request_with_retries(url, headers, retries=3, delay=5):
    """서버에 요청을 보내고, 실패 시 재시도하는 함수"""
    attempt = 0
    while attempt < retries:
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()  # HTTP 오류가 발생하면 예외가 발생합니다.
            return response
        except (ConnectionError, Timeout, RequestException) as e:
            print(f"Request failed: {e}. Retrying in {delay} seconds...")
            attempt += 1
            time.sleep(delay)
    raise Exception(f"Failed to fetch data after {retries} attempts.")


@app.route('/progress')
def progress():
    global current_progress, estimated_time_remaining
    return jsonify(progress=current_progress, time_remaining=estimated_time_remaining)

@app.route('/view_data')
def view_data():
    df = pd.read_csv(os.path.join(app.root_path, 'static', 'data.csv'))

    for col in df.columns:
        if df[col].dtype == 'object':  # 문자열 타입인 경우에만 적용
            df[col] = df[col].map(lambda x: x.replace('\n', ' ') if isinstance(x, str) else x)

    table_html = df.head().to_html(classes='table table-striped table-bordered table-hover')

    return render_template('view_data.html', tables=table_html)

@app.route('/download_csv')
def download_csv():
    return send_file(os.path.join(app.root_path, 'static', f'data.csv'))

@app.route('/merge_data', methods=['GET', 'POST'])
def merge_data():
    if request.method == 'POST':
        existing_csv = request.files['existing_csv']
        new_csv = request.files['new_csv']

        if existing_csv and new_csv:
            df_existing = pd.read_csv(existing_csv)
            df_new = pd.read_csv(new_csv)

            df_updated = update_existing_dataset(df_existing, df_new)
            merge_csv_path = os.path.join(app.root_path, 'static', f'merged_data_{int(time.time())}.csv')
            print(merge_csv_path)

            # CSV 파일 저장
            df_updated.to_csv(merge_csv_path, index=False, encoding='utf-8')

            # 파일 존재 확인 및 디버깅 출력
            if os.path.exists(merge_csv_path):
                print(f"File {merge_csv_path} saved successfully.")
            else:
                print(f"Error: File {merge_csv_path} not found after saving.")
                return "File not found error", 500  # 오류를 반환하고 종료
            
            # 파일을 한 번에 전송
            return send_file(merge_csv_path, mimetype='text/csv', as_attachment=True)

    return render_template('merge_data.html')

def open_browser():
    webbrowser.open_new('http://127.0.0.1:8080/')

if __name__ == '__main__':
    open_browser()
    app.run(debug=True, port= 8080)