# main.py
from flask import Flask, render_template, request, send_file, redirect, url_for, session
import os
import webbrowser
from threading import Timer
import pandas as pd
from data_extraction import get_list_info, get_all_visitors_info, map_data_to_dataframe
from data_merging import update_existing_dataset

app = Flask(__name__)
app.secret_key = 'your_secret_key'  # 세션을 사용하기 위한 비밀키

@app.route('/')
def index():
    previous_values = {
        'api_key': session.get('api_key', ''),
        'list_id': session.get('list_id', ''),
        'cid_count': session.get('cid_count', 10),
        'max_events_per_cid': session.get('max_events_per_cid', 10)
    }
    return render_template('index.html', previous_values=previous_values)

@app.route('/generate_csv', methods=['GET', 'POST'])
def generate_csv():
    if request.method == 'POST':
        api_key = request.form['api_key']
        list_id = request.form['list_id']
        cid_count = int(request.form['cid_count'])
        max_events_per_cid = int(request.form['max_events_per_cid'])

        columns = [
            'category', 'industry', 'interest', 'name', 'created_at1', 'lead_count', 'last_lead_activity',
            'group_id', 'user_type', 'lead_score', 'geoip_country', 'geoip_state', 'geoip_city', 'created_at',
            'id', 'key', 'value', 'source', 'offer_category', 'offer_product', 'offer_price', 'offer_created_at',
            'url', 'referrer', 'cid', 'utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content',
            'properties', 'event_context'
        ]

        df = pd.DataFrame(columns=columns)

        try:
            list_info = get_list_info(api_key, list_id)
            visitors_data, total_entries = get_all_visitors_info(api_key, list_id, cid_count)
            df = map_data_to_dataframe(df, api_key, list_info, visitors_data, total_entries, max_events_per_cid)  # api_key 추가

            if not os.path.exists('static'):
                os.makedirs('static')

            csv_path = os.path.join('static', 'data.csv')
            df.to_csv(csv_path, index=False)

            return redirect(url_for('view_data'))
        except Exception as e:
            return str(e)

    return render_template('generate_csv.html')

@app.route('/view_data')
def view_data():
    df = pd.read_csv(os.path.join('static', 'data.csv'))

    df = df.applymap(lambda x: x.replace('\n', ' ') if isinstance(x, str) else x)

    table_html = df.head().to_html(classes='table table-striped table-bordered table-hover')

    return render_template('view_data.html', tables=table_html)

@app.route('/download_csv')
def download_csv():
    return send_file(os.path.join('static', 'data.csv'), as_attachment=True)

@app.route('/merge_data', methods=['GET', 'POST'])
def merge_data():
    if request.method == 'POST':
        existing_csv = request.files['existing_csv']
        new_csv = request.files['new_csv']

        if existing_csv and new_csv:
            df_existing = pd.read_csv(existing_csv)
            df_new = pd.read_csv(new_csv)

            df_updated = update_existing_dataset(df_existing, df_new)

            merge_csv_path = os.path.join('static', 'merged_data.csv')
            df_updated.to_csv(merge_csv_path, index=False)

            return send_file(merge_csv_path, as_attachment=True)

    return render_template('merge_data.html')

def open_browser():
    webbrowser.open_new('http://127.0.0.1:5000/')

if __name__ == '__main__':
    Timer(1, open_browser).start()
    app.run(debug=True)
