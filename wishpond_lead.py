import requests
import pymysql

# Wishpond API 설정
wishpond_api_url = "https://api.wishpond.com/api/v1/leads"
wishpond_api_key = "e2e50ffbc317acddf1c6ff03c2b9b1f3"

# MySQL 데이터베이스 설정
db_config = {
    'host': '127.0.0.1',
    'port': 3306,
    'user': 'root',
    'password': '1234',
    'database': 'wishpond_data'
}

# 고객 정보를 Wishpond API에서 가져오는 함수
def fetch_customers():
    headers = {
        "X-Api-Token": "e2e50ffbc317acddf1c6ff03c2b9b1f3",
        "Content-Type": "application/json"
    }
    response = requests.get(wishpond_api_url, headers=headers)
    
    if response.status_code == 200:
        return response.json().get('leads', [])
    else:
        print(f"Error: Unable to fetch data from Wishpond API. Status code: {response.status_code}")
        return []

# 고객 정보를 MySQL 데이터베이스에 삽입하는 함수
def insert_customer(cursor, customer):
    sql = """
    INSERT INTO customers (id, first_name, email, phone_number)
    VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
    email = VALUES(first_name),
    first_name = VALUES(email),
    phone_number = VALUES(phone_number)
    """

    dynamic_attributes = customer.get('dynamic_attributes', {})
    cursor.execute(sql, (
        str(customer['id']),  # id 값을 문자열로 변환하여 삽입
        customer['email'],
        dynamic_attributes.get('first_name', ''),
        dynamic_attributes.get('phone_number', '')
    ))

def main():
    # 고객 정보 가져오기
    customers = fetch_customers()
    
    # MySQL 데이터베이스에 연결
    connection = pymysql.connect(**db_config)
    cursor = connection.cursor()
    
    # 데이터 삽입
    for customer in customers:
        insert_customer(cursor, customer)
    
    # 변경 사항 커밋 및 연결 종료
    connection.commit()
    connection.close()

if __name__ == "__main__":
    main()

