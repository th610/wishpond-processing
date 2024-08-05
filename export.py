import pymysql
import csv

# MySQL 데이터베이스 설정
db_config = {
    'host': '127.0.0.1',
    'port': 3306,
    'user': 'root',
    'password': '1234',
    'database': 'wishpond_data'
}

def export_to_csv():
    # MySQL 데이터베이스에 연결
    connection = pymysql.connect(**db_config)
    cursor = connection.cursor()

    # 쿼리 작성
    query = "SELECT id, first_name, email, phone_number FROM customers"

    # 쿼리 실행
    cursor.execute(query)

    # 결과 가져오기
    results = cursor.fetchall()

    # CSV 파일로 출력
    with open('customers.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['ID', 'First Name', 'Email', 'Phone Number'])  # CSV 파일의 헤더 작성
        writer.writerows(results)  # 결과를 CSV 파일에 작성

    # 연결 종료
    connection.close() 

if __name__ == "__main__":
    export_to_csv()

