from flask import Flask, jsonify, render_template
import mysql.connector
from mysql.connector import Error

app = Flask(__name__, static_folder='static', template_folder='templates')

DB_CONFIG = {
    'user': 'sqoopdang',
    'password': '1',
    'host': '127.0.0.1',
    'database': 'car_analysis_db'
}

def get_db_connection():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except Error as err:
        print(f"Lỗi kết nối MySQL: {err}")
        return None

def fetch_data(table_name):
    conn = get_db_connection()
    if conn is None:
        return []

    data = []
    try:
        cursor = conn.cursor(dictionary=True)
        query = f"SELECT * FROM {table_name}"
        cursor.execute(query)
        data = cursor.fetchall()
        cursor.close()
    except Error as e:
        print(f"Lỗi truy vấn bảng {table_name}: {e}")
    finally:
        if conn and conn.is_connected():
            conn.close()
    return data

@app.route('/api/avg_price_by_origin', methods=['GET'])
def api_avg_price_by_origin():
    return jsonify(fetch_data('avg_price_by_origin'))

@app.route('/api/avg_price_by_year', methods=['GET'])
def api_avg_price_by_year():
    return jsonify(fetch_data('avg_price_by_year'))

@app.route('/api/max_min_price_by_year', methods=['GET'])
def api_max_min_price_by_year():
    return jsonify(fetch_data('max_min_price_by_year'))

@app.route('/api/top_5_cars', methods=['GET'])
def api_top_5_cars():
    return jsonify(fetch_data('top_5_cars'))

@app.route('/')
def index():
    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)