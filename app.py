import os
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
from flask import Flask, request, jsonify
from threading import Thread
import time
import random
import json
from datetime import datetime, timezone
from flask_socketio import SocketIO
from flask_cors import CORS, cross_origin

CREATE_DEVICES_TABLE = "CREATE TABLE IF NOT EXISTS devices (id SERIAL PRIMARY KEY, device_id TEXT UNIQUE, name TEXT, type TEXT, register_time TIMESTAMP);"
CREATE_TEMPERATURE_TABLE = "CREATE TABLE IF NOT EXISTS temperatures (device_id TEXT, sensor_value REAL, time TIMESTAMP);"
CREATE_HUMIDITY_TABLE = "CREATE TABLE IF NOT EXISTS humidities (device_id TEXT, sensor_value REAL, time TIMESTAMP);"
CREATE_WIND_TABLE = "CREATE TABLE IF NOT EXISTS winds (device_id TEXT, sensor_value REAL, time TIMESTAMP);"
CREATE_PRESSURE_TABLE = "CREATE TABLE IF NOT EXISTS pressures (device_id TEXT, sensor_value REAL, time TIMESTAMP);"

INSERT_DEVICE = "INSERT INTO devices (device_id, name, type, register_time) VALUES (%s, %s, %s, %s) ON CONFLICT (device_id) DO NOTHING RETURNING id;"
INSERT_TEMPERATURE = "INSERT INTO temperatures (device_id, sensor_value, time) VALUES (%s, %s, %s);"
INSERT_HUMIDITY = "INSERT INTO humidities (device_id, sensor_value, time) VALUES (%s, %s, %s);"
INSERT_WIND = "INSERT INTO winds (device_id, sensor_value, time) VALUES (%s, %s, %s);"
INSERT_PRESSURE = "INSERT INTO pressures (device_id, sensor_value, time) VALUES (%s, %s, %s);"

FETCH_ALL_DEVICES = "SELECT * FROM devices;"
FETCH_AVERAGE_TEMPERATURE = "SELECT AVG(sensor_value) FROM temperatures WHERE device_id = %s AND time BETWEEN %s AND %s;"
FETCH_AVERAGE_HUMIDITY = "SELECT AVG(sensor_value) FROM humidities WHERE device_id = %s AND time BETWEEN %s AND %s;"
FETCH_AVERAGE_WIND = "SELECT AVG(sensor_value) FROM winds WHERE device_id = %s AND time BETWEEN %s AND %s;"
FETCH_AVERAGE_PRESSURE = "SELECT AVG(sensor_value) FROM pressures WHERE device_id = %s AND time BETWEEN %s AND %s;"

FETCH_MAX_TEMPERATURE = "SELECT MAX(sensor_value) FROM temperatures WHERE device_id = %s AND time BETWEEN %s AND %s;"
FETCH_MAX_HUMIDITY = "SELECT MAX(sensor_value) FROM humidities WHERE device_id = %s AND time BETWEEN %s AND %s;"
FETCH_MAX_WIND = "SELECT MAX(sensor_value) FROM winds WHERE device_id = %s AND time BETWEEN %s AND %s;"
FETCH_MAX_PRESSURE = "SELECT MAX(sensor_value) FROM pressures WHERE device_id = %s AND time BETWEEN %s AND %s;"

FETCH_MIN_TEMPERATURE = "SELECT MIN(sensor_value) FROM temperatures WHERE device_id = %s AND time BETWEEN %s AND %s;"
FETCH_MIN_HUMIDITY = "SELECT MIN(sensor_value) FROM humidities WHERE device_id = %s AND time BETWEEN %s AND %s;"
FETCH_MIN_WIND = "SELECT MIN(sensor_value) FROM winds WHERE device_id = %s AND time BETWEEN %s AND %s;"
FETCH_MIN_PRESSURE = "SELECT MIN(sensor_value) FROM pressures WHERE device_id = %s AND time BETWEEN %s AND %s;"

FETCH_INFO_TEMPERATURE = """
SELECT 
    MIN(sensor_value) AS minimum_value,
    MAX(sensor_value) AS maximum_value,
    AVG(sensor_value) AS average_value
FROM 
    temperatures
WHERE 
    device_id = %s
AND
    time BETWEEN %s AND %s;
"""
FETCH_INFO_HUMIDITY = """
SELECT 
    MIN(sensor_value) AS minimum_value,
    MAX(sensor_value) AS maximum_value,
    AVG(sensor_value) AS average_value
FROM 
    humidities
WHERE 
    device_id = %s
AND
    time BETWEEN %s AND %s;
"""
FETCH_INFO_WIND = """
SELECT 
    MIN(sensor_value) AS minimum_value,
    MAX(sensor_value) AS maximum_value,
    AVG(sensor_value) AS average_value
FROM 
    winds
WHERE 
    device_id = %s
AND
    time BETWEEN %s AND %s;
"""
FETCH_INFO_PRESSURE = """
SELECT 
    MIN(sensor_value) AS minimum_value,
    MAX(sensor_value) AS maximum_value,
    AVG(sensor_value) AS average_value
FROM 
    pressures
WHERE 
    device_id = %s
AND
    time BETWEEN %s AND %s;
"""


last_values = {}
flag = False
count = 0
testing = False

load_dotenv()

app = Flask(__name__)
CORS(app)
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='gevent')  # Allow all origins for development
# socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet')
url = os.getenv('IOT_SIMULATION_BACKEND_DATABASE_URL')
usr = os.getenv('IOT_SIMULATION_BACKEND_USER')
pwd = os.getenv('IOT_SIMULATION_BACKEND_PASSWORD')
connection = psycopg2.connect(host=url, user=usr, password=pwd)

def get_time():
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%d %H:%M:%S")

def simulate_device(deviceID, deviceType, value, dt):
    if deviceType == "Temperature":
        n = random.randint(1, 10)
        if n > 8 and value < 32:
            value = value + 0.1
        elif n >= 7 and n <= 8 and value > 26:
            value = value - 0.1
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(INSERT_TEMPERATURE, (deviceID, value, dt))
    elif deviceType == "Humidity":
        n = random.randint(1, 10)
        if n > 8 and value < 90:
            value = value + 1
        elif n >= 7 and n <= 8 and value > 20:
            value = value - 1
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(INSERT_HUMIDITY, (deviceID, value, dt))
    elif deviceType == "Wind":
        n = random.randint(1, 10)
        if n > 8 and value < 18:
            value = value + 1
        elif n >= 7 and n <= 8 and value > 2:
            value = value - 1
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(INSERT_WIND, (deviceID, value, dt))
    elif deviceType == "Pressure":
        n = random.randint(1, 10)
        if n > 7 and value < 1032:
            value = value + 1
        elif n >= 5 and n <= 7 and value > 997:
            value = value - 1
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(INSERT_PRESSURE, (deviceID, value, dt))
    last_values[deviceID]["sensor_value"] = value
    return value

def continuous_function():
    global flag, count
    while flag:
        dt = get_time()
        device_data = {
            "Temperature": [],
            "Humidity": [],
            "Wind": [],
            "Pressure": [],
            "timestamp": dt
        }
        for d in last_values:
            result = simulate_device(d, last_values[d]["type"], last_values[d]["sensor_value"], dt)
            device_data[last_values[d]["type"]].append({
                "deviceID": d,
                "value": result
            })
            
        print(last_values)
        socketio.emit('data', json.dumps(device_data), broadcast=True)
        time.sleep(10)  # Sleep for 10 seconds

        count += 1

        if count == 100:
            count = 0
            flag = False

def random_number_generator():
    while flag:
        time.sleep(5)  # Wait for 5 seconds
        number = random.randint(1, 10)
        socketio.emit('new_number', {'number': number}, broadcast=True)

# Start the thread within the Flask application
def start_continuous_function():
    if testing:
        thread = Thread(target=random_number_generator)
    else:
        thread = Thread(target=continuous_function)
    thread.daemon = True  # Daemonize thread
    thread.start()

@app.route('/')
def home():
    print("START........")
    return "qvvfvfsdv"

@app.route('/api/start')
def start_server():
    global flag, last_values
    if flag:
        return "Socket is already running."
    else:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(CREATE_DEVICES_TABLE)
                cursor.execute(CREATE_TEMPERATURE_TABLE)
                cursor.execute(CREATE_HUMIDITY_TABLE)
                cursor.execute(CREATE_WIND_TABLE)
                cursor.execute(CREATE_PRESSURE_TABLE)
        flag = True

        with connection:
            with connection.cursor() as cursor:
                cursor.execute(FETCH_ALL_DEVICES)
                devices = cursor.fetchall()
        
        for d in devices:
            value = 0
            if d[3] == "Temperature":
                value = float(random.randint(26, 32))
            elif d[3] == "Humidity":
                value = float(random.randint(20, 90))
            elif d[3] == "Wind":
                value = float(random.randint(0, 18))
            elif d[3] == "Pressure":
                value = float(random.randint(997, 1032))

            last_values[d[1]] = {
                "type": d[3],
                "sensor_value": value
            }
        start_continuous_function()
        return "Socket has started."

@app.route('/api/check')
def check_server():
    if flag:
        return "Socket is running."
    else:
        return "Socket is stopped."

@app.route('/api/end')
def end_server():
    global flag
    if flag:
        flag = False
        return "Socket has been stopped."
    else:
        return "Socket is not running."

@app.route('/api/dropall')
def drop_all():
    global flag
    flag = False
    
    with connection:
        with connection.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS temperatures;")
            cursor.execute("DROP TABLE IF EXISTS humidities;")
            cursor.execute("DROP TABLE IF EXISTS winds;")
            cursor.execute("DROP TABLE IF EXISTS pressures;")
    return "Socket is stopped AND all data is deleted!!!"

@app.post('/api/register')
def register_device():
    data = request.get_json()
    device_id = data["device_id"]
    name = data["name"]
    d_type = data["type"]

    register_time = get_time()
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(INSERT_DEVICE, (device_id, name, d_type, register_time))
            try:
                result = cursor.fetchone()[0]
            except:
                result = None
    if result:
        global last_values
        value = 0

        if d_type == "Temperature":
            value = float(random.randint(26, 32))
        elif d_type == "Humidity":
            value = float(random.randint(20, 90))
        elif d_type == "Wind":
            value = float(random.randint(0, 18))
        elif d_type == "Pressure":
            value = float(random.randint(997, 1032))

        last_values[device_id] = {
            "type": d_type,
            "sensor_value": value
        }
        return {"id":device_id, "message": f"Device {name} of type: {d_type} created on {register_time}"}, 201
    else:
        return {"message": "Error device id already present"}, 403

@app.get('/api/fetch-device')
def fetch_device():
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(FETCH_ALL_DEVICES)
            devices = cursor.fetchall()
    
    # data_dict = [
    #     {
    #         "id": d[0],
    #         "deviceID": d[1],
    #         "deviceName": d[2],
    #         "type": d[3],
    #         "registeredTime": d[4].isoformat()
    #     } 
    #     for d in devices
    # ]
    data_dict = {
        "Temperature": [],
        "Humidity": [],
        "Wind": [],
        "Pressure": []
    }

    for d in devices:
        data_dict[d[3]].append(d[1])

    return json.dumps(data_dict), 201

@app.get('/api/average')
def get_average():
    data = request.get_json()
    device_id = data["deviceID"]
    start_time = data["startTime"]
    end_time = data["endTime"]
    dtype = data["type"]

    fetch_query = {
        "Temperature": FETCH_AVERAGE_TEMPERATURE,
        "Humidity": FETCH_AVERAGE_HUMIDITY,
        "Wind": FETCH_AVERAGE_WIND,
        "Pressure": FETCH_AVERAGE_PRESSURE
    }.get(dtype)

    if not fetch_query:
        return jsonify({"error": "Invalid type"}), 400

    with connection:
        with connection.cursor() as cursor:
            cursor.execute(fetch_query, (device_id, start_time, end_time))
            result = cursor.fetchone()[0]

    return jsonify({"value": result})

@app.route('/api/max')
def get_max():
    data = request.get_json()
    device_id = data["deviceID"]
    start_time = data["startTime"]
    end_time = data["endTime"]
    dtype = data["type"]

    fetch_query = {
        "Temperature": FETCH_MAX_TEMPERATURE,
        "Humidity": FETCH_MAX_HUMIDITY,
        "Wind": FETCH_MAX_WIND,
        "Pressure": FETCH_MAX_PRESSURE
    }.get(dtype)

    if not fetch_query:
        return jsonify({"error": "Invalid type"}), 400

    with connection:
        with connection.cursor() as cursor:
            cursor.execute(fetch_query, (device_id, start_time, end_time))
            result = cursor.fetchone()[0]

    return jsonify({"value": result})

@app.route('/api/min')
def get_min():
    data = request.get_json()
    device_id = data["deviceID"]
    start_time = data["startTime"]
    end_time = data["endTime"]
    dtype = data["type"]

    fetch_query = {
        "Temperature": FETCH_MIN_TEMPERATURE,
        "Humidity": FETCH_MIN_HUMIDITY,
        "Wind": FETCH_MIN_WIND,
        "Pressure": FETCH_MIN_PRESSURE
    }.get(dtype)

    if not fetch_query:
        return jsonify({"error": "Invalid type"}), 400

    with connection:
        with connection.cursor() as cursor:
            cursor.execute(fetch_query, (device_id, start_time, end_time))
            result = cursor.fetchone()[0]

    return jsonify({"value": result})

@app.post('/api/info')
def get_info():
    data = request.get_json()
    device_id = data["deviceID"]
    start_time = data["startTime"]
    end_time = data["endTime"]
    dtype = data["type"]

    fetch_query = {
        "Temperature": FETCH_INFO_TEMPERATURE,
        "Humidity": FETCH_INFO_HUMIDITY,
        "Wind": FETCH_INFO_WIND,
        "Pressure": FETCH_INFO_PRESSURE
    }.get(dtype)

    if not fetch_query:
        return jsonify({"error": "Invalid type"}), 400

    with connection:
        with connection.cursor() as cursor:
            cursor.execute(fetch_query, (device_id, start_time, end_time))
            result = cursor.fetchone()

    print(result)

    return jsonify({"minimum": result[0],
                    "maximum": result[1],
                    "average": result[2]})

@app.route('/socket')
def index():
    return "WebSocket Server Running"

@socketio.on('connect')
def handle_connect():
    print('Client connected')
    return "Connected"

@socketio.on('disconnect')
def handle_disconnect():
    print('Client disconnected')
    return 'Client disconnected'

if __name__ == "__main__":
    # socketio.run(app)
    # socketio.run(app, host='0.0.0.0', port=5000)
    socketio.run(app, host='0.0.0.0', debug=True, port=5000)