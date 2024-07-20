import os
import psycopg2
from dotenv import load_dotenv
from flask import Flask, request
import threading
import time
import random
import json
from datetime import datetime,timezone

CREATE_DEVICES_TABLE = ("CREATE TABLE IF NOT EXISTS devices (id SERIAL PRIMARY KEY, device_id TEXT UNIQUE, name TEXT, type TEXT, register_time TIMESTAMP);")
CREATE_TEMPERATURE_TABLE = """CREATE TABLE IF NOT EXISTS temperatures (device_id TEXT, sensor_value REAL, time TIMESTAMP);"""
CREATE_HUMIDITY_TABLE = """CREATE TABLE IF NOT EXISTS humidities (device_id TEXT, sensor_value REAL, time TIMESTAMP);"""
CREATE_WIND_TABLE = """CREATE TABLE IF NOT EXISTS winds (device_id TEXT, sensor_value REAL, time TIMESTAMP);"""
CREATE_PRESSURE_TABLE = """CREATE TABLE IF NOT EXISTS pressures (device_id TEXT, sensor_value REAL, time TIMESTAMP);"""

INSERT_DEVICE = """INSERT INTO devices (device_id, name, type, register_time) VALUES (%s, %s, %s, %s) ON CONFLICT (device_id) DO NOTHING RETURNING id;"""
INSERT_TEMPERATURE="INSERT INTO temperatures (device_id, sensor_value, time) VALUES (%s, %s, %s);"
INSERT_HUMIDITY="INSERT INTO humidities (device_id, sensor_value, time) VALUES (%s, %s, %s);"
INSERT_WIND="INSERT INTO winds (device_id, sensor_value, time) VALUES (%s, %s, %s);"
INSERT_PRESSURE="INSERT INTO pressures (device_id, sensor_value, time) VALUES (%s, %s, %s);"

FETCH_ALL_DEVICES="SELECT * FROM devices;"

last_values = {}
flag = True

load_dotenv()

app = Flask(__name__)
url = os.getenv('DATABASE_URL')
connection = psycopg2.connect(host=url, user="postgres", password="password")

def get_time():
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%d %H:%M:%S")

def simulate_device(deviceID, deviceType, value):
    dt = get_time()
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
    
    print(deviceID, value)

def continuous_function():
    global flag
    while flag:
        for d in last_values:
            simulate_device(d, last_values[d]["type"], last_values[d]["sensor_value"])
        time.sleep(10)  # Sleep for 10 seconds

# Start the thread within the Flask application
def start_continuous_function():
    thread = threading.Thread(target=continuous_function)
    thread.daemon = True  # Daemonize thread
    thread.start()

@app.route('/api/start')
def start_server():
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(CREATE_DEVICES_TABLE)
            cursor.execute(CREATE_TEMPERATURE_TABLE)
            cursor.execute(CREATE_HUMIDITY_TABLE)
            cursor.execute(CREATE_WIND_TABLE)
            cursor.execute(CREATE_PRESSURE_TABLE)
    global flag, last_values
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
    return "IoT devices running"

@app.route('/api/check')
def check_server():
    return "CHECKED!!"

@app.route('/api/end')
def end_server():
    global flag
    flag = False
    # start_continuous_function()
    return "IoT devices stopped"

@app.route('/api/dropall')
def drop_all():
    global flag
    flag = False
    
    with connection:
        with connection.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS temps;")
            cursor.execute("DROP TABLE IF EXISTS humidities;")
            cursor.execute("DROP TABLE IF EXISTS winds;")
            cursor.execute("DROP TABLE IF EXISTS precips;")
    return "IoT devices stopped AND all data is deleted!!!"

@app.post('/api/register')
def register_device():
    data = request.get_json()
    device_id = data["device_id"]
    name = data["name"]
    d_type = data["type"]

    register_time = get_time()
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(CREATE_DEVICES_TABLE)
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
    
    data_dict = [
        {
            "id": d[0],
            "deviceID": d[1],
            "deviceName": d[2],
            "type": d[3],
            "registeredTime": d[4].isoformat()
        } 
        for d in devices
    ]
    return json.dumps(data_dict), 201