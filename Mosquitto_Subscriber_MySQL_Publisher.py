import paho.mqtt.client as mqtt
import pymysql.cursors
import sys
import json

# User variable for MySQL DATABASE name
myGatewayID = "Office"
# it is expected that this Database will already contain one table called sensors.  Create that table inside the Database with this command:
# CREATE TABLE sensors(device_id char(23) NOT NULL, transmission_count INT NOT NULL, battery_level FLOAT NOT NULL, type INT NOT NULL, node_id INT NOT NULL, rssi INT NOT NULL, last_heard TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP);

# User variables for MQTT Broker connection
mqttBroker = "localhost"
mqttBrokerPort = 1883
mqttUser = None
mqttPassword = None

mysqlHost = "localhost"
mysqlUser = "python_logger"
mysqlPassword = "supersecure"

# This callback function fires when the MQTT Broker conneciton is established.  At this point a connection to MySQL server will be attempted.
def on_connect(client, userdata, flags, rc):
    print("MQTT Client Connected")
    client.subscribe("gateway/"+myGatewayID+"/sensor/#")
    try:
        db = pymysql.connect(host=mysqlHost, user=mysqlUser, password=mysqlPassword, db=myGatewayID, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
        db.close()
        print("MySQL Client Connected")
    except:
        sys.exit("Connection to MySQL failed")

# This function updates the sensor's information in the sensor index table
def sensor_update(db, payload):
    cursor = db.cursor()
    # See if sensor already exists in sensors table, if not insert it, if so update it with latest information.
    deviceQuery = "EXISTS(SELECT * FROM sensors WHERE device_id = '%s')"%(payload['sensor_id'])
    cursor.execute("SELECT "+deviceQuery)
    data = cursor.fetchone()
    print("exists result for device_id: "+payload["sensor_id"] + " was: "+ str(data[deviceQuery]))
    if(data[deviceQuery] >= 1):
        updateRequest = "UPDATE sensors SET transmission_count = %i, battery_level = %.2f, rssi = %i, last_heard = CURRENT_TIMESTAMP WHERE device_id = '%s'" % (payload['data']['transmission_count'], payload['data']['battery_level'], payload['data']['rssi'], payload['sensor_id'])
        cursor.execute(updateRequest)
        db.commit()
    else:
        insertRequest = "INSERT INTO sensors(device_id, transmission_count, battery_level, type, node_id, rssi, last_heard) VALUES('%s',%i,%.2f,%i,%i,%i,CURRENT_TIMESTAMP)" % (payload['sensor_id'], payload['data']['transmission_count'], payload['data']['battery_level'], payload['data']['type'], payload['data']['node_id'], payload['data']['rssi'])
        cursor.execute(insertRequest)
        db.commit()

# This function determines if there is a table for the sensor telemetry, if not it creates one, then it logs sensor telemetry to the appropriate table
def log_telemetry(db, payload):
    cursor = db.cursor()
    # See if a table already exists for this sensor type, if not then create one
    tableExistsQuery = "SHOW TABLES LIKE 'type_%i'" % payload['data']['type']
    cursor.execute(tableExistsQuery)
    data = cursor.fetchone()

    if(data == None):
        # No table for that sensor type yet so create one
        createTableRequest = "CREATE TABLE type_%i(device_id CHAR(23) NOT NULL,timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP," % payload['data']['type']

        for key in payload['data']:
            if type(payload['data'][key]) == int:
                newColumn = key+" INT,"
                createTableRequest += newColumn
            if type(payload['data'][key]) == float:
                newColumn = key+" FLOAT,"
                createTableRequest += newColumn
        # remove last comma from string
        createTableRequest = createTableRequest[:-1]
        # close the command with parenthases
        createTableRequest += ')'
        cursor.execute(createTableRequest)
        db.commit()
    # Log sensor data
    logInsertRequest = "INSERT INTO type_%i(device_id,timestamp," % payload['data']['type']
    for key in payload['data']:
        columnKey = key+","
        logInsertRequest+=columnKey
    # remove last comma from string
    logInsertRequest = logInsertRequest[:-1]

    logInsertRequest += ") VALUES('" + payload['sensor_id'] +"',CURRENT_TIMESTAMP,"

    for key in payload['data']:
        columnData = str(payload['data'][key])+","
        logInsertRequest += columnData
    # remove last comma from string
    logInsertRequest = logInsertRequest[:-1]
    logInsertRequest += ')'
    cursor.execute(logInsertRequest)
    db.commit()

# The callback for when a PUBLISH message is received from the MQTT Broker.
def on_message(client, userdata, msg):
    payload = json.loads((msg.payload).decode("utf-8"))
    if 'sensor_id' in payload and 'data' in payload:
        if 'transmission_count' in payload['data'] and 'battery_level' in payload['data'] and 'type' in payload['data'] and 'node_id' in payload['data'] and 'rssi' in payload['data']:
            db = pymysql.connect(host="localhost", user="python", password="Spunky11", db="sensors",charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)
            sensor_update(db,payload)
            log_telemetry(db,payload)
            db.close()

# Connect the MQTT Client
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
# client.username_pw_set(username=mqttUser, password=mqttPassword)
try:
    client.connect(mqttBroker, mqttBrokerPort, 60)
except:
    sys.exit("Connection to MQTT Broker failed")
# Stay connected to the MQTT Broker indefinitely
client.loop_forever()
