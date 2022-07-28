import os
from threading import Thread
from time import sleep

from dotenv import load_dotenv
from flask import Flask, jsonify, Response
from pycomfoconnect import *

load_dotenv()

ip = os.environ['COMFO_IP']
pin = int(os.environ['COMFO_PIN'])
local_name = os.environ['COMFO_LOCAL_NAME']
local_uuid = bytes.fromhex(os.environ['COMFO_LOCAL_UUID'])
sensor_names = {
    SENSOR_FAN_NEXT_CHANGE: 'sensor_fan_next_change',
    SENSOR_FAN_SPEED_MODE: 'sensor_fan_speed_mode',
    SENSOR_FAN_SUPPLY_DUTY: 'sensor_fan_supply_duty',
    SENSOR_FAN_EXHAUST_DUTY: 'sensor_fan_exhaust_duty',
    SENSOR_FAN_SUPPLY_FLOW: 'sensor_fan_supply_flow',
    SENSOR_FAN_EXHAUST_FLOW: 'sensor_fan_exhaust_flow',
    SENSOR_FAN_SUPPLY_SPEED: 'sensor_fan_supply_speed',
    SENSOR_FAN_EXHAUST_SPEED: 'sensor_fan_exhaust_speed',
    SENSOR_POWER_CURRENT: 'sensor_power_current',
    SENSOR_POWER_TOTAL_YEAR: 'sensor_power_total_year',
    SENSOR_POWER_TOTAL: 'sensor_power_total',
    SENSOR_DAYS_TO_REPLACE_FILTER: 'sensor_days_to_replace_filter',
    SENSOR_AVOIDED_HEATING_CURRENT: 'sensor_avoided_heating_current',
    SENSOR_AVOIDED_HEATING_TOTAL_YEAR: 'sensor_avoided_heating_total_year',
    SENSOR_AVOIDED_HEATING_TOTAL: 'sensor_avoided_heating_total',
    SENSOR_TEMPERATURE_SUPPLY: 'sensor_temperature_supply',
    SENSOR_TEMPERATURE_EXTRACT: 'sensor_temperature_extract',
    SENSOR_TEMPERATURE_EXHAUST: 'sensor_temperature_exhaust',
    SENSOR_TEMPERATURE_OUTDOOR: 'sensor_temperature_outdoor',
    SENSOR_HUMIDITY_SUPPLY: 'sensor_humidity_supply',
    SENSOR_HUMIDITY_EXTRACT: 'sensor_humidity_extract',
    SENSOR_HUMIDITY_EXHAUST: 'sensor_humidity_exhaust',
    SENSOR_HUMIDITY_OUTDOOR: 'sensor_humidity_outdoor',
    SENSOR_BYPASS_STATE: 'sensor_bypass_state',
    SENSOR_OPERATING_MODE: 'sensor_operating_mode',
    SENSOR_OPERATING_MODE_BIS: 'sensor_operating_mode_bis',
    SENSOR_PROFILE_TEMPERATURE: 'sensor_profile_temperature',
    SENSOR_AWAY: 'sensor_away',
}
sensors = {}
bridge = None


def bridge_discovery():
    bridges = Bridge.discover(ip)
    if bridges:
        bridge = bridges[0]
    else:
        bridge = None

    if bridge is None:
        print("No bridges found!")
        exit(1)

    print("Bridge found: %s (%s)" % (bridge.uuid.hex(), bridge.host))
    bridge.debug = True

    return bridge


def callback_sensor(var, raw_value):
    result = {
        'value': raw_value,
    }
    if var == SENSOR_TEMPERATURE_EXTRACT or var == SENSOR_TEMPERATURE_EXHAUST or var == SENSOR_TEMPERATURE_OUTDOOR:
        result['value'] = raw_value / 10.0
        result['unit'] = "C"

    if var == SENSOR_BYPASS_STATE:
        result['unit'] = "%"

    if var == SENSOR_HUMIDITY_SUPPLY or var == SENSOR_HUMIDITY_EXTRACT or var == SENSOR_HUMIDITY_EXHAUST or var == SENSOR_HUMIDITY_OUTDOOR:
        result['unit'] = "%"

    if var == SENSOR_FAN_EXHAUST_SPEED or var == SENSOR_FAN_SUPPLY_SPEED:
        result['unit'] = "rpm"

    if var == SENSOR_FAN_EXHAUST_DUTY or var == SENSOR_FAN_SUPPLY_DUTY:
        result['unit'] = "%"

    if var == SENSOR_FAN_EXHAUST_FLOW or var == SENSOR_FAN_SUPPLY_FLOW:
        result['unit'] = "m3/h"

    if var == SENSOR_POWER_CURRENT:
        result['unit'] = "W"

    if var == SENSOR_POWER_TOTAL_YEAR or var == SENSOR_POWER_TOTAL:
        result['unit'] = "kWh"

    sensors[var] = result


def connect_comfoconnect():
    global bridge

    bridge = bridge_discovery()

    comfoconnect = ComfoConnect(bridge, local_uuid, local_name, pin)
    comfoconnect.callback_sensor = callback_sensor

    try:
        comfoconnect.connect(True)

    except Exception as e:
        print('ERROR: %s' % e)
        exit(1)

    # comfoconnect.register_sensor(SENSOR_FAN_NEXT_CHANGE)  # General: Countdown until next fan speed change
    comfoconnect.register_sensor(SENSOR_FAN_SPEED_MODE)  # Fans: Fan speed setting
    comfoconnect.register_sensor(SENSOR_FAN_SUPPLY_DUTY)  # Fans: Supply fan duty
    comfoconnect.register_sensor(SENSOR_FAN_EXHAUST_DUTY)  # Fans: Exhaust fan duty
    comfoconnect.register_sensor(SENSOR_FAN_SUPPLY_FLOW)  # Fans: Supply fan flow
    comfoconnect.register_sensor(SENSOR_FAN_EXHAUST_FLOW)  # Fans: Exhaust fan flow
    comfoconnect.register_sensor(SENSOR_FAN_SUPPLY_SPEED)  # Fans: Supply fan speed
    comfoconnect.register_sensor(SENSOR_FAN_EXHAUST_SPEED)  # Fans: Exhaust fan speed
    comfoconnect.register_sensor(SENSOR_POWER_CURRENT)
    comfoconnect.register_sensor(SENSOR_POWER_TOTAL_YEAR)  # Power Consumption: Total year-to-date
    comfoconnect.register_sensor(SENSOR_POWER_TOTAL)  # Power Consumption: Total from start
    comfoconnect.register_sensor(SENSOR_DAYS_TO_REPLACE_FILTER)
    comfoconnect.register_sensor(SENSOR_AVOIDED_HEATING_CURRENT)  # Avoided Heating: Avoided actual
    comfoconnect.register_sensor(SENSOR_AVOIDED_HEATING_TOTAL_YEAR)  # Avoided Heating: Avoided year-to-date
    comfoconnect.register_sensor(SENSOR_AVOIDED_HEATING_TOTAL)  # Avoided Heating: Avoided total
    comfoconnect.register_sensor(SENSOR_TEMPERATURE_SUPPLY)  # Temperature & Humidity: Supply Air (temperature)
    comfoconnect.register_sensor(SENSOR_TEMPERATURE_EXTRACT)  # Temperature & Humidity: Extract Air (temperature)
    comfoconnect.register_sensor(SENSOR_TEMPERATURE_EXHAUST)  # Temperature & Humidity: Exhaust Air (temperature)
    comfoconnect.register_sensor(SENSOR_TEMPERATURE_OUTDOOR)  # Temperature & Humidity: Outdoor Air (temperature)
    comfoconnect.register_sensor(SENSOR_HUMIDITY_SUPPLY)  # Temperature & Humidity: Supply Air (temperature)
    comfoconnect.register_sensor(SENSOR_HUMIDITY_EXTRACT)  # Temperature & Humidity: Extract Air (temperature)
    comfoconnect.register_sensor(SENSOR_HUMIDITY_EXHAUST)  # Temperature & Humidity: Exhaust Air (temperature)
    comfoconnect.register_sensor(SENSOR_HUMIDITY_OUTDOOR)  # Temperature & Humidity: Outdoor Air (temperature)
    comfoconnect.register_sensor(SENSOR_BYPASS_STATE)  # Bypass state
    comfoconnect.register_sensor(SENSOR_OPERATING_MODE)  # Operating mode
    comfoconnect.register_sensor(SENSOR_OPERATING_MODE_BIS)  # Operating mode (bis)

    comfoconnect.register_sensor(SENSOR_PROFILE_TEMPERATURE)
    comfoconnect.register_sensor(SENSOR_AWAY)

    try:
        while True:
            sleep(1)

            try:
                if not comfoconnect.is_connected():
                    print('Reconnecting ...')
                    comfoconnect.connect(False)
                    print('Reconnected.')

            except Exception as e:
                if str(e) == 'Could not connect to the bridge since there is already an open session.':
                    sleep(1)
                else:
                    raise e

    finally:
        print('Disconnecting ...')
        comfoconnect.disconnect()


app = Flask(__name__)


def generate_metric(key):
    result = sensors[key]
    extra_labels = ""

    if result.get('unit', None) is not None:
        extra_labels += ",unit=\"C\""

    return "comfoconnect_%s{bridge_uuid=\"%s\",bridge_ip=\"%s\"%s} %s %s" % (
        sensor_names[key], bridge.uuid.hex(), bridge.host, extra_labels, result['value'], round(time.time() * 1000))


@app.route("/")
def generate_metrics():
    metrics = map(generate_metric, sensors)
    output = "\n".join(metrics)

    return Response(output, mimetype='text/plain')


if __name__ == '__main__':
    Thread(target=lambda: app.run(debug=False, host='0.0.0.0', port=9293, use_reloader=False), daemon=True).start()

    connect_comfoconnect()
