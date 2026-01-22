"""
Collect PV data from the SolarmanPV API and send Power+Energy data (W+kWh) to MQTT
"""

import argparse
import http.client
import json
import logging
import sys
import time
import datetime
import mqtt
from suntime import Sun

logging.basicConfig(level=logging.INFO)

def load_config(file):
    """
    Load configuration
    :return:
    """
    with open(file, "r", encoding="utf-8") as config_file:
        config = json.load(config_file)
        return config

def today():
    """
    Return date in YYYY-MM-DD
    :return:
    """
    date = time.strftime("%Y-%m-%d")
    return date

def get_token(url, appid, secret, username, passhash):
    """
    Get a token from the API
    :return: access_token
    """
    try:
        conn = http.client.HTTPSConnection(url)
        payload = json.dumps({
            "appSecret": secret,
            "email": username,
            "password": passhash
        })
        headers = {
            'Content-Type': 'application/json'
        }
        url = f"/account/v1.0/token?appId={appid}&language=en"
        conn.request("POST", url, payload, headers)
        res = conn.getresponse()
        data = json.loads(res.read())
        logging.debug("Received token")
        return data["access_token"]
    except Exception as error:  # pylint: disable=broad-except
        logging.error("Unable to fetch token: %s", str(error))
        sys.exit(1)

def get_station_realtime(url, stationid, token):
    """
    Return station realtime data
    :return: realtime data
    """
    conn = http.client.HTTPSConnection(url)
    payload = json.dumps({
        "stationId": stationid
    })
    headers = {
        'Content-Type': 'application/json',
        'Authorization': "bearer " + token
    }
    conn.request("POST", "/station/v1.0/realTime?language=en", payload, headers)
    res = conn.getresponse()
    data = json.loads(res.read())
    return data

def get_device_current_data(url, device_sn, token):
    """
    Return device current data
    :return: current data
    """
    conn = http.client.HTTPSConnection(url)
    payload = json.dumps({
        "deviceSn": device_sn
    })
    headers = {
        'Content-Type': 'application/json',
        'Authorization': "bearer " + token
    }
    conn.request("POST", "/device/v1.0/currentData?language=en", payload, headers)
    res = conn.getresponse()
    data = json.loads(res.read())
    return data

def restruct_and_separate_current_data(data):
    """
    Return restructured and separated device current data
    Original data is removed
    :return: new current data
    """
    new_data_list = {}
    if "dataList" in data and data["dataList"]:
        data_list = data["dataList"]
        for i in data_list:
            del i["key"]
            name = i["name"]
            name = name.replace(" ", "_")
            del i["name"]
            new_data_list[name] = i["value"]
        del data["dataList"]
    return new_data_list

def single_run(config):
    """
    Output current watts and kilowatts
    :return:
    """
    token = get_token(
        config["url"],
        config["appid"],
        config["secret"],
        config["username"],
        config["passhash"]
    )
    
    fetch_station = config.get("fetch_station", True)
    fetch_inverter = config.get("fetch_inverter", True)
    fetch_logger = config.get("fetch_logger", True)
    
    if not fetch_station and not fetch_inverter and not fetch_logger:
        logging.info("Nothing to fetch")
        return

    station_data = {}
    inverter_data = {}
    logger_data = {}
    inverter_data_list = []
    logger_data_list = []

    if fetch_station:
        station_data = get_station_realtime(config["url"], config["stationId"], token)
    
    if fetch_inverter:
        inverter_data = get_device_current_data(config["url"], config["inverterId"], token)
        inverter_data_list = restruct_and_separate_current_data(inverter_data)
        
    if fetch_logger:
        logger_data = get_device_current_data(config["url"], config["loggerId"], token)
        logger_data_list = restruct_and_separate_current_data(logger_data)

    if config["debug"]:
        if fetch_station: logging.info("Station data response: %s", json.dumps(station_data, indent=2))
        if fetch_inverter: logging.info("Inverter data response: %s", json.dumps(inverter_data, indent=2))
        if fetch_logger: logging.info("Logger data response: %s", json.dumps(logger_data, indent=2))

    discard = ["code", "msg", "requestId", "success"]
    topic = config["mqtt"]["topic"]
    _t = time.strftime("%Y-%m-%d %H:%M:%S")
    
    inverter_device_state = inverter_data.get("deviceState", 0)

    if inverter_device_state == 1:
        logging.info("%s - Inverter DeviceState: %s -> Publishing MQTT...", _t, inverter_device_state)
        
        if fetch_station:
            for i in station_data:
                if station_data[i] and i not in discard:
                    mqtt.message(config["mqtt"], topic+"/station/" + i, station_data[i])

        if fetch_inverter:
            for i in inverter_data:
                if inverter_data[i] and i not in discard:
                    mqtt.message(config["mqtt"], topic+"/inverter/" + i, inverter_data[i])
            if inverter_data_list:
                mqtt.message(config["mqtt"], topic+"/inverter/attributes", json.dumps(inverter_data_list))

        if fetch_logger:
            for i in logger_data:
                if logger_data[i] and i not in discard:
                    mqtt.message(config["mqtt"], topic+"/logger/" + i, logger_data[i])
            if logger_data_list:
                mqtt.message(config["mqtt"], topic+"/logger/attributes", json.dumps(logger_data_list))
    else:
        if fetch_inverter:
            mqtt.message(config["mqtt"], topic+"/inverter/deviceState", inverter_device_state)
        if fetch_logger:
            mqtt.message(config["mqtt"], topic+"/logger/deviceState", logger_data.get("deviceState", 0))
            
        logging.info("%s - Inverter DeviceState: %s -> Only status MQTT publish", _t, inverter_device_state)

def is_sun_active(config):
    latitude = config.get("latitude", 0.0)
    longitude = config.get("longitude", 0.0)    
    if latitude == 0.0 and longitude == 0.0:
        return True
        
    sun_margin_minutes = config.get("sunmarginminutes", 30)    
    sun = Sun(latitude, longitude)
    now = datetime.datetime.now(datetime.timezone.utc).astimezone()    
    sunrise = sun.get_local_sunrise_time()
    sunset = sun.get_local_sunset_time()
    margin = datetime.timedelta(minutes=sun_margin_minutes)    
    start_window = sunrise - margin
    end_window = sunset + margin    
    is_within_window = start_window <= now <= end_window    
    print(f"-----------------------------")       
    print(f"Current System Time:      {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"Sunrise-Sunset:           {sunrise.strftime('%H:%M:%S')} - {sunset.strftime('%H:%M:%S')}")
    print(f"Operational Window Start: {start_window.strftime('%H:%M:%S')} (Margin: -{sun_margin_minutes}m)")
    print(f"Operational Window End:   {end_window.strftime('%H:%M:%S')} (Margin: +{sun_margin_minutes}m)")
    print(f"Status:                   {'WITHIN_OPERATIONAL_WINDOW' if is_within_window else 'OUTSIDE_OPERATIONAL_WINDOW'}")    
    return is_within_window

def daemon(file, interval):
    """
    Run as a daemon process
    :param file: Config file
    :param interval: Run interval in seconds
    :return:
    """
    interval = int(interval)
    logging.info("Starting daemonized with a %s seconds run interval", str(interval))
    while True:
        try:
            config = load_config(file)
            sun_active = is_sun_active(config)
            if sun_active:
                single_run(config)
            time.sleep(interval)
        except Exception as error:  # pylint: disable=broad-except
            logging.error("Error on start: %s", str(error))
            sys.exit(1)

def main():
    """
    Main
    :return:
    """
    parser = argparse.ArgumentParser(description="Collect data from Trannergy / Solarman API")
    parser.add_argument("-d", "--daemon",
                        action="store_true",
                        help="run as a service")
    parser.add_argument("-s", "--single",
                        action="store_true",
                        help="single run and exit")
    parser.add_argument("-i", "--interval",
                        default="300",
                        help="run interval in seconds (default 300 sec.)")
    parser.add_argument("-f", "--file",
                        default="config.json",
                        help="config file (default ./config.json)")
    parser.add_argument("-v", "--version",
                        action='version',
                        version='%(prog)s 0.0.1')
    args = parser.parse_args()
    if args.single:
        single_run(args.file)
    elif args.daemon:
        daemon(args.file, args.interval)
    else:
        parser.print_help(sys.stderr)


if __name__ == '__main__':
    main()
