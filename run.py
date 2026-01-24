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
    
cached_token = None
token_expiry = 0

def get_token(url, appid, secret, username, passhash):
    """
    Get a token from the API
    :return: access_token
    """
    global cached_token, token_expiry
    # Check if the cached token is still valid (using a 5-minute safety buffer)
    current_time = time.time()
    if cached_token and current_time < (token_expiry - 300):
        logging.debug("Using cached token")
        return cached_token
        
    logging.debug("Getting token")    
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

        if "access_token" in data:
            cached_token = data["access_token"]
            # Solarman tokens usually last 2 months; 
            # Use 'expires_in' from response or default to 5,184,000 seconds
            expires_in = data.get("expires_in", 5183999)
            token_expiry = current_time + int(expires_in)
            return cached_token
        else:
            raise Exception(f"API Error: {data.get('msg', 'Unknown error')}")        
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
    if not token:
        logging.error("No valid token available. Skipping this run.")
        return
        
    fetch_station = config.get("fetch_station", True)
    fetch_inverter = config.get("fetch_inverter", True)
    fetch_logger = config.get("fetch_logger", True)
    
    if not any([fetch_station, fetch_inverter, fetch_logger]):
        logging.info("Nothing to fetch (all modules disabled)")
        return

    station_data = get_station_realtime(config["url"], config["stationId"], token) if fetch_station else {}
    inverter_raw = get_device_current_data(config["url"], config["inverterId"], token) if fetch_inverter else {}
    logger_raw = get_device_current_data(config["url"], config["loggerId"], token) if fetch_logger else {}

    inverter_attr = restruct_and_separate_current_data(inverter_raw)
    logger_attr = restruct_and_separate_current_data(logger_raw)

    discard = ["code", "msg", "requestId", "success", "dataList"]
    topic = config["mqtt"]["topic"]
    _t = time.strftime("%Y-%m-%d %H:%M:%S")
    
    inverter_state = inverter_raw.get("deviceState", 0)

    if inverter_state == 1:
        logging.info("%s - Inverter Online -> Publishing Data", _t)
        
        if fetch_station:
            for k, v in station_data.items():
                if v is not None and k not in discard:
                    mqtt.message(config["mqtt"], f"{topic}/station/{k}", v)

        if fetch_inverter:
            for k, v in inverter_raw.items():
                if v is not None and k not in discard:
                    mqtt.message(config["mqtt"], f"{topic}/inverter/{k}", v)
            if inverter_attr:
                mqtt.message(config["mqtt"], f"{topic}/inverter/attributes", json.dumps(inverter_attr))

        if fetch_logger:
            for k, v in logger_raw.items():
                if v is not None and k not in discard:
                    mqtt.message(config["mqtt"], f"{topic}/logger/{k}", v)
            if logger_attr:
                mqtt.message(config["mqtt"], f"{topic}/logger/attributes", json.dumps(logger_attr))
    else:
        if fetch_inverter: mqtt.message(config["mqtt"], f"{topic}/inverter/deviceState", inverter_state)
        logging.info("%s - Inverter Offline/Night (State: %s)", _t, inverter_state)
        
def is_sun_active(config):
    latitude = config.get("latitude", 0.0)
    longitude = config.get("longitude", 0.0)    
    if latitude == 0.0 and longitude == 0.0:
        return True
        
    sun = Sun(latitude, longitude)
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    sunrise_utc = sun.get_sunrise_time(now_utc)
    sunset_utc = sun.get_sunset_time(now_utc)
    now_local = now_utc.astimezone()
    sunrise_local = sunrise_utc.astimezone()
    sunset_local = sunset_utc.astimezone()
    tz_name = now_local.strftime('%Z')
    tz_offset = now_local.strftime('%z')
    sun_margin_minutes = config.get("sunmarginminutes", 30)
    margin = datetime.timedelta(minutes=sun_margin_minutes)
    start_window = sunrise_local - margin
    end_window = sunset_local + margin
    is_within_window = start_window <= now_local <= end_window
    logging.info(f"-----------------------------")
    logging.info(f"Current Timezone:                    {tz_name} ({tz_offset})")
    logging.info(f"Current Local Time:                  {now_local.strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"Sunrise-Sunset (Local):              {sunrise_local.strftime('%H:%M:%S')} - {sunset_local.strftime('%H:%M:%S')}")
    logging.info(f"Operational Window (Margin: +/-{sun_margin_minutes}m): {start_window.strftime('%H:%M:%S')} - {end_window.strftime('%H:%M:%S')}")
    logging.info(f"Status:                              {'Active' if is_within_window else 'Awaiting sun'}")
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
