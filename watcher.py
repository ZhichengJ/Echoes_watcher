#!/usr/bin/python

__author__ = "Samuel Lemes Perera @slemesp"

import os
import sys
import time
import json
# import time

# pip install psutil --user
import psutil
import threading

# pip install configparser --user
import configparser

from datetime import datetime

# pip install paho-mqtt --user
import paho.mqtt.client as mqtt

# pip install watchdog --user
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


import logging
import logging.handlers as handlers

logger = logging.getLogger('echoes_watcher')
logger.setLevel(logging.INFO)

logHandler = handlers.RotatingFileHandler(os.path.join(os.path.dirname(
    os.path.realpath(__file__)), 'watcher.log'), maxBytes=1000000, backupCount=2)
logHandler.setLevel(logging.INFO)
logger.addHandler(logHandler)


# Configuration variables
DIRECTORY_TO_WATCH = os.path.expanduser(os.path.expandvars("$HOME/echoes"))
ECHOES_CONFIG_FILE = os.path.expanduser(os.path.expandvars("$HOME/echoes/default.rts"))

MQTT_HOST = 'vps190.cesvima.upm.es'
MQTT_PORT = 1883
MQTT_TOPIC = "station/echoes/event/%s"
MQTT_TOPIC_REGISTER = "station/echoes/register"
# MQTT_TOPIC_FINISH = "station/echoes/finish"
MQTT_TOPIC_SERVER_UP = "server/status/up"
MQTT_TOPIC_GRAFANA = "station/grafana/event"

STATIONNAME = "None"

# Global methods
Event = None


class Watcher:

    def __init__(self, dir_path):
        self.DIRECTORY_TO_WATCH = dir_path

        self.observer = Observer()

    def run(self, callback):
        event_handler = Handler()
        self.observer.schedule(event_handler, self.DIRECTORY_TO_WATCH, recursive=True)
        self.observer.start()
        try:
            while True:
                time.sleep(2)
        except Exception as e:
            self.observer.stop()
            logger.error(e)

        self.observer.join()


class Handler(FileSystemEventHandler):

    @staticmethod
    def on_any_event(event):
        if event.is_directory:
            return None

        elif event.event_type == 'created':

            try:
                historicalSize = -1
                while (historicalSize != os.path.getsize(event.src_path)):
                    historicalSize = os.path.getsize(event.src_path)
                    time.sleep(0.1)
            except Exception as e:
                logger.error(e)
                return None

            download_thread = threading.Thread(target=sendMQTTMessage, args=(Event.extractEvent(event.src_path),))
            download_thread.start()


class EventParser():
    def __init__(self, peak_upper, peak_lower, same_event):
        self.peak_upper = 15
        self.peak_lower = 12
        self.same_event = same_event / 1000
        self.last_time = 1

    def __readFile(self, file_path):
        # on read file get only first event
        with open(file_path) as f:
            logger.info("Reading file %s " % (file_path))
            data = []
            t = []
            power = []
            resultado = 0
            idx_close_event = None

            for line in f.readlines():
                line = line.replace('\n',' ')
                if (len(line.split('   ')) < 2):
                    continue
                data.append(line.split('   '))
            
            for h,line in enumerate(data):
                if len(line) > 3:
                    nFrequencies = h
                    nBtwFrequencies = h + 1
                    break
            
            logger.info("Data registered correctly")
            
            ini = nFrequencies
            signal = 0
            
            # Inicio de la señal
            while ini < len(data):
                if(len(data[ini]) > 3 and float(data[ini][6]) > float(self.peak_upper)):
                    signal = 1
                    logger.info("Signal detected")
                    break
                ini += 1
            if (signal == 0):
                logger.warning("No signal detected")
                return [],[]
            
            logger.info("Processing data")
            
            i = ini
            # Comprueba el fin de la señal
            while i < len(data):
                # Señal baja del umbral
                if(len(data[i]) > 3 and float(data[i][6]) < float(self.peak_lower)):
                    fin = i
                    j = i
                    logger.info("Lower")
                    # comprueba si la señal vuelve a subir durante 1s
                    dur = 0.0
                    while dur < float(1) and j < len(data):
                        dur = float(data[j][0])-float(data[ini][0])
                        if(len(data[j]) > 3 and float(data[j][6]) > self.peak_upper):
                            resultado = -1
                            logger.info("Upper")
                            break
                        j += 1
                    if (resultado == 0):
                        break
                # La señal sube del umbral
                i = i + 1
            index = ini - nFrequencies
            
            logger.info("Data processed successfully")
            logger.info(data[index+1][0])
            
            if (resultado == -1):
                logger.info(str(ini) + ':' + str(j) + ' ' + str(len(data)))
                while index < j:
                    t.append(float(data[index][0]))
                    power.append(float(data[index][2]))
                    index = index + 1
                return t, power
            else:
                logger.info(str(ini) + ':' + str(fin) + ' ' + str(len(data)))
                while index < fin:
                    t.append(float(data[index][0]))
                    power.append(float(data[index][2]))
                    index = index + 1
                return t, power
            
            
    def __convert2Json(self, t, s_n, event_id):
        return {"t": t, "s_n": s_n, "peak_lower": self.peak_lower, "event_id": event_id}

    def __getEventID(self, fname):
        try:
            mystring = os.path.basename(fname).split(".")[0]
            start = mystring.find('(')
            end = mystring.find(')')
            if start != -1 and end != -1:
                mystring = mystring[:start + 1] + mystring[end:]

            return (filter(str.isdigit, mystring))[-13:]
        except Exception as e:
            logger.error(e)
            return datetime.now().strftime("%Y%m%d%H%M?")

    def extractEvent(self, file_path):
        if not file_path or not file_path.endswith(".dat") or not os.path.isfile(file_path):
            return None

        try:
            t, s_n = self.__readFile(file_path)
            logger.info("Extracted %s values" % len(t))
            if not t or not s_n:
                return None

            self.last_time = t[-1]
            return self.__convert2Json(t, s_n, self.__getEventID(file_path))
        except Exception as e:
            logger.error(e)
            return None


def sendMQTTMessage(data):
    if not data:
        logger.warning("No datas")
        return
    logger.info("Send:    " + str(data))

    mqtt_client = mqtt.Client()
    try:
        mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)
        mqtt_client.loop_start()
        mqtt_client.publish(MQTT_TOPIC, json.dumps(data))
        mqtt_client.loop_stop()

        sendMQTTGrafana(data)

    except Exception as e:
        logger.error("Warning!!! MQTT error: %s" % (e))


def sendMQTTGrafana(data):
    if 't' in data and 's_n' in data and 'event_id' in data:
        mqtt_client = mqtt.Client()
        mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)
        mqtt_client.loop_start()
        logger.info("Start sending datas")	
        for idx, t in enumerate(data['t']):
            mqtt_client.publish(MQTT_TOPIC_GRAFANA, json.dumps({
                # 'time': time.mktime(datetime.fromtimestamp(float(data['t'][idx])).timetuple()),
                'time': data['t'][idx],
                's_n': data['s_n'][idx],
                'name': STATIONNAME,
                # 'peak_lower':  data['peak_lower'],
                'event': data['event_id'],
                'type': "unknown"
            }))
        mqtt_client.loop_stop()


def sendMQTTRegister():
    logger.info('sendMQTTRegister')
    if not STATIONNAME or STATIONNAME == "None":
        logger.warning("No station name")
        return

    mqtt_client = mqtt.Client()
    try:
        mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)
        mqtt_client.loop_start()
        mqtt_client.publish(MQTT_TOPIC_REGISTER, STATIONNAME)
        mqtt_client.loop_stop()

    except Exception as e:
        logger.error("Warning!!! MQTT error: %s" % (e))


# def sendMQTTFinish():
#     if not STATIONNAME or STATIONNAME == "None":
#         logger.warning("No station name")
#         return
#
#     mqtt_client = mqtt.Client()
#     try:
#         mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)
#         mqtt_client.loop_start()
#         mqtt_client.publish(MQTT_TOPIC_FINISH, STATIONNAME)
#         mqtt_client.loop_stop()
#
#     except Exception as e:
#         logger.error("Warning!!! MQTT error: %s" % (e))
#

def listen2server():
    mqtt_client = mqtt.Client()
    try:
        mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)
        mqtt_client.on_message = on_server_status_message
        mqtt_client.subscribe(MQTT_TOPIC_SERVER_UP)
        mqtt_client.loop_start()
        # mqtt_client.loop_forever()
    except Exception as e:
        logger.error("Warning!!! MQTT error: %s" % (e))


def on_server_status_message(client, userdata, message):
    if bool(message.payload):
        sendMQTTRegister()
        # listen2server()


def filter_nonprintable(text):
    import string
    nonprintable = set([chr(i) for i in range(128)]).difference(string.printable)
    return text.translate({ord(character): None for character in nonprintable})


def isRunning():
    count = 0
    for q in psutil.process_iter():
        if q.name() == 'python':
            cmdLine = q.cmdline()
            if cmdLine and os.path.basename(__file__) in cmdLine[1]:
                count += 1

    return count > 1


if __name__ == '__main__':

    if isRunning():
        print("Previous Script running. Please kill previous process and execute newly.")
        sys.exit()

    # Read arguments
    if len(sys.argv) > 1:
        ECHOES_CONFIG_FILE = os.path.expanduser(os.path.expandvars(sys.argv[1]))

    if len(sys.argv) > 2:
        DIRECTORY_TO_WATCH = os.path.expanduser(os.path.expandvars(sys.argv[2]))

    if not os.path.isfile(ECHOES_CONFIG_FILE) or not os.path.isdir(DIRECTORY_TO_WATCH):
        print("Error: ")
        print("\tEchoes configuration file or working directory not exists")
        print("")
        print("\tThe script can optionally receive two parameters: ECHOES configuration file and working directory")
        print("\tBy default, the configuration file is: %s and the working directory: %s" % (ECHOES_CONFIG_FILE, DIRECTORY_TO_WATCH))
        print("")
        print("\tTry with:  %s [path_to_echoes_config.rts [path_to_echoes_working_directory]] " % sys.argv[0])
        print("")
        exit()

    config = configparser.ConfigParser()
    config.read(ECHOES_CONFIG_FILE)

    Event = EventParser(float(config['Output%20settings']['ShotUpThreshold']), float(
        config['Output%20settings']['ShotDnThreshold']), float(config['Output%20settings']['JoinTime']))

    STATIONNAME = filter_nonprintable(config['Site%20infos']['StationName'].replace("/", "-").replace("\\", "-").replace(" ", "_")).lower()
    if STATIONNAME == "None" or STATIONNAME == "none":
        print("Error: ")
        print("\tStation Name not assigned")
        print("")
        print("\tGo to \"Reporting\" tab and then in \"Report Content\" section click on \"site Infos\" and fill \"Station Name\" and save it.")
        print("\tRemember store the new configuration file using \"Save As\" button.")
        print("")
        exit()

    MQTT_TOPIC = MQTT_TOPIC % (STATIONNAME)

    print("")
    print("Configuration File:   %s " % (ECHOES_CONFIG_FILE))
    print("Working Directory:    %s " % (DIRECTORY_TO_WATCH))
    print("Station Name:         %s " % (config['Site%20infos']['StationName']))
    print("Peak Upper Threshold: %s dbfs" % (config['Output%20settings']['ShotUpThreshold']))
    print("Peak Lower Threshold: %s dbfs" % (config['Output%20settings']['ShotDnThreshold']))
    print("Join time:            %s ms" % (config['Output%20settings']['JoinTime']))
    print("")
    print("Topic:                %s" % (MQTT_TOPIC))
    print("")
    print("Please, check that this data is correct, otherwise, cancel the program and execute it with the configuration file and the working directory:")
    print(" %s [path_to_echoes_config.rts [path_to_echoes_working_directory]]" % sys.argv[0])
    print("")

    listen2server()
    sendMQTTRegister()

    w = Watcher(DIRECTORY_TO_WATCH)
    w.run(sendMQTTMessage)
