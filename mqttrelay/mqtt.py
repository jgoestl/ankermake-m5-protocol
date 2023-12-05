import paho.mqtt.client as mqtt
import paho.mqtt
import logging as log
import ssl
from datetime import datetime, timedelta


def mqtt_open():
    client = MqttRelay.login(
        "",
        ""
    )
    client.connect("")
    return client


class MqttRelay:

    def __init__(self, mqtt):
        self._mqtt = mqtt
        self._mqtt.on_connect    = self._on_connect
        self._mqtt.on_disconnect = self._on_disconnect
        self._mqtt.on_publish    = self.on_publish
        self._queue = []
        self._connected = False

    # internal function
    def _on_connect(self, client, userdata, flags, rc):
        if rc != 0:
            raise IOError(f"could not connect: rc={rc} ({paho.mqtt.client.error_string(rc)})")
        self._connected = True
        self.on_connect(client, userdata, flags)

    # internal function
    def _on_disconnect(self, client, userdata, rc):
        self._connected = False

    # public api: override in subclass (if needed)
    def on_connect(self, client, userdata, flags):
        log.info("Connected to mqttrelay")

    # public api: override in subclass (if needed)
    def on_publish(self, client, userdata, result):
        pass


    @classmethod
    def login(cls, username, password, verify=True):
        client = mqtt.Client()
        client.username_pw_set(username, password)

        return cls(client)

    def connect(self, server, port=1883, timeout=60):
        self._mqtt.connect(server, port, timeout)

        start = datetime.now()
        end = start + timedelta(seconds=timeout)

        while datetime.now() < end:
            timeout = (end - datetime.now()).total_seconds()
            self._mqtt.loop(timeout=timeout)
            if self._connected:
                return

        raise IOError("Timeout while connecting to mqtt server")

    def send(self, topic, msg):
        log.debug(f"Sending mqtt command on [{topic}]: {msg}")
        self._mqtt.publish(topic, payload=msg)

