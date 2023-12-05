import logging as log

from ..lib.service import Service
from .. import app

from libflagship.util import enhex
from libflagship.mqtt import MqttMsgType

import cli.mqtt
import mqttrelay.mqtt as mqttrelay


class MqttQueue(Service):

    def worker_start(self):
        self.client = cli.mqtt.mqtt_open(
            app.config["config"],
            app.config["printer_index"],
            app.config["insecure"]
        )
        self.relay_client = mqttrelay.mqtt_open()

    def worker_run(self, timeout):
        for msg, body in self.client.fetch(timeout=timeout):
            log.info(f"TOPIC [{msg.topic}]")
            log.debug(enhex(msg.payload[:]))

            for obj in body:
                self.notify(obj)

            for obj in body:
                try:
                    cmdtype = obj["commandType"]
                    name = MqttMsgType(cmdtype).name
                    if name.startswith("ZZ_MQTT_CMD_"):
                        name = name[len("ZZ_MQTT_CMD_"):].lower()

                    del obj["commandType"]
                    log.debug(f"  [{cmdtype:4}] {name:20} {obj}")
                    self.relay_client.send("AnkerMake/M5/" + f"{name}", f"{obj}")
                except Exception:
                    log.warn(f"  {obj}")

    def worker_stop(self):
        del self.client
