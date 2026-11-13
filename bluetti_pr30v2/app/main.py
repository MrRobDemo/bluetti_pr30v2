#!/usr/bin/env python3
"""Bluetti PR30V2: BLE -> MQTT bridge for Home Assistant OS.

Данные читаются через CLI `bluetti-read` (пакет bluetti-bt-lib), результат
парсится и публикуется в MQTT с авто-обнаружением (MQTT Discovery).
Управление выполняется через CLI `bluetti-write`.
"""
import asyncio
import json
import logging
import os
import re
import shlex
import signal
import urllib.request

import paho.mqtt.client as mqtt

OPTIONS_PATH = "/data/options.json"
DISCOVERY_PREFIX = "homeassistant"

# Строки вида "FieldName.AC_INPUT_VOLTAGE: 232.3V"
LINE_RE = re.compile(r"^FieldName\.([A-Za-z0-9_]+):\s*(.*?)\s*$")
# Число с необязательной единицей: "232.3V", "58W", "100%", "-3.5A"
NUM_RE = re.compile(r"^(-?\d+(?:\.\d+)?)\s*([A-Za-z%°/]*)$")

# unit -> (device_class, state_class)
UNIT_META = {
    "W": ("power", "measurement"),
    "kW": ("power", "measurement"),
    "V": ("voltage", "measurement"),
    "A": ("current", "measurement"),
    "Hz": ("frequency", "measurement"),
    "°C": ("temperature", "measurement"),
    "C": ("temperature", "measurement"),
    "%": (None, "measurement"),
    "Wh": ("energy", None),
    "kWh": ("energy", None),
}
DIAGNOSTIC_FIELDS = {"DEVICE_TYPE", "DEVICE_SN"}

log = logging.getLogger("bluetti")


def load_options() -> dict:
    with open(OPTIONS_PATH, encoding="utf-8") as f:
        return json.load(f)


def mqtt_credentials(opts: dict) -> dict:
    """Ручные настройки имеют приоритет, иначе берём у Supervisor (аддон Mosquitto)."""
    if opts.get("mqtt_host"):
        return {
            "host": opts["mqtt_host"],
            "port": int(opts.get("mqtt_port") or 1883),
            "username": opts.get("mqtt_username") or None,
            "password": opts.get("mqtt_password") or None,
        }
    token = os.environ.get("SUPERVISOR_TOKEN")
    if not token:
        raise RuntimeError("MQTT не настроен и SUPERVISOR_TOKEN недоступен")
    req = urllib.request.Request(
        "http://supervisor/services/mqtt",
        headers={"Authorization": f"Bearer {token}"},
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        data = json.load(resp)["data"]
    return {
        "host": data["host"],
        "port": int(data["port"]),
        "username": data.get("username"),
        "password": data.get("password"),
    }


def pretty(field: str) -> str:
    return field.replace("_", " ").strip().capitalize()


def parse_output(text: str) -> dict:
    """Возвращает {FIELD: (raw_value_str, number|None, unit)}."""
    result = {}
    for line in text.splitlines():
        m = LINE_RE.match(line.strip())
        if not m:
            continue
        field, raw = m.group(1), m.group(2)
        nm = NUM_RE.match(raw)
        if nm:
            result[field] = (raw, float(nm.group(1)), nm.group(2))
        else:
            result[field] = (raw, None, "")
    return result


class Bridge:
    def __init__(self, opts: dict, loop: asyncio.AbstractEventLoop):
        self.opts = opts
        self.loop = loop
        self.mac = opts["mac"].strip().upper()
        self.dev_type = opts["device_type"].strip()
        self.encryption = bool(opts.get("encryption", True))
        self.poll_interval = int(opts.get("poll_interval", 30))
        self.cmd_timeout = int(opts.get("command_timeout", 60))
        self.dev_name = opts.get("device_name") or f"Bluetti {self.dev_type}"
        self.controls = {c["field"].lower(): c for c in opts.get("controls", [])}

        self.dev_id = "bluetti_" + re.sub(r"[^a-z0-9]", "", self.mac.lower())
        self.base = f"bluetti/{self.dev_id}"
        self.avail_topic = f"{self.base}/availability"

        self.ble_lock = asyncio.Lock()          # BLE: одно соединение за раз
        self.refresh_event = asyncio.Event()
        self.announced: set[str] = set()
        self.fail_count = 0
        self.last_values: dict = {}

        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2, client_id=self.dev_id
        )
        creds = mqtt_credentials(opts)
        if creds["username"]:
            self.client.username_pw_set(creds["username"], creds["password"])
        self.client.will_set(self.avail_topic, "offline", retain=True)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.connect_async(creds["host"], creds["port"], keepalive=60)

    # ---------- MQTT ----------
    def start_mqtt(self):
        self.client.loop_start()

    def on_connect(self, client, userdata, flags, reason_code, properties=None):
        log.info("MQTT подключён: %s", reason_code)
        client.subscribe(f"{self.base}/+/set")
        client.subscribe(f"{DISCOVERY_PREFIX}/status")
        self.announced.clear()
        self.publish_control_discovery()
        client.publish(self.avail_topic, "online", retain=True)

    def on_message(self, client, userdata, msg):
        payload = msg.payload.decode(errors="replace").strip()
        if msg.topic == f"{DISCOVERY_PREFIX}/status":
            if payload == "online":  # HA перезапустился — повторяем discovery
                self.announced.clear()
                self.publish_control_discovery()
                self.publish_states(self.last_values)
            return
        m = re.match(rf"^{re.escape(self.base)}/([^/]+)/set$", msg.topic)
        if m:
            asyncio.run_coroutine_threadsafe(
                self.handle_command(m.group(1), payload), self.loop
            )

    def device_info(self) -> dict:
        return {
            "identifiers": [self.dev_id],
            "connections": [["bluetooth", self.mac]],
            "name": self.dev_name,
            "manufacturer": "Bluetti",
            "model": self.dev_type,
        }

    def publish_control_discovery(self):
        for field, ctl in self.controls.items():
            uid = f"{self.dev_id}_{field}_switch"
            cfg = {
                "name": ctl["name"],
                "unique_id": uid,
                "command_topic": f"{self.base}/{field}/set",
                "state_topic": f"{self.base}/{field}/state",
                "payload_on": "ON",
                "payload_off": "OFF",
                "state_on": "True",
                "state_off": "False",
                "optimistic": True,
                "availability_topic": self.avail_topic,
                "device": self.device_info(),
            }
            self.client.publish(
                f"{DISCOVERY_PREFIX}/switch/{uid}/config",
                json.dumps(cfg),
                retain=True,
            )

    def announce_field(self, field: str, raw: str, num, unit: str):
        if field in self.announced:
            return
        self.announced.add(field)
        lf = field.lower()
        uid = f"{self.dev_id}_{lf}"
        cfg = {
            "name": pretty(field),
            "unique_id": uid,
            "state_topic": f"{self.base}/{lf}/state",
            "availability_topic": self.avail_topic,
            "device": self.device_info(),
        }
        if field in DIAGNOSTIC_FIELDS:
            cfg["entity_category"] = "diagnostic"

        if raw in ("True", "False"):
            component = "binary_sensor"
            cfg["payload_on"] = "True"
            cfg["payload_off"] = "False"
        else:
            component = "sensor"
            if num is not None:
                dclass, sclass = UNIT_META.get(unit, (None, None))
                if field == "BATTERY_SOC":
                    dclass = "battery"
                if unit in ("Wh", "kWh") and any(
                    k in field for k in ("TOTAL", "ENERGY", "GENERATION")
                ):
                    sclass = "total_increasing"
                if unit:
                    cfg["unit_of_measurement"] = unit
                if dclass:
                    cfg["device_class"] = dclass
                if sclass:
                    cfg["state_class"] = sclass
        self.client.publish(
            f"{DISCOVERY_PREFIX}/{component}/{uid}/config",
            json.dumps(cfg),
            retain=True,
        )

    def publish_states(self, values: dict):
        for field, (raw, num, unit) in values.items():
            self.announce_field(field, raw, num, unit)
            state = (
                (str(int(num)) if num == int(num) else str(num))
                if num is not None
                else raw
            )
            self.client.publish(
                f"{self.base}/{field.lower()}/state", state, retain=True
            )

    # ---------- BLE (через CLI) ----------
    def enc_args(self) -> list[str]:
        return ["-e", "True"] if self.encryption else []

    async def run_cli(self, args: list[str]) -> tuple[int, str, str]:
        log.debug("exec: %s", " ".join(args))
        proc = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            out, err = await asyncio.wait_for(proc.communicate(), self.cmd_timeout)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            raise
        return proc.returncode, out.decode(errors="replace"), err.decode(errors="replace")

    async def read_once(self) -> dict:
        args = ["bluetti-read", "-m", self.mac, "-t", self.dev_type, *self.enc_args()]
        async with self.ble_lock:
            code, out, err = await self.run_cli(args)
        if code != 0:
            log.warning("bluetti-read завершился с кодом %s: %s", code, err.strip()[-300:])
        return parse_output(out)

    async def handle_command(self, field: str, payload: str):
        ctl = self.controls.get(field)
        if not ctl:
            log.warning("Неизвестное поле управления: %s", field)
            return
        payload = payload.upper()
        if payload not in ("ON", "OFF"):
            log.warning("Неверная команда для %s: %s", field, payload)
            return
        value_args = shlex.split(ctl["on_args"] if payload == "ON" else ctl["off_args"])
        args = [
            "bluetti-write", "-m", self.mac, "-t", self.dev_type,
            *self.enc_args(), *value_args, ctl["field"],
        ]
        log.info("Команда %s=%s", field, payload)
        try:
            async with self.ble_lock:
                code, out, err = await self.run_cli(args)
        except asyncio.TimeoutError:
            log.error("Таймаут bluetti-write")
            return
        if code != 0:
            log.error("bluetti-write код %s: %s %s", code, out.strip()[-200:], err.strip()[-300:])
            return
        log.info("bluetti-write OK: %s", out.strip()[-200:])
        await asyncio.sleep(3)          # даём устройству применить значение
        self.refresh_event.set()        # внеочередное чтение

    # ---------- основной цикл ----------
    async def poll_loop(self):
        while True:
            try:
                values = await self.read_once()
            except asyncio.TimeoutError:
                log.error("Таймаут bluetti-read")
                values = {}
            except Exception:
                log.exception("Ошибка чтения")
                values = {}

            if values:
                self.fail_count = 0
                self.last_values = values
                self.client.publish(self.avail_topic, "online", retain=True)
                self.publish_states(values)
                log.info("Получено полей: %d", len(values))
            else:
                self.fail_count += 1
                log.warning("Данные не получены (%d подряд)", self.fail_count)
                if self.fail_count >= 3:
                    self.client.publish(self.avail_topic, "offline", retain=True)

            self.refresh_event.clear()
            try:
                await asyncio.wait_for(self.refresh_event.wait(), self.poll_interval)
            except asyncio.TimeoutError:
                pass


async def main():
    opts = load_options()
    logging.basicConfig(
        level=getattr(logging, opts.get("log_level", "info").upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    loop = asyncio.get_running_loop()
    bridge = Bridge(opts, loop)
    bridge.start_mqtt()

    stop = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    task = asyncio.create_task(bridge.poll_loop())
    await stop.wait()
    task.cancel()
    bridge.client.publish(bridge.avail_topic, "offline", retain=True).wait_for_publish(2)
    bridge.client.loop_stop()
    bridge.client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
