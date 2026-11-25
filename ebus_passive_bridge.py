#!/usr/bin/env python3
"""
ebus_passive_bridge.py
Passive eBUS listener for Vaillant aroTHERM via C6 Adapter (TCP plain mode).
Decodes live broadcast telemetry without writing to or arbitrating on the bus.
Publishes to both ebusd-compatible MQTT topics and dedicated Home Assistant Discovery entities.
"""

import os
import sys
import time
import json
import socket
import signal
import logging
import struct
import paho.mqtt.client as mqtt

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("ebus_passive_bridge")

def _load_dotenv(path: str) -> None:
    if not os.path.isfile(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))


_load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

# Adapter & MQTT Configuration (override via environment or local .env)
ADAPTER_HOST = os.getenv("EBUS_ADAPTER_HOST", "127.0.0.1")
ADAPTER_PORT = int(os.getenv("EBUS_ADAPTER_PORT", "9999"))
MQTT_BROKER = os.getenv("MQTT_BROKER", "127.0.0.1")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USER = os.getenv("MQTT_USER", "")
MQTT_PASS = os.getenv("MQTT_PASS", "")
DEFAULT_HEATING_DAY_TARGET = float(os.getenv("HEATING_DAY_TARGET", "20.0"))
DEFAULT_HEATING_CURVE = float(os.getenv("HEATING_CURVE", "0.6"))
HEATING_MIN_FLOW = float(os.getenv("HEATING_MIN_FLOW", "30.0"))
HEATING_MAX_FLOW = float(os.getenv("HEATING_MAX_FLOW", "45.0"))
HEATPUMP_MODEL = os.getenv("HEATPUMP_MODEL", "aroTHERM (HMU Passive Stream)")
EBUS_DEBUG_DUMP = os.getenv("EBUS_DEBUG_DUMP", "0").lower() in ("1", "true", "yes")

# eBUS CRC-8 lookup table for polynomial 0x9B (x^8 + x^7 + x^4 + x^3 + x + 1)
CRC_LOOKUP_TABLE = [
    0x00, 0x9B, 0xAD, 0x36, 0xC1, 0x5A, 0x6C, 0xF7, 0x19, 0x82, 0xB4, 0x2F, 0xD8, 0x43, 0x75, 0xEE,
    0x32, 0xA9, 0x9F, 0x04, 0xF3, 0x68, 0x5E, 0xC5, 0x2B, 0xB0, 0x86, 0x1D, 0xEA, 0x71, 0x47, 0xDC,
    0x64, 0xFF, 0xC9, 0x52, 0xA5, 0x3E, 0x08, 0x93, 0x7D, 0xE6, 0xD0, 0x4B, 0xBC, 0x27, 0x11, 0x8A,
    0x56, 0xCD, 0xFB, 0x60, 0x97, 0x0C, 0x3A, 0xA1, 0x4F, 0xD4, 0xE2, 0x79, 0x8E, 0x15, 0x23, 0xB8,
    0xC8, 0x53, 0x65, 0xFE, 0x09, 0x92, 0xA4, 0x3F, 0xD1, 0x4A, 0x7C, 0xE7, 0x10, 0x8B, 0xBD, 0x26,
    0xFA, 0x61, 0x57, 0xCC, 0x3B, 0xA0, 0x96, 0x0D, 0xE3, 0x78, 0x4E, 0xD5, 0x22, 0xB9, 0x8F, 0x14,
    0xAC, 0x37, 0x01, 0x9A, 0x6D, 0xF6, 0xC0, 0x5B, 0xB5, 0x2E, 0x18, 0x83, 0x74, 0xEF, 0xD9, 0x42,
    0x9E, 0x05, 0x33, 0xA8, 0x5F, 0xC4, 0xF2, 0x69, 0x87, 0x1C, 0x2A, 0xB1, 0x46, 0xDD, 0xEB, 0x70,
    0x0B, 0x90, 0xA6, 0x3D, 0xCA, 0x51, 0x67, 0xFC, 0x12, 0x89, 0xBF, 0x24, 0xD3, 0x48, 0x7E, 0xE5,
    0x39, 0xA2, 0x94, 0x0F, 0xF8, 0x63, 0x55, 0xCE, 0x20, 0xBB, 0x8D, 0x16, 0xE1, 0x7A, 0x4C, 0xD7,
    0x6F, 0xF4, 0xC2, 0x59, 0xAE, 0x35, 0x03, 0x98, 0x76, 0xED, 0xDB, 0x40, 0xB7, 0x2C, 0x1A, 0x81,
    0x5D, 0xC6, 0xF0, 0x6B, 0x9C, 0x07, 0x31, 0xAA, 0x44, 0xDF, 0xE9, 0x72, 0x85, 0x1E, 0x28, 0xB3,
    0xC3, 0x58, 0x6E, 0xF5, 0x02, 0x99, 0xAF, 0x34, 0xDA, 0x41, 0x77, 0xEC, 0x1B, 0x80, 0xB6, 0x2D,
    0xF1, 0x6A, 0x5C, 0xC7, 0x30, 0xAB, 0x9D, 0x06, 0xE8, 0x73, 0x45, 0xDE, 0x29, 0xB2, 0x84, 0x1F,
    0xA7, 0x3C, 0x0A, 0x91, 0x66, 0xFD, 0xCB, 0x50, 0xBE, 0x25, 0x13, 0x88, 0x7F, 0xE4, 0xD2, 0x49,
    0x95, 0x0E, 0x38, 0xA3, 0x54, 0xCF, 0xF9, 0x62, 0x8C, 0x17, 0x21, 0xBA, 0x4D, 0xD6, 0xE0, 0x7B,
]

def calc_crc(data: bytes) -> int:
    crc = 0
    for b in data:
        crc = CRC_LOOKUP_TABLE[crc] ^ b
    return crc

# State name mapping from aroTHERM HMU
STATE_MAPPING = {
    0x01: ("standby", "Standby"),
    0x09: ("heat_compressor_active", "Heating Active"),
    0x81: ("hwc_compressor_active", "DHW Heating Active"),
    0x11: ("cool_compressor_active", "Cooling Active"),
    0x0B: ("device_error", "Error"),
}

class EbusPassiveBridge:
    def __init__(self):
        self.running = True
        self.mqtt_client = None
        self.sock = None
        self.raw_buffer = bytearray()
        self.decoded_buffer = bytearray()

        # Telemetry Cache
        self.flow_temp = None
        self.circuit_flow_temp = None
        self.return_temp = None
        self.outdoor_temp = None
        self.dhw_temp = None
        self.pressure = 1.8  # default baseline bar
        self.state_code = "standby"
        self.state_name = "Standby"
        self.dhw_mode = "auto"
        self.heating_mode = "auto"
        self.heating_day_target = DEFAULT_HEATING_DAY_TARGET
        self.heating_curve = DEFAULT_HEATING_CURVE
        self.target_flow_temp = None
        self.flow_rate = None  # l/h
        self.compressor_elec_power = None  # Watts (from EEBUS acpowertotal)
        self.thermal_power_kw = None  # kW
        self.live_cop = None  # instantaneous COP
        self.yield_power_kw = None  # kW (from B5 1A 23)
        self.consumed_power_kw = None  # kW (from B5 1A 24)
        self.cop_hc = None  # COP (from B5 1A 11)
        self.energy_integral = None  # °min (from B5 1A 21)
        self.compressor_util = None  # % (from B5 1A 25)
        self.cumulative_energy_kwh = None  # kWh
        self.cumulative_elec_kwh = None  # kWh
        self.last_pub_time = 0
        self.last_integration_time = time.time()
        self.last_summary_time = 0
        self.last_poll_time = 0
        self.poll_interval = 30.0  # seconds between opportunistic quiet-window injections

    def setup_mqtt(self):
        client = mqtt.Client(client_id="ebus_passive_bridge", protocol=mqtt.MQTTv311)
        if MQTT_USER:
            client.username_pw_set(MQTT_USER, MQTT_PASS or None)
        client.will_set("ebus_passive/availability", "offline", retain=True)

        def on_connect(c, userdata, flags, rc):
            if rc == 0:
                logger.info("Connected to MQTT broker")
                c.subscribe("ship/#")
                c.publish("ebus_passive/availability", "online", retain=True)
                self.publish_ha_discovery(c)
            else:
                logger.error(f"MQTT connection failed with code {rc}")

        def on_message(c, userdata, msg):
            try:
                payload_str = msg.payload.decode("utf-8").strip()
                if "acpowertotal" in msg.topic:
                    val = float(payload_str)
                    if self.compressor_elec_power is None or abs(self.compressor_elec_power - val) > 1.0:
                        self.compressor_elec_power = val
                        self.update_hvac_action()
                elif "dhwtemperature" in msg.topic:
                    self.dhw_temp = float(payload_str)
                elif "dhw/mode/state" in msg.topic:
                    self.dhw_mode = payload_str.lower()
                    self.update_hvac_action()
                elif "heating/mode/state" in msg.topic:
                    self.heating_mode = payload_str.lower()
                    self.update_hvac_action()
                elif "heating/temp/state" in msg.topic:
                    try:
                        self.heating_day_target = float(payload_str)
                    except ValueError:
                        pass
                elif "outsideairtemperature" in msg.topic:
                    val = float(payload_str)
                    if self.outdoor_temp is None or abs(self.outdoor_temp - val) > 0.05:
                        self.outdoor_temp = val
            except Exception:
                pass

        client.on_connect = on_connect
        client.on_message = on_message
        client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
        client.loop_start()
        self.mqtt_client = client

    def publish_ha_discovery(self, client: mqtt.Client):
        device_info = {
            "identifiers": ["vaillant_arotherm_ebus_passive"],
            "name": "Vaillant aroTHERM Passive eBUS",
            "model": HEATPUMP_MODEL,
            "manufacturer": "Vaillant / eBUS Adapter C6",
            "sw_version": "1.0",
            "suggested_area": "Heating"
        }

        sensors = [
            {
                "topic": "homeassistant/sensor/arotherm_ebus_flow_temperature/config",
                "payload": {
                    "unique_id": "arotherm_ebus_flow_temperature",
                    "name": "eBUS Heat Pump Flow Temperature",
                    "device": device_info,
                    "state_topic": "ebus_passive/flow_temperature/state",
                    "availability_topic": "ebus_passive/availability",
                    "unit_of_measurement": "°C",
                    "device_class": "temperature",
                    "state_class": "measurement"
                }
            },
            {
                "topic": "homeassistant/sensor/arotherm_ebus_dhw_temp/config",
                "payload": {
                    "unique_id": "arotherm_ebus_dhw_temp",
                    "name": "eBUS DHW Cylinder Temperature",
                    "device": device_info,
                    "state_topic": "ebus_passive/dhw_temp/state",
                    "availability_topic": "ebus_passive/availability",
                    "unit_of_measurement": "°C",
                    "device_class": "temperature",
                    "state_class": "measurement"
                }
            },
            {
                "topic": "homeassistant/sensor/arotherm_ebus_circuit_flow_temperature/config",
                "payload": {
                    "unique_id": "arotherm_ebus_circuit_flow_temperature",
                    "name": "eBUS Circuit Flow Temperature",
                    "device": device_info,
                    "state_topic": "ebus_passive/circuit_flow_temperature/state",
                    "availability_topic": "ebus_passive/availability",
                    "unit_of_measurement": "°C",
                    "device_class": "temperature",
                    "state_class": "measurement"
                }
            },
            {
                "topic": "homeassistant/sensor/arotherm_ebus_water_pressure/config",
                "payload": {
                    "unique_id": "arotherm_ebus_water_pressure",
                    "name": "eBUS Water Pressure",
                    "device": device_info,
                    "state_topic": "ebus_passive/pressure/state",
                    "availability_topic": "ebus_passive/availability",
                    "unit_of_measurement": "bar",
                    "device_class": "pressure",
                    "state_class": "measurement",
                    "icon": "mdi:gauge"
                }
            },
            {
                "topic": "homeassistant/sensor/arotherm_ebus_outdoor_temperature/config",
                "payload": {
                    "unique_id": "arotherm_ebus_outdoor_temperature",
                    "name": "eBUS Outdoor Temperature",
                    "device": device_info,
                    "state_topic": "ebus_passive/outdoor_temperature/state",
                    "availability_topic": "ebus_passive/availability",
                    "unit_of_measurement": "°C",
                    "device_class": "temperature",
                    "state_class": "measurement"
                }
            },
            {
                "topic": "homeassistant/sensor/arotherm_ebus_operation_state/config",
                "payload": {
                    "unique_id": "arotherm_ebus_operation_state",
                    "name": "eBUS Operating State",
                    "device": device_info,
                    "state_topic": "ebus_passive/operation_state/state",
                    "availability_topic": "ebus_passive/availability",
                    "icon": "mdi:heat-pump-outline"
                }
            },
            {
                "topic": "homeassistant/sensor/arotherm_ebus_compressor_power/config",
                "payload": {
                    "unique_id": "arotherm_ebus_compressor_power",
                    "name": "eBUS Compressor Electrical Power",
                    "device": device_info,
                    "state_topic": "ebus_passive/compressor_power/state",
                    "availability_topic": "ebus_passive/availability",
                    "unit_of_measurement": "W",
                    "device_class": "power",
                    "state_class": "measurement"
                }
            },
            {
                "topic": "homeassistant/sensor/arotherm_ebus_target_flow_temp/config",
                "payload": {
                    "unique_id": "arotherm_ebus_target_flow_temp",
                    "name": "eBUS Target Flow Temperature",
                    "device": device_info,
                    "state_topic": "ebus_passive/target_flow_temp/state",
                    "availability_topic": "ebus_passive/availability",
                    "unit_of_measurement": "°C",
                    "device_class": "temperature",
                    "state_class": "measurement",
                    "icon": "mdi:thermometer-chevron-up"
                }
            },
            {
                "topic": "homeassistant/sensor/arotherm_ebus_cumulative_energy/config",
                "payload": {
                    "unique_id": "arotherm_ebus_cumulative_energy",
                    "name": "eBUS Cumulative Energy",
                    "device": device_info,
                    "state_topic": "ebus_passive/cumulative_energy/state",
                    "availability_topic": "ebus_passive/availability",
                    "unit_of_measurement": "kWh",
                    "device_class": "energy",
                    "state_class": "total_increasing"
                }
            },
            {
                "topic": "homeassistant/binary_sensor/arotherm_ebus_bridge_connected/config",
                "payload": {
                    "unique_id": "arotherm_ebus_bridge_connected",
                    "name": "eBUS Passive Bridge Connected",
                    "device": device_info,
                    "state_topic": "ebus_passive/bridge_connected/state",
                    "availability_topic": "ebus_passive/availability",
                    "device_class": "connectivity"
                }
            }
        ]

        for s in sensors:
            client.publish(s["topic"], json.dumps(s["payload"]), retain=True)

        logger.info(f"Published {len(sensors)} Home Assistant MQTT Discovery entity configs")

    def connect_adapter(self):
        while self.running:
            try:
                logger.info(f"Connecting to C6 adapter at {ADAPTER_HOST}:{ADAPTER_PORT}...")
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(15.0)
                sock.connect((ADAPTER_HOST, ADAPTER_PORT))
                self.sock = sock
                if self.mqtt_client:
                    self.mqtt_client.publish("ebus_passive/bridge_connected/state", "ON", retain=True)
                logger.info("Successfully connected to C6 adapter stream")
                return
            except Exception as e:
                logger.warning(f"Could not connect to C6 adapter: {e}. Retrying in 5s...")
                if self.mqtt_client:
                    self.mqtt_client.publish("ebus_passive/bridge_connected/state", "OFF", retain=True)
                time.sleep(5)

    def process_broadcast_frame(self, frame: bytes):
        """Parse verified CRC eBUS broadcast frame."""
        if frame[0] == 0x00 and len(frame) >= 3:
            length = frame[1]
            payload = frame[2:-1]  # exclude master byte, length byte, and CRC byte
            if len(payload) != length:
                return

            if length != 0x07:
                logger.debug(f"[Broadcast/Reply 00] len={length:02x}: {payload.hex(' ')}")

            # Telegram 1: Auxiliary / DHW Cylinder Sensor (VR10 analog thermistor)
            # Format: 00 07 [temp_lsb] [temp_msb] 00 00 80 ff 00 [crc]
            if length == 0x07 and len(payload) >= 2:
                raw_dhw = int.from_bytes(payload[0:2], byteorder="little", signed=True)
                if raw_dhw > 0:
                    self.dhw_temp = round(raw_dhw / 16.0, 2)

            # Telegram 2: HMU Flow / Supply Temperature (Status02 D1C)
            elif length == 0x09 and len(payload) >= 6:
                f_byte = payload[5]
                if 20 <= f_byte <= 140:
                    self.flow_temp = round(f_byte / 2.0, 1)

            # Telegram 3: Pressure
            elif length == 0x0A and len(payload) >= 2:
                p_val = payload[0]
                if 5 <= p_val <= 40:
                    self.pressure = round(p_val / 10.0, 2)  # bar

            # Telegram 4: Operational State
            elif length == 0x01 and len(payload) >= 1:
                state_val = payload[0]
                if state_val in STATE_MAPPING:
                    self.state_code, self.state_name = STATE_MAPPING[state_val]

        self.publish_telemetry()

    def process_master_slave(self, qq: int, zz: int, pb: int, sb: int, m_data: bytes, s_data: bytes):
        """Parse snooped Master-Slave transaction between controllers."""
        logger.debug(
            f"[Snoop M-S] {qq:02x}->{zz:02x} cmd={pb:02x}{sb:02x} "
            f"m_data={m_data.hex(' ')} s_data={s_data.hex(' ')}"
        )

        # Exchanges targeting HMU (0x08)
        if zz == 0x08:
            # Command B5 11: HMU State and Energy
            if pb == 0xB5 and sb == 0x11:
                logger.info(f"[b511 07 reply] s_data ({len(s_data)} bytes): {s_data.hex(' ')}")
                if len(m_data) >= 1 and m_data[0] == 0x07 and len(s_data) >= 5:
                    energy_dec = s_data[0]
                    energy_int = int.from_bytes(s_data[1:3], byteorder="little", signed=False)
                    onoff = s_data[3]
                    st = s_data[4]
                    dec = energy_dec / 10.0 if energy_dec < 10 else energy_dec / 100.0
                    self.cumulative_energy_kwh = round(energy_int + dec, 1)
                    if onoff == 1 and self.state_code == "standby":
                        self.state_code = "heat_compressor_active"
                        self.state_name = "Heating Active"
                    elif onoff == 0 and self.compressor_elec_power is not None and self.compressor_elec_power < 30:
                        self.state_code = "standby"
                        self.state_name = "Standby"

            # Command B5 1A: Live Monitor & Yield Registers
            elif pb == 0xB5 and sb == 0x1A:
                if len(m_data) >= 2 and len(s_data) >= 4:
                    ext_id = m_data[-1]
                    # Format of s_data is [ign0, ign1, ign2, val_bytes...]
                    payload = s_data[3:]

                    if ext_id == 0x10 and len(payload) >= 4:  # YieldHc (kWh)
                        val = int.from_bytes(payload[:4], byteorder="little", signed=False)
                        logger.debug(f"[eBUS HMU] YieldHc: {val} kWh")
                    elif ext_id == 0x11 and len(payload) >= 2:  # CopHc
                        cop_raw = int.from_bytes(payload[:2], byteorder="little", signed=False)
                        if cop_raw > 0:
                            self.cop_hc = round(cop_raw / 10.0, 2)
                    elif ext_id == 0x16 and len(payload) >= 4:  # YieldHwc (kWh)
                        val = int.from_bytes(payload[:4], byteorder="little", signed=False)
                        logger.debug(f"[eBUS HMU] YieldHwc: {val} kWh")
                    elif ext_id == 0x17 and len(payload) >= 2:  # CopHwc
                        cop_raw = int.from_bytes(payload[:2], byteorder="little", signed=False)
                        logger.debug(f"[eBUS HMU] CopHwc: {cop_raw / 10.0}")
                    elif ext_id == 0x21 and len(payload) >= 2:  # EnergyIntegral (°min)
                        val = int.from_bytes(payload[:2], byteorder="little", signed=True)
                        self.energy_integral = val
                    elif ext_id == 0x23 and len(payload) >= 1:  # CurrentYieldPower (kW)
                        val = int.from_bytes(payload[:1], byteorder="little", signed=True) / 10.0
                        self.yield_power_kw = max(0.0, round(val, 2))
                    elif ext_id == 0x24 and len(payload) >= 1:  # CurrentConsumedPower (kW)
                        val = int.from_bytes(payload[:1], byteorder="little", signed=True) / 10.0
                        self.consumed_power_kw = max(0.0, round(val, 2))
                    elif ext_id == 0x25 and len(payload) >= 1:  # CurrentCompressorUtil (%)
                        val = payload[0]
                        if 0 <= val <= 100:
                            self.compressor_util = val
                    elif ext_id == 0x3C and len(payload) >= 2:  # BuildingCircuitFlow (l/h)
                        flow = int.from_bytes(payload[:2], byteorder="little", signed=False)
                        if 0 <= flow <= 6000:
                            self.flow_rate = flow

            # Command B5 09: Live Run Data
            elif pb == 0xB5 and sb == 0x09:
                if len(m_data) >= 1 and len(s_data) >= 4:
                    ext_id = m_data[-1]
                    payload = s_data[3:]
                    # e.g. pump power or compressor rps
                    if ext_id == 0xC5 and len(payload) >= 1:  # RunDataBuildingCPumpPower
                        logger.debug(f"[eBUS HMU] Building Pump: {payload[0]}%")

        # Exchanges targeting VRC700 System Controller (0x15)
        elif zz == 0x15:
            logger.info(
                f"[Snoop VRC700 M-S] {qq:02x}->{zz:02x} cmd={pb:02x}{sb:02x} "
                f"m_data={m_data.hex(' ')} s_data={s_data.hex(' ')}"
            )
            # Command B5 24: VRC700 Circuit & System Data
            if pb == 0xB5 and sb == 0x24:
                # Format: m_data = [02, 00, sub_lsb, sub_msb, reg_lsb, reg_msb]
                # s_data = [ign0, ign1, ign2, ign3, float_b0, float_b1, float_b2, float_b3]
                if len(s_data) >= 8:
                    try:
                        val = struct.unpack('<f', s_data[4:8])[0]
                        # 4B 00 = SystemFlowTemp, 08 00 = Hc1FlowTemp
                        if len(m_data) >= 6 and (m_data[4:6] == b'\x4b\x00' or m_data[4:6] == b'\x08\x00'):
                            if 10.0 <= val <= 85.0:
                                self.circuit_flow_temp = round(val, 2)
                                logger.info(f"[eBUS VRC700] Circuit Flow Temperature: {self.circuit_flow_temp}°C")
                        # 07 00 = Hc1ActualFlowTempDesired
                        elif len(m_data) >= 6 and m_data[4:6] == b'\x07\x00':
                            if 10.0 <= val <= 65.0:
                                self.target_flow_temp = round(val, 1)
                                logger.info(f"[eBUS VRC700] Actual Desired Flow Temp: {self.target_flow_temp}°C")
                    except Exception as e:
                        logger.warning(f"Failed to decode VRC700 B5 24 payload: {e}")

        # Log any exchanges initiated by VRC700 master (0x10 or 0x15)
        elif qq in (0x10, 0x15):
            logger.info(
                f"[Snoop VRC700 Master] {qq:02x}->{zz:02x} cmd={pb:02x}{sb:02x} "
                f"m_data={m_data.hex(' ')} s_data={s_data.hex(' ')}"
            )

        self.publish_telemetry()

    def process_master_only(self, qq: int, zz: int, pb: int, sb: int, m_data: bytes):
        """Parse master broadcast or unacknowledged frame."""
        # Broadcast Datetime: pb=0x07 sb=0x00 (outsidetemp in D2B)
        if pb == 0x07 and sb == 0x00 and len(m_data) >= 2:
            raw_out = int.from_bytes(m_data[0:2], byteorder="little", signed=True) / 256.0
            if -40.0 <= raw_out <= 60.0:
                self.outdoor_temp = round(raw_out, 1)
                logger.info(f"[eBUS Broadcast] Outside Temperature: {self.outdoor_temp}°C")
        elif zz == 0x15 or qq in (0x10, 0x15):
            logger.info(f"[Snoop VRC700 Master Frame] {qq:02x}->{zz:02x} cmd={pb:02x}{sb:02x} data={m_data.hex(' ')}")
        elif zz == 0xFE:
            logger.debug(f"[eBUS Broadcast Master] {qq:02x}->{zz:02x} cmd={pb:02x}{sb:02x} data={m_data.hex(' ')}")

    def publish_telemetry(self):
        now = time.time()
        # Rate limit MQTT publications to at most once per 2 seconds
        if now - self.last_pub_time < 2.0 or not self.mqtt_client:
            return

        self.last_pub_time = now

        # Derive operating state from compressor power and DHW status
        if self.compressor_elec_power is not None and self.compressor_elec_power > 100:
            if self.dhw_mode == "on" or (self.state_name and "DHW" in self.state_name):
                self.state_code = "hwc_compressor_active"
                self.state_name = "DHW Heating Active"
            else:
                self.state_code = "heat_compressor_active"
                self.state_name = "Heating Active"
        elif self.compressor_elec_power is not None and self.compressor_elec_power > 25:
            self.state_code = "pump_overrun"
            self.state_name = "Pump Overrun / Thermal Scavenging"
        else:
            self.state_code = "standby"
            self.state_name = "Standby"

        # Dedicated Custom Entities
        if self.flow_temp is not None:
            self.mqtt_client.publish("ebus_passive/flow_temperature/state", str(self.flow_temp), retain=True)

        if self.dhw_temp is not None:
            self.mqtt_client.publish("ebus_passive/dhw_temp/state", str(self.dhw_temp), retain=True)

        if self.circuit_flow_temp is not None:
            self.mqtt_client.publish("ebus_passive/circuit_flow_temperature/state", str(self.circuit_flow_temp), retain=True)

        if self.pressure is not None:
            self.mqtt_client.publish("ebus_passive/pressure/state", str(self.pressure), retain=True)

        if self.outdoor_temp is not None:
            self.mqtt_client.publish("ebus_passive/outdoor_temperature/state", str(self.outdoor_temp), retain=True)

        self.mqtt_client.publish("ebus_passive/operation_state/state", self.state_name, retain=True)

        # Update HVAC Action for Home Assistant Thermostat
        self.update_hvac_action()

        # Target Flow Temperature (pure weather compensation curve with VRC700 heat pump base shift)
        if self.heating_mode == "off":
            self.target_flow_temp = 0.0
        elif self.outdoor_temp is not None and self.heating_day_target is not None:
            dt = self.heating_day_target - self.outdoor_temp
            if dt <= 0:
                self.target_flow_temp = HEATING_MIN_FLOW
            else:
                # VRC700 heat pump curve offset (+2.9°C parallel baseline shift)
                flow = self.heating_day_target + 2.9 + (self.heating_curve * 0.98 * (dt ** 1.075))
                self.target_flow_temp = max(HEATING_MIN_FLOW, min(HEATING_MAX_FLOW, round(flow, 1)))

        if self.target_flow_temp is not None:
            self.mqtt_client.publish("ebus_passive/target_flow_temp/state", str(self.target_flow_temp), retain=True)

        if self.compressor_elec_power is not None:
            self.mqtt_client.publish("ebus_passive/compressor_power/state", str(round(self.compressor_elec_power, 1)), retain=True)

        if self.cumulative_energy_kwh is not None:
            self.mqtt_client.publish("ebus_passive/cumulative_energy/state", str(round(self.cumulative_energy_kwh, 2)), retain=True)

        # Log periodic summary every 60s
        if now - self.last_summary_time >= 60.0:
            self.last_summary_time = now
            circ_str = f" | Circuit: {self.circuit_flow_temp}°C" if self.circuit_flow_temp is not None else ""
            logger.info(
                f"[eBUS Live] Flow: {self.flow_temp}°C{circ_str} | Pressure: {self.pressure} bar | "
                f"Outdoor: {self.outdoor_temp}°C | Comp: {self.compressor_elec_power} W | State: {self.state_name}"
            )

    def update_hvac_action(self):
        """Update and publish current hvac_action for Home Assistant climate card."""
        if not self.mqtt_client:
            return
        if self.heating_mode == "off":
            action = "off"
        elif self.compressor_elec_power is not None and self.compressor_elec_power > 100:
            if self.dhw_mode == "on" or (self.state_name and "DHW" in self.state_name):
                action = "idle"  # DHW cylinder heating active
            else:
                action = "heating"
        else:
            action = "idle"
        self.mqtt_client.publish("ebus_passive/heating_action/state", action, retain=True)

    def decode_enhanced_stream(self, data: bytes) -> tuple[bytes, int]:
        """
        Decodes eBUS enhanced protocol (ens) framing into raw eBUS byte stream.
        Handles ENH_RES_RECEIVED, ENH_RES_STARTED, ENH_RES_FAILED sequences.
        """
        out = bytearray()
        i = 0
        n = len(data)
        consumed = 0
        while i < n:
            b = data[i]
            # Normal raw byte (bit 7 clear)
            if not (b & 0x80):
                out.append(b)
                i += 1
                consumed = i
                continue

            kind = b & 0xC0
            if kind == 0xC0:  # ENH_BYTE1 (0xC0)
                if i + 1 >= n:
                    # Incomplete 2-byte sequence; leave in buffer
                    break
                b2 = data[i + 1]
                if (b2 & 0xC0) == 0x80:  # ENH_BYTE2 (0x80)
                    cmd = (b >> 2) & 0x0F
                    sym = ((b & 0x03) << 6) | (b2 & 0x3F)
                    # 0x01 = ENH_RES_RECEIVED, 0x02 = ENH_RES_STARTED, 0x0A = ENH_RES_FAILED
                    if cmd in (0x01, 0x02, 0x0A):
                        out.append(sym)
                    i += 2
                    consumed = i
                    continue
                else:
                    # Stray/malformed byte 2
                    i += 1
                    consumed = i
                    continue
            else:
                # Stray byte
                i += 1
                consumed = i
                continue

        return bytes(out), consumed

    def run(self):
        self.setup_mqtt()
        self.connect_adapter()

        while self.running:
            try:
                chunk = self.sock.recv(1024)
                if not chunk:
                    logger.warning("Empty read from C6 adapter, reconnecting...")
                    self.sock.close()
                    self.connect_adapter()
                    continue

                if EBUS_DEBUG_DUMP:
                    with open("/tmp/ebus_raw_stream.bin", "ab") as f_raw:
                        f_raw.write(chunk)

                self.raw_buffer.extend(chunk)
                decoded, consumed = self.decode_enhanced_stream(self.raw_buffer)
                self.raw_buffer = self.raw_buffer[consumed:]
                self.decoded_buffer.extend(decoded)

                # Split decoded buffer by SYN byte 0xAA
                while b'\xaa' in self.decoded_buffer:
                    idx = self.decoded_buffer.index(b'\xaa')
                    segment = bytes(self.decoded_buffer[:idx])
                    self.decoded_buffer = self.decoded_buffer[idx+1:]

                    if not segment:
                        continue

                    if EBUS_DEBUG_DUMP:
                        with open("/tmp/ebus_segments.log", "a") as f:
                            f.write(f"{time.strftime('%H:%M:%S')} [{len(segment)}b] {segment.hex(' ')}\n")

                    # Deterministic eBUS frame parsing based on header length fields
                    i = 0
                    while i < len(segment):
                        # Case 1: Master 00 Broadcast: [00] [NN] [Data x NN] [CRC]
                        if segment[i] == 0x00 and i + 2 <= len(segment):
                            nn = segment[i + 1]
                            candidate_len = nn + 3
                            if i + candidate_len <= len(segment):
                                if calc_crc(segment[i : i + candidate_len - 1]) == segment[i + candidate_len - 1]:
                                    sub = segment[i : i + candidate_len]
                                    self.process_broadcast_frame(sub)
                                    i += candidate_len
                                    continue

                        # Case 2: Master Frame with optional Slave response
                        # [QQ] [ZZ] [PB] [SB] [NN_m] [Data_m x NN_m] [CRC_m] [0x00] [NN_s] [Data_s x NN_s] [CRC_s] [0x00]
                        if i + 6 <= len(segment):
                            nn_m = segment[i + 4]
                            if nn_m <= 24:
                                candidate_m_len = nn_m + 6
                                if i + candidate_m_len <= len(segment):
                                    m_crc = segment[i + candidate_m_len - 1]
                                    if calc_crc(segment[i : i + candidate_m_len - 1]) == m_crc:
                                        qq = segment[i]
                                        zz = segment[i + 1]
                                        pb = segment[i + 2]
                                        sb = segment[i + 3]
                                        m_data = segment[i + 5 : i + candidate_m_len - 1]

                                        # Check for Slave ACK (0x00) and response
                                        s_ack_idx = i + candidate_m_len
                                        if s_ack_idx < len(segment) and segment[s_ack_idx] == 0x00:
                                            if s_ack_idx + 2 <= len(segment):
                                                nn_s = segment[s_ack_idx + 1]
                                                candidate_s_len = nn_s + 2  # NN_s + data + CRC_s
                                                s_end = s_ack_idx + 1 + candidate_s_len
                                                if s_end <= len(segment):
                                                    s_crc = segment[s_end - 1]
                                                    if calc_crc(segment[s_ack_idx + 1 : s_end - 1]) == s_crc:
                                                        s_data = segment[s_ack_idx + 2 : s_end - 1]
                                                        self.process_master_slave(qq, zz, pb, sb, m_data, s_data)

                                                        # Advance past slave response and optional master ACK (0x00)
                                                        if s_end < len(segment) and segment[s_end] == 0x00:
                                                            i = s_end + 1
                                                        else:
                                                            i = s_end
                                                        continue

                                        # Master only or unacknowledged frame
                                        self.process_master_only(qq, zz, pb, sb, m_data)
                                        i += candidate_m_len
                                        continue

                        i += 1

            except socket.timeout:
                continue
            except Exception as e:
                logger.error(f"Error reading socket: {e}. Reconnecting...")
                if self.sock:
                    try:
                        self.sock.close()
                    except Exception:
                        pass
                self.connect_adapter()

    def stop(self):
        logger.info("Stopping ebus_passive_bridge...")
        self.running = False
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass
        if self.mqtt_client:
            self.mqtt_client.publish("ebus_passive/bridge_connected/state", "OFF", retain=True)
            self.mqtt_client.publish("ebus_passive/availability", "offline", retain=True)
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()

if __name__ == "__main__":
    bridge = EbusPassiveBridge()

    def handle_sig(sig, frame):
        bridge.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    bridge.run()
