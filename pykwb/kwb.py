# -*- coding: utf-8 -*-
"""
The MIT License (MIT)

Copyright (c) 2017 Markus Peter mpeter at emdev dot de

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.


Support for KWB Easyfire central heating units.
"""

import asyncio
import struct
import logging
import socket
import select
import errno
import sys
import time
import threading
import argparse
import serial

if __name__ == "__main__" and not __package__:
    # Direct script execution puts pykwb/, not its parent, on sys.path.
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pykwb.decode import decode_pairs, decode_temperature
from pykwb.messages import load_messages

PROP_LOGLEVEL_TRACE = 5
PROP_LOGLEVEL_DEBUG = 4
PROP_LOGLEVEL_INFO = 3
PROP_LOGLEVEL_WARN = 2
PROP_LOGLEVEL_ERROR = 1
PROP_LOGLEVEL_NONE = 0

PROP_MODE_SERIAL = 0
PROP_MODE_TCP = 1
PROP_MODE_FILE = 2
# Connectionless input. The serial server is configured as a UDP client and
# pushes datagrams at us unprompted; we only bind and read. There is no
# session to establish, lose or reconnect, which makes this immune to the
# stale-session lockups some serial servers hit when a TCP client restarts.
PROP_MODE_UDP = 3
# We listen and the serial server dials in. Its documented client-mode
# recovery then applies: on losing us it retries, and after the configured
# number of attempts reboots itself, so it comes back without being touched.
PROP_MODE_TCP_SERVER = 4

STATUS_WAITING = 0
STATUS_PRE_1 = 1
STATUS_SENSE_PRE_2 = 2
STATUS_SENSE_PRE_3 = 3
STATUS_SENSE_PRE_LENGTH = 6
STATUS_SENSE_DATA = 8
STATUS_SENSE_CHECKSUM = 9
STATUS_CTRL_PRE_2 = 10
STATUS_CTRL_PRE_3 = 11
STATUS_CTRL_DATA = 12
STATUS_CTRL_CHECKSUM = 19
STATUS_PACKET_DONE = 255

PROP_PACKET_SENSE = 32
PROP_PACKET_CTRL = 33
PROP_PACKET_SENSE_64 = 64
# An Easyfire 1 driving a Comfort 3 controller reports these message IDs
# instead of 32/33. messages.csv already defines them; they were simply never
# wired up, so every frame such a boiler sends was parsed and then discarded.
PROP_PACKET_SENSE_16 = 16
PROP_PACKET_CTRL_17 = 17

PROP_SENSE_MESSAGE_IDS = (PROP_PACKET_SENSE_16, PROP_PACKET_SENSE,
                          PROP_PACKET_SENSE_64)
PROP_CTRL_MESSAGE_IDS = (PROP_PACKET_CTRL_17, PROP_PACKET_CTRL)

# messages.csv describes several mutually exclusive boiler generations, told
# apart by its "source" column: 16/17 are documented by sources 2, 4 and 8,
# 32/33/64 by source 10, 48/49 by source 6. Their field layouts conflict, so
# only one generation may be loaded at a time. connection-independent config
# key "source" selects it; source 2 is an Easyfire 1 with a Comfort 3.
#
# Leaving it unset keeps the historical behaviour of loading messages
# 32/33/64 regardless of source, so existing callers are unaffected.

PROP_SENSOR_TEMPERATURE = 0
PROP_SENSOR_FLAG = 1
PROP_SENSOR_RAW = 2
PROP_SENSOR_NUMBER = 3
PROP_SENSOR_PRESSURE = 4
PROP_SENSOR_DURATION = 5
PROP_SENSOR_SPEED = 6

TCP_IP = "127.0.0.1"
TCP_PORT = 23

SERIAL_INTERFACE = "/dev/ttyUSB0"
SERIAL_SPEED = 19200

_LOGGER = logging.getLogger(__name__)
_LOGGING_LEVELS = {
    PROP_LOGLEVEL_TRACE: logging.DEBUG,
    PROP_LOGLEVEL_DEBUG: logging.DEBUG,
    PROP_LOGLEVEL_INFO: logging.INFO,
    PROP_LOGLEVEL_WARN: logging.WARNING,
    PROP_LOGLEVEL_ERROR: logging.ERROR,
}


class _ListenerStopped(Exception):
    """Internal signal for interrupting synchronous connection/read waits."""


class KWBEasyfireSensor:
    """This Class represents as single sensor."""

    def __init__(self, _packet, _index, _name, _sensor_type, _bit=None,
                 _length=2, _signed=True, _scale=0.1, _units="", _key=""):

        self._packet = _packet
        self._index = _index
        self._bit = _bit
        self._name = _name
        self._sensor_type = _sensor_type
        self._value = None
        self._available = False
        self._length = _length
        self._signed = _signed
        self._scale = _scale
        self._units = _units
        self._key = _key

    @classmethod
    def from_message(cls, message):
        """Create a sensor from one packet definition in messages.csv."""
        if message['type'] == 'bit':
            sensor_type = PROP_SENSOR_FLAG
        elif message['type'] == 'int':
            sensor_type = {
                'C': PROP_SENSOR_TEMPERATURE,
                'mbar': PROP_SENSOR_PRESSURE,
                'ms': PROP_SENSOR_DURATION,
                'msec': PROP_SENSOR_DURATION,
                'sec': PROP_SENSOR_DURATION,
                'rpm': PROP_SENSOR_SPEED,
            }.get(message['units'], PROP_SENSOR_NUMBER)
        else:
            raise ValueError("Unsupported sensor type: " + message['type'])
        return cls(
            int(message['message_id']), int(message['offset']),
            message['name_en'] or message['name_de'] or message['key'],
            sensor_type,
            _bit=int(message['bit']) if message['bit'] else None,
            _length=int(message['length'] or 1),
            _signed=message['signed'] == '1',
            _scale=float(message['scale'] or 1),
            _units=message['units'], _key=message['key'],
        )

    @property
    def key(self):
        """Return the optional CSV key (not necessarily unique)."""
        return self._key

    def decode(self, packet):
        """Update from an unescaped, big-endian payload."""
        if self.sensor_type == PROP_SENSOR_RAW:
            self.value = packet
            return
        offset = self.index
        length = 1 if self.sensor_type == PROP_SENSOR_FLAG else self._length
        if offset is None or offset < 0 or offset + length > len(packet):
            self.value = None
        elif self.sensor_type == PROP_SENSOR_FLAG:
            self.value = ((packet[offset] >> self.bit) & 1
                          if self.bit is not None and 0 <= self.bit < 8 else None)
        else:
            value = int.from_bytes(packet[offset:offset + length], 'big',
                                   signed=self._signed)
            if self.sensor_type == PROP_SENSOR_TEMPERATURE and value == 1300:
                self.value = None
            else:
                self.value = round(value * self._scale, 10)

    @property
    def index(self):
        """Return the unescaped payload byte offset, or None if unmapped."""
        return self._index

    @property
    def bit(self):
        """Return the bit position within the payload byte for flags."""
        return self._bit

    @property
    def name(self):
        """Returns the name of the sensor."""
        return self._name

    @property
    def sensor_type(self):
        """Return the sensor's measurement or data type."""
        return self._sensor_type

    @property
    def unit_of_measurement(self):
        """Return the CSV unit, displaying Celsius as °C."""
        if (self._sensor_type == PROP_SENSOR_TEMPERATURE):
            return "°C"
        else:
            return self._units

    @property
    def value(self):
        """Returns the value of the sensor. Unit is unit_of_measurement."""
        return self._value

    @value.setter
    def value(self, _value):
        """Sets the value of the sensor. Unit is unit_of_measurement."""
        self._available = _value is not None
        self._value = _value

    @property
    def available(self):
        """Return if sensor is available."""
        return self._available

    def __str__(self):
        """Returns an informational text representation of the sensor."""
        return self.name + ": I: " + str(self.index) + " T: " + str(self.sensor_type) + "(" + str(self.unit_of_measurement) + ") V: " + str(self.value)


# pylint: disable=too-many-instance-attributes
class KWBEasyfire:
    """Communicats with the KWB Easyfire unit."""

    def __init__(self, _mode, _ip="", _port=0, _serial_device="", _serial_speed=19200,
                 _file_path="", _config=None):
        """Initialize the Object."""

        self._config = dict(_config or {})
        self._config['connection'] = {
            'reconnect': False,
            'connect_timeout': 5,
            'stale_timeout': 30,
            'retry_initial': 1,
            'retry_max': 30,
            **self._config.get('connection', {}),
        }
        self._debug_level = PROP_LOGLEVEL_INFO
        self._run_thread = True
        self._packet_parser = None
        self._socket = None
        self._udp_buffer = b""
        self._udp_offset = 0
        self._udp_source = None
        self._tcp_peer = None
        self._listener = None
        self._stop_event = threading.Event()
        self._retry_delay = self._config['connection']['retry_initial']
        self._last_valid_packet = time.monotonic()
        for key in ('connect_timeout', 'stale_timeout', 'retry_initial', 'retry_max'):
            if not 0 < self._config['connection'][key] < float('inf'):
                raise ValueError("connection.%s must be finite and positive" % key)

        self._mode = _mode
        self._ip = _ip
        self._port = _port
        self._serial_device = _serial_device
        self._serial_speed = _serial_speed
        self._file_path = _file_path
        self._logdatalen = 1024
        self._logdata = []

        # Only one boiler generation's definitions may be active at a time,
        # since their field layouts conflict. Without an explicit source this
        # loads messages 32/33/64 exactly as before.
        source = self._config.get('source')
        legacy_ids = (PROP_PACKET_SENSE, PROP_PACKET_CTRL, PROP_PACKET_SENSE_64)
        known_ids = PROP_SENSE_MESSAGE_IDS + PROP_CTRL_MESSAGE_IDS
        if source is None:
            messages = [m for m in load_messages()
                        if int(m['message_id']) in legacy_ids]
            active_ids = list(legacy_ids)
        else:
            # A row may list several sources, comma separated, when the same
            # definition applies to more than one document.
            wanted = str(source)
            messages = [m for m in load_messages()
                        if wanted in [part.strip()
                                      for part in m['source'].split(',')]
                        and int(m['message_id']) in known_ids]
            active_ids = sorted({int(m['message_id']) for m in messages})

        raw_names = {
            PROP_PACKET_SENSE_16: "RAW SENSE",
            PROP_PACKET_CTRL_17: "RAW CTRL",
            PROP_PACKET_SENSE: "RAW SENSE",
            PROP_PACKET_CTRL: "RAW CTRL",
            PROP_PACKET_SENSE_64: "RAW SENSE 64",
        }
        self._sensors = {
            message_id: [KWBEasyfireSensor(message_id, 0, raw_names[message_id],
                                           PROP_SENSOR_RAW)]
            for message_id in active_ids
        }
        for message in messages:
            self._sensors[int(message['message_id'])].append(
                KWBEasyfireSensor.from_message(message))

        self._thread = threading.Thread(target=self.run, daemon=True)

        try:
            self._open_connection()
        except OSError as error:
            if not self._reconnect_enabled():
                raise
            self._connection_lost(error)

    def _debug(self, level, text):
        """Log text through the module logger so the host application can filter it."""
        if (level <= self._debug_level):
            _LOGGER.log(_LOGGING_LEVELS[level], text)

    def __del__(self):
        """Destruct the object."""
        self._close_connection()

    def _open_connection(self):
        """Open a connection to the easyfire unit."""
        if (self._mode == PROP_MODE_SERIAL):
            self._serial = serial.Serial(self._serial_device, self._serial_speed)
        elif (self._mode == PROP_MODE_TCP):
            self._connect_tcp()
        elif (self._mode == PROP_MODE_UDP):
            self._bind_udp()
        elif (self._mode == PROP_MODE_TCP_SERVER):
            self._listen_tcp()
        elif (self._mode == PROP_MODE_FILE):
            self._file = open(self._file_path, "r")

    def _close_connection(self):
        """Close resources, including after a failed constructor/connect."""
        for name in ('_socket', '_listener', '_serial', '_file'):
            resource = getattr(self, name, None)
            if resource is not None:
                resource.close()
        self._socket = None
        self._listener = None

    def _reconnect_enabled(self):
        return self._mode == PROP_MODE_TCP and self._config['connection']['reconnect']

    def _connection_lost(self, error):
        self._debug(PROP_LOGLEVEL_WARN, "TCP disconnected: %s" % error)
        self._close_connection()
        self._packet_parser = None
        for sensor in self.get_sensors():
            sensor.value = None

    def _next_retry_delay(self):
        settings = self._config['connection']
        delay = min(self._retry_delay, settings['retry_max'])
        self._retry_delay = min(delay * 2, settings['retry_max'])
        self._debug(PROP_LOGLEVEL_INFO, "TCP reconnect in %g seconds" % delay)
        return delay

    def _listen_tcp(self):
        """Listen for the serial server to connect to us.

        self._port is the LOCAL port to listen on. self._ip, when set, is the
        only address a connection is accepted from. One peer at a time: the
        backlog holds a reconnecting server until the stale session is gone.
        """
        self._tcp_peer = (
            socket.gethostbyname(self._ip) if self._ip else None)
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            # Safe on a listening socket, and lets us rebind immediately
            # after a restart instead of waiting out TIME_WAIT.
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(("", self._port))
            listener.listen(1)
        except BaseException:
            listener.close()
            raise
        self._listener = listener
        self._last_valid_packet = time.monotonic()

    def _drop_peer(self):
        """Forget the current peer so a fresh connection can be accepted."""
        if self._socket is not None:
            self._socket.close()
            self._socket = None
        # A half-received frame cannot be continued by a different session.
        self._packet_parser = None
        for sensor in self.get_sensors():
            sensor.value = None

    def _bind_udp(self):
        """Bind the local port the serial server sends its datagrams to.

        self._port is the LOCAL port to listen on, not a remote one. self._ip,
        when set, is the address datagrams must come from; anything else is
        dropped. Leave it empty to accept from any source.
        """
        # Datagrams report a numeric source address, so a configured hostname
        # has to be resolved here or the comparison would reject everything.
        self._udp_source = (
            socket.gethostbyname(self._ip) if self._ip else None)
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sock.bind(("", self._port))
        except BaseException:
            sock.close()
            raise
        self._socket = sock
        self._udp_buffer = b""
        self._udp_offset = 0
        self._last_valid_packet = time.monotonic()

    def _connect_tcp(self):
        """Connect with a deadline and interruptible readiness waits."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.setblocking(False)
            result = sock.connect_ex((self._ip, self._port))
            pending = (errno.EINPROGRESS, errno.EWOULDBLOCK, errno.EALREADY, errno.EINTR)
            if result != 0 and result not in pending:
                raise OSError(result, "TCP connect failed")
            deadline = time.monotonic() + self._config['connection']['connect_timeout']
            while result != 0:
                if self._stop_event.is_set():
                    raise _ListenerStopped()
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("TCP connect timed out")
                _, ready, failed = select.select([], [sock], [sock], min(0.1, remaining))
                if ready or failed:
                    result = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
                    if result:
                        raise OSError(result, "TCP connect failed")
                    break
            sock.setblocking(True)
        except BaseException:
            sock.close()
            raise
        self._socket = sock
        self._last_valid_packet = time.monotonic()

    async def _connect_tcp_async(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setblocking(False)
        try:
            await asyncio.wait_for(
                asyncio.get_running_loop().sock_connect(sock, (self._ip, self._port)),
                timeout=self._config['connection']['connect_timeout'])
        except BaseException:
            sock.close()
            raise
        self._socket = sock
        self._last_valid_packet = time.monotonic()

    def _stale_remaining(self):
        remaining = (self._config['connection']['stale_timeout']
                     - (time.monotonic() - self._last_valid_packet))
        if remaining <= 0:
            raise TimeoutError("No valid TCP packet within stale_timeout")
        return remaining

    def _read_tcp_byte(self):
        sock = self._socket
        timeout = sock.gettimeout()
        try:
            while True:
                if self._stop_event.is_set():
                    raise _ListenerStopped()
                remaining = self._stale_remaining() if self._reconnect_enabled() else 0.1
                sock.settimeout(min(0.1, remaining))
                try:
                    return sock.recv(1)
                except socket.timeout:
                    continue
        finally:
            sock.settimeout(timeout)

    @staticmethod
    def _byte_rot_left(byte, distance):
        """Rotate a byte left by distance bits."""
        return ((byte << distance) | (byte >> (8 - distance))) % 256

    def _add_to_checksum(self, checksum, value):
        """Add a byte to the checksum."""
        checksum = self._byte_rot_left(checksum, 1)
        checksum = checksum + value
        if (checksum > 255):
            checksum = checksum - 255
        self._debug(PROP_LOGLEVEL_TRACE, "C: " + str(checksum) + " V: " + str(value))
        return checksum

    def _read_byte(self):
        """Read a byte from input."""

        to_return = ""
        if (self._mode == PROP_MODE_SERIAL):
            to_return = self._serial.read(1)
        elif (self._mode == PROP_MODE_TCP):
            to_return = self._read_tcp_byte()
        elif (self._mode == PROP_MODE_FILE):
            read = self._file.readline()
            if (read == ''):
                raise EOFError("EOF")
            to_return = struct.pack("B", int(read))

        if not to_return:
            raise EOFError("Input connection closed")

        self._record_byte(ord(to_return))
        return to_return

    def _record_byte(self, value):
        """Keep identical diagnostics for synchronous and async input."""
        _LOGGER.debug("READ: %s", value)
        self._logdata.append(value)
        if len(self._logdata) > self._logdatalen:
            self._logdata = self._logdata[-self._logdatalen:]
        self._debug(PROP_LOGLEVEL_TRACE, "READ: " + str(value))

    def _read_ord_byte(self):
        """Read a byte as number from the input."""
        if self._mode in (PROP_MODE_UDP, PROP_MODE_TCP_SERVER):
            raise ValueError(
                "UDP and TCP server input are only supported by the asyncio reader; "
                "use listen_forever()/listen_for() rather than run_thread()")
        return ord(self._read_byte())

    @staticmethod
    def _sense_packet_to_data(packet):
        """Remove the escape pad bytes from a sense packet (\2\0 -> \2)."""
        data = bytearray(0)
        last = 0
        i = 0
        while (i < len(packet)):
            if not (last == 2 and packet[i] == 0):
                data.append(packet[i])
            last = packet[i]
            i += 1

        return data

    @staticmethod
    def _decode_temp(byte_1, byte_2):
        """Decode a signed short temperature as two bytes to a single number."""
        return decode_temperature(byte_1, byte_2)

    def _read_packet(self):
        """Read a checksum-valid frame and return its unescaped payload."""
        while True:
            packet = self._consume_byte(self._read_ord_byte())
            if packet is not None:
                return packet

    def _consume_byte(self, value):
        """Retain partial framing state across reads and listening sessions."""
        if self._packet_parser is None:
            self._packet_parser = self._parse_packet()
            next(self._packet_parser)
        try:
            self._packet_parser.send(value)
        except StopIteration as complete:
            self._packet_parser = None
            self._last_valid_packet = time.monotonic()
            self._retry_delay = self._config['connection']['retry_initial']
            return complete.value
        return None

    def _parse_packet(self):
        """Accept bytes via send(), returning one valid, unescaped frame."""
        pending_length = None
        while True:
            if pending_length is None:
                if (yield) != 2:
                    continue
                length = (yield)
            else:
                length = pending_length
                pending_length = None

            if length == 0:
                continue
            mode = PROP_PACKET_CTRL
            while length == 2:
                mode = PROP_PACKET_SENSE
                length = (yield)
            if length < 5:
                continue

            version = (yield)
            counter = (yield)
            checksum = 2
            for value in (length, version, counter):
                checksum = self._add_to_checksum(checksum, value)

            # Length includes the four header bytes and the checksum, but
            # excludes the extra sense header and payload escape padding.
            packet = bytearray()
            valid = True
            for _ in range(length - 5):
                value = (yield)
                packet.append(value)
                checksum = self._add_to_checksum(checksum, value)
                if value == 2:
                    padding = (yield)
                    if padding != 0:
                        # An unescaped 2 starts a new frame. Reuse its next
                        # byte as the length (or extra sense header), rather
                        # than discarding the beginning of that frame.
                        pending_length = padding
                        valid = False
                        break
            if not valid:
                continue
            if (yield) != checksum:
                continue

            packet_type = "SENSE" if mode == PROP_PACKET_SENSE else "CTRL"
            summary = "\n\nPacket ID %d %s counter=%d length=%d" % (
                version, packet_type, counter, len(packet))
            if self._debug_level >= PROP_LOGLEVEL_DEBUG:
                summary += " payload=" + packet.hex(" ")
            self._debug(PROP_LOGLEVEL_INFO, summary)
            return (mode, version, packet)

    def _decode_sense_packet(self, version, packet):
        """Decode boiler temperatures using the message ID's payload layout."""
        if version not in PROP_SENSE_MESSAGE_IDS or version not in self._sensors:
            return
        for sensor in self._sensors[version]:
            sensor.decode(packet)

        for sensor in self._sensors[version]:
            level = (PROP_LOGLEVEL_DEBUG if sensor.sensor_type == PROP_SENSOR_RAW
                     else PROP_LOGLEVEL_INFO)
            self._debug(level, str(sensor))

    def _decode_ctrl_packet(self, version, packet):
        """Decode a control packet into the list of sensors."""
        if version not in PROP_CTRL_MESSAGE_IDS or version not in self._sensors:
            return

        for i in range(min(5, len(packet))):
            input_bit = packet[i]
            self._debug(PROP_LOGLEVEL_DEBUG, "Byte " + str(i) + ": " + str((input_bit >> 7) & 1) + str((input_bit >> 6) & 1) + str((input_bit >> 5) & 1) + str((input_bit >> 4) & 1) + str((input_bit >> 3) & 1) + str((input_bit >> 2) & 1) + str((input_bit >> 1) & 1) + str(input_bit & 1))

        for sensor in self._sensors[version]:
            sensor.decode(packet)

        if version == 33:
            self._debug(PROP_LOGLEVEL_INFO, "ID 33 control values:\n" +
                        "\n".join(str(sensor) for sensor in self._sensors[version]))

    def get_sensors(self):
        """Return the list of sensors."""
        return [sensor for sensors in self._sensors.values() for sensor in sensors]

    def __str__(self):
        """Returns an informational text representation of the object."""
        ret = ""

        for sensor in self.get_sensors():
            ret = ret + str(sensor) + "\n"

        return ret

    def _decode_packet(self, mode, version, packet):
        """Decode only configured message IDs with matching frame types."""
        if (mode == PROP_PACKET_SENSE and version in PROP_SENSE_MESSAGE_IDS
                and version in self._sensors):
            self._decode_sense_packet(version, packet)
        elif (mode == PROP_PACKET_CTRL and version in PROP_CTRL_MESSAGE_IDS
                and version in self._sensors):
            self._decode_ctrl_packet(version, packet)
        if version in self._config.get('decode', []):
            for line in decode_pairs(version, packet):
                self._debug(PROP_LOGLEVEL_INFO, line)

    def run(self):
        """Read until stopped, retrying TCP failures when configured."""
        try:
            while self._run_thread:
                try:
                    if self._reconnect_enabled() and self._socket is None:
                        if self._stop_event.wait(self._next_retry_delay()):
                            return
                        self._connect_tcp()
                        self._debug(PROP_LOGLEVEL_INFO, "TCP reconnected")
                    packet = self._read_packet()
                except (EOFError, OSError) as error:
                    if self._reconnect_enabled():
                        self._connection_lost(error)
                        continue
                    if isinstance(error, EOFError):
                        return
                    raise
                self._decode_packet(*packet)
        except _ListenerStopped:
            pass
        finally:
            self._run_thread = False

    def run_thread(self):
        """Start the background listener."""
        if self._mode == PROP_MODE_UDP:
            raise ValueError(
                "UDP input is only supported by the asyncio reader; "
                "use listen_forever()/listen_for() rather than run_thread()")
        self._run_thread = True
        self._stop_event.clear()
        self._thread.start()

    async def _read_async_byte(self):
        if self._mode == PROP_MODE_TCP:
            if self._reconnect_enabled():
                remaining = self._stale_remaining()
                data = await asyncio.wait_for(
                    asyncio.get_running_loop().sock_recv(self._socket, 1), remaining)
            else:
                data = await asyncio.get_running_loop().sock_recv(self._socket, 1)
            if not data:
                raise EOFError("Input connection closed")
        elif self._mode == PROP_MODE_TCP_SERVER:
            loop = asyncio.get_running_loop()
            while True:
                if self._socket is None:
                    conn, peer = await loop.sock_accept(self._listener)
                    if self._tcp_peer and peer[0] != self._tcp_peer:
                        conn.close()
                        await asyncio.sleep(0)
                        continue
                    conn.setblocking(False)
                    self._socket = conn
                    self._last_valid_packet = time.monotonic()
                    self._debug(PROP_LOGLEVEL_INFO,
                                "accepted connection from %s" % peer[0])
                try:
                    data = await asyncio.wait_for(
                        loop.sock_recv(self._socket, 1), self._stale_remaining())
                except (TimeoutError, asyncio.TimeoutError, OSError) as error:
                    # A peer that connects and then says nothing is the
                    # failure this mode exists to survive: drop it and wait
                    # for the server to dial in again.
                    self._debug(PROP_LOGLEVEL_WARN,
                                "dropping silent or failed peer: %s" % error)
                    self._drop_peer()
                    await asyncio.sleep(0)
                    continue
                if not data:
                    self._debug(PROP_LOGLEVEL_INFO, "peer disconnected")
                    self._drop_peer()
                    await asyncio.sleep(0)
                    continue
                break
        elif self._mode == PROP_MODE_UDP:
            loop = asyncio.get_running_loop()
            while self._udp_offset >= len(self._udp_buffer):
                datagram, source = await loop.sock_recvfrom(self._socket, 65535)
                # UDP has no EOF: an empty datagram is legal and just means
                # "nothing here", so keep waiting rather than raising. Both
                # discard paths yield, or a flood of empty or wrong-source
                # datagrams could starve cancellation and listen_for deadlines.
                if not datagram or (self._udp_source
                                    and source[0] != self._udp_source):
                    await asyncio.sleep(0)
                    continue
                self._udp_buffer = datagram
                self._udp_offset = 0
            data = self._udp_buffer[self._udp_offset:self._udp_offset + 1]
            self._udp_offset += 1
        elif self._mode == PROP_MODE_SERIAL:
            # timeout=0 makes serial reads nonblocking on all platforms.
            while True:
                data = self._serial.read(1)
                if data:
                    break
                await asyncio.sleep(0.01)
        elif self._mode == PROP_MODE_FILE:
            return self._read_ord_byte()
        else:
            raise ValueError("Unsupported input mode")
        value = data[0]
        self._record_byte(value)
        return value

    async def listen_forever(self):
        """Update sensors until EOF or task cancellation, without a worker thread.

        Partial packets survive cancellation. Connection construction remains
        synchronous; TCP/serial blocking settings are restored on exit.
        """
        if self._mode == PROP_MODE_TCP_SERVER:
            timeout = None
            if self._listener is not None:
                self._listener.setblocking(False)
        elif self._mode in (PROP_MODE_TCP, PROP_MODE_UDP):
            timeout = self._socket.gettimeout() if self._socket is not None else None
            if self._socket is not None:
                self._socket.setblocking(False)
        elif self._mode == PROP_MODE_SERIAL:
            timeout = self._serial.timeout
            self._serial.timeout = 0
        try:
            while True:
                # Yield even when input is buffered so deadlines and
                # cancellation work under continuous traffic.
                await asyncio.sleep(0)
                try:
                    if self._reconnect_enabled() and self._socket is None:
                        await asyncio.sleep(self._next_retry_delay())
                        await self._connect_tcp_async()
                        self._debug(PROP_LOGLEVEL_INFO, "TCP reconnected")
                    value = await self._read_async_byte()
                except (EOFError, OSError) as error:
                    if self._reconnect_enabled():
                        self._connection_lost(error)
                        continue
                    if isinstance(error, EOFError):
                        return
                    raise
                packet = self._consume_byte(value)
                if packet is not None:
                    self._decode_packet(*packet)
        finally:
            if self._mode == PROP_MODE_TCP_SERVER:
                pass
            elif self._mode in (PROP_MODE_TCP, PROP_MODE_UDP) and self._socket is not None:
                self._socket.settimeout(timeout)
            elif self._mode == PROP_MODE_SERIAL:
                self._serial.timeout = timeout

    async def listen_for(self, seconds=1):
        """Update sensors for at most seconds, or until EOF; preserve partial input."""
        if not 0 <= seconds < float('inf'):
            raise ValueError("seconds must be finite and non-negative")
        if seconds == 0:
            return
        try:
            await asyncio.wait_for(self.listen_forever(), timeout=seconds)
        except asyncio.TimeoutError:
            pass

    def stop_thread(self):
        """Stop the main thread."""
        self._run_thread = False
        self._stop_event.set()

    def is_alive(self):
        """Determine if thread is alive."""
        return self._thread.is_alive()


def _print_summary(kwb):
    """Print sensor values in alphabetical order."""
    print("\n\n---\nSUMMARY: " + time.strftime("%Y-%m-%d %H:%M:%S %Z"))
    for sensor in sorted(kwb.get_sensors(), key=lambda sensor: sensor.name.casefold()):
        if sensor.sensor_type != PROP_SENSOR_RAW:
            print(sensor)


async def _listen_with_summaries(kwb, seconds, summary):
    """Keep listening while reporting periodically, until EOF or cancellation."""
    listener = asyncio.create_task(kwb.listen_forever())
    try:
        while True:
            done, _ = await asyncio.wait({listener}, timeout=seconds)
            if done:
                listener.result()
            if summary:
                _print_summary(kwb)
            if done:
                break
    finally:
        listener.cancel()
        try:
            await listener
        except asyncio.CancelledError:
            pass


def main():
    """Main method for debug purposes."""
    parser = argparse.ArgumentParser()
    group_execution = parser.add_argument_group('Execution')
    group_execution.add_argument('--mode', dest='execution_mode', choices=('thread', 'async'),
                                 default='thread', help="Execution mode (default: thread)")
    group_execution.add_argument('--wait', type=float, default=5,
                                 help="Seconds to listen, or summary interval with --forever (default: 5)")
    group_execution.add_argument('--forever', action='store_true', default=False,
                                 help="Listen continuously, printing summaries every --wait seconds")
    group_execution.add_argument('--decode', nargs='*', type=int, default=[], metavar='ID',
                                 help="Also decode two-byte values from offsets 3 and 4 for these message IDs (0-255)")
    group_execution.add_argument('--source', dest='source', default=None,
                                 help="messages.csv signal-map source to load "
                                      "(e.g. 2 for an Easyfire 1 / Comfort 3). "
                                      "Omit to load messages 32/33/64 as before")
    group_tcp = parser.add_argument_group('TCP')
    group_tcp.add_argument('--tcp', dest='mode', action='store_const', const=PROP_MODE_TCP, help="Set tcp mode")
    group_tcp.add_argument('--host', dest='hostname', help="Specify hostname", default='')
    group_tcp.add_argument('--port', dest='port', help="Specify port", default=23, type=int)
    group_listen = parser.add_argument_group('TCP server')
    group_listen.add_argument('--tcp-server', dest='mode', action='store_const',
                              const=PROP_MODE_TCP_SERVER,
                              help="Listen for the serial server to connect to us; "
                                   "--port is the local port to listen on and --host, "
                                   "if given, the only accepted peer")
    group_udp = parser.add_argument_group('UDP')
    group_udp.add_argument('--udp', dest='mode', action='store_const', const=PROP_MODE_UDP,
                           help="Set udp mode; --port is the local port to bind and "
                                "--host, if given, is the only accepted sender")
    group_serial = parser.add_argument_group('Serial')
    group_serial.add_argument('--serial', dest='mode', action='store_const', const=PROP_MODE_SERIAL, help="Set serial mode")
    group_serial.add_argument('--interface', dest='interface', help="Specify interface", default='')
    group_file = parser.add_argument_group('File')
    group_file.add_argument('--file', dest='mode', action='store_const', const=PROP_MODE_FILE, help="Set file mode")
    group_file.add_argument('--name', dest='file', help="Specify file name", default='')
    group_terminal = parser.add_argument_group('Terminal')
    log_levels = {
        'none': PROP_LOGLEVEL_NONE,
        'error': PROP_LOGLEVEL_ERROR,
        'warn': PROP_LOGLEVEL_WARN,
        'warning': PROP_LOGLEVEL_WARN,
        'info': PROP_LOGLEVEL_INFO,
        'debug': PROP_LOGLEVEL_DEBUG,
        'trace': PROP_LOGLEVEL_TRACE,
    }
    group_terminal.add_argument('--log-level', type=str.lower, choices=log_levels,
                                default='info', help="Log verbosity (default: info)")
    group_terminal.add_argument('--log', choices=('true', 'false'), default='true',
                                help="Print individual messages; false overrides --log-level (default: true)")
    group_terminal.add_argument('--summary', action='store_true', default=True,
                                help="Print sensor summaries (default: true)")
    group_terminal.add_argument('--no-summary', dest='summary', action='store_false',
                                help="Disable sensor summaries")
    args = parser.parse_args()
    if not 0 <= args.wait < float('inf'):
        parser.error('--wait must be a finite, non-negative number')
    if args.forever and args.wait == 0:
        parser.error('--wait must be positive with --forever')
    if any(message_id < 0 or message_id > 255 for message_id in args.decode):
        parser.error('--decode IDs must be between 0 and 255')

    if args.mode in (PROP_MODE_UDP, PROP_MODE_TCP_SERVER) and args.execution_mode != 'async':
        parser.error('--udp and --tcp-server require --mode async')

    config = {'decode': args.decode}
    if args.source is not None:
        config['source'] = args.source
    kwb = KWBEasyfire(args.mode, args.hostname, args.port, args.interface, 0, args.file,
                     _config=config)
    kwb._debug_level = (PROP_LOGLEVEL_NONE if args.log == 'false'
                        else log_levels[args.log_level])
    logging.basicConfig(level=logging.DEBUG, format="%(message)s", stream=sys.stdout)
    # Run in either async loop or thread
    try:
        if args.execution_mode == 'async':
            if args.forever:
                asyncio.run(_listen_with_summaries(kwb, args.wait, args.summary))
            else:
                asyncio.run(kwb.listen_for(seconds=args.wait))
        else:
            kwb.run_thread()
            try:
                while True:
                    time.sleep(args.wait)
                    if not args.forever:
                        break
                    if args.summary:
                        _print_summary(kwb)
                    if not kwb.is_alive():
                        break
            finally:
                kwb.stop_thread()
    except KeyboardInterrupt:
        return
    # Print summary
    if not args.forever and args.summary:
        _print_summary(kwb)


if __name__ == "__main__":
    main()
