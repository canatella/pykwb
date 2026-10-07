"""Regression coverage for boiler temperature layouts and wire framing."""
import unittest
from pathlib import Path
from unittest.mock import call, patch

from pykwb.kwb import KWBEasyfire, PROP_MODE_FILE, PROP_MODE_TCP, PROP_PACKET_SENSE, PROP_PACKET_CTRL, PROP_SENSOR_TEMPERATURE, PROP_SENSOR_FLAG


ROOT = Path(__file__).resolve().parents[1]


def frame(message_id, payload, sense=True):
    """Encode a frame with the KWB rotating checksum and payload escaping."""
    header = bytes((2, len(payload) + 5, message_id, 1))
    checksum = 0
    for value in header + payload:
        checksum = ((checksum << 1) | (checksum >> 7)) & 255
        checksum += value
        if checksum > 255:
            checksum -= 255
    return ((b'\x02' if sense else b'') + header
            + payload.replace(b'\x02', b'\x02\x00') + bytes((checksum,)))


class TemperatureTests(unittest.TestCase):
    def make_reader(self):
        reader = KWBEasyfire(-1)
        reader._debug_level = 0
        return reader

    def test_signed_temperatures_and_disconnected_sensor(self):
        for encoded, expected in ((b'\x02\x5f', 60.7), (b'\xff\xc9', -5.5),
                                  (b'\x80\x00', -3276.8), (b'\x00\x00', 0),
                                  (b'\x01\xf4', 50), (b'\x05\x14', None)):
            with self.subTest(encoded=encoded):
                self.assertEqual(KWBEasyfire._decode_temp(*encoded), expected)

    def test_framing_escapes_lengths_and_checksum(self):
        reader = self.make_reader()
        # Include an escaped 2 followed by a real zero, plus another escaped 2.
        payload = b'\x02\x00\x02\x07'
        good = frame(32, payload)
        corrupt = good[:-1] + bytes((good[-1] ^ 1,))
        stream = iter(corrupt + good + frame(33, b'\x01' * 24, sense=False))
        with patch.object(reader, '_read_ord_byte', side_effect=lambda: next(stream)):
            self.assertEqual(reader._read_packet(), (PROP_PACKET_SENSE, 32, payload))
            mode, message_id, decoded = reader._read_packet()
            self.assertEqual((mode, message_id, decoded), (PROP_PACKET_CTRL, 33, b'\x01' * 24))

    def test_recorded_boiler_layouts(self):
        cases = (
            ('kwb_17_16.txt', 16, 59,
             [None] * 13),
            ('kwb_33_32.txt', 32, 62,
             [30.4, 77.5, 45.3, 74.1, None, None, 14.6, 73.1,
              32.4, 19.8, 50.0, None, 3276.7]),
        )
        for filename, sense_id, count, expected in cases:
            with self.subTest(filename=filename):
                reader = KWBEasyfire(PROP_MODE_FILE, _file_path=ROOT / 'testdata' / filename)
                reader._debug_level = 0
                self.addCleanup(reader._close_connection)
                counts = {}
                while True:
                    try:
                        mode, message_id, payload = reader._read_packet()
                    except EOFError:
                        break
                    counts[message_id] = counts.get(message_id, 0) + 1
                    if mode == PROP_PACKET_SENSE and counts[message_id] == 1:
                        reader._decode_sense_packet(message_id, payload)
                        sensors = [s for s in reader._sensors[PROP_PACKET_SENSE]
                                   if s.sensor_type == PROP_SENSOR_TEMPERATURE]
                        self.assertEqual([s.value for s in sensors], expected)
                        self.assertEqual([s.available for s in sensors],
                                         [v is not None for v in expected])
                self.assertEqual(counts, {sense_id: count, sense_id + 1: count})

    def test_truncated_payload_recovers_at_next_header(self):
        for sense in (False, True):
            with self.subTest(sense=sense):
                reader = self.make_reader()
                # A frame declaring 20 payload bytes stops after just one.
                truncated = bytes((2, 25, 17, 1, 255))
                payload = b'\x02\x00\x02\x07'
                stream = iter(truncated + frame(32, payload, sense=sense))
                with patch.object(reader, '_read_ord_byte', side_effect=lambda: next(stream)):
                    packet_type = PROP_PACKET_SENSE if sense else PROP_PACKET_CTRL
                    self.assertEqual(reader._read_packet(), (packet_type, 32, payload))

    def test_empty_and_short_payloads_do_not_crash_temperature_decoder(self):
        reader = self.make_reader()
        for message_id in (16, 32, 64, 255):
            for length in range(33):
                with self.subTest(message_id=message_id, length=length):
                    reader._decode_sense_packet(message_id, bytes(length))

    def test_closed_tcp_connection_stops_reader_cleanly(self):
        with patch('pykwb.kwb.socket.socket') as socket_factory:
            socket_factory.return_value.recv.return_value = b''
            reader = KWBEasyfire(PROP_MODE_TCP)
            reader._debug_level = 0
            reader.run()
            self.assertFalse(reader._run_thread)

    def test_eof_in_partial_packet_stops_reader_cleanly(self):
        reader = self.make_reader()
        with patch.object(reader, '_read_ord_byte', side_effect=[2, 25, 17, 1, EOFError()]):
            reader.run()
        self.assertFalse(reader._run_thread)

    def test_captured_short_control_frame_keeps_reader_running(self):
        reader = self.make_reader()
        reader._decode_ctrl_packet(33, bytes((255, 255, 255)))
        flags_before = [sensor.value for sensor in reader._sensors[PROP_PACKET_CTRL][1:]]
        payload = bytearray(32)
        payload[12:14] = b'\x02\xe5'
        wire = bytes((2, 7, 0, 65, 27, 82, 62)) + frame(32, payload)
        position = 0

        def read_byte():
            nonlocal position
            value = wire[position]
            position += 1
            if position == len(wire):
                reader._run_thread = False
            return value

        with patch.object(reader, '_read_ord_byte', side_effect=read_byte):
            reader.run()
        self.assertEqual(reader._sensors[PROP_PACKET_CTRL][0].value, bytes((255, 255, 255)))
        self.assertEqual([sensor.value for sensor in reader._sensors[PROP_PACKET_CTRL][1:]], flags_before)
        self.assertEqual(next(s for s in reader.get_sensors() if s.key == 'heater_temp').value, 74.1)

    def test_control_flags_use_message_33_positions(self):
        reader = self.make_reader()
        positions = [
            (1, 2), (1, 5), (1, 6), (1, 7), (2, 0), (2, 1), (2, 2),
            (2, 3), (2, 4), (2, 5), (2, 6), (2, 7), (3, 0),
            (3, 2), (3, 6), (3, 7), (3, 7), (4, 1), (4, 5), (5, 0),
            (9, 1), (9, 2), (16, 2),
        ]
        flags = [s for s in reader._sensors[PROP_PACKET_CTRL]
                 if s.sensor_type == PROP_SENSOR_FLAG]
        self.assertEqual(len(flags), len(positions))
        # Walk every payload bit to detect wrong offsets and cross-talk.
        for offset in range(24):
            for bit in range(8):
                payload = bytearray(24)
                payload[offset] = 1 << bit
                reader._decode_ctrl_packet(33, payload)
                for sensor, position in zip(flags, positions):
                    expected = None if position is None else int(position == (offset, bit))
                    with self.subTest(sensor=sensor.name, offset=offset, bit=bit):
                        self.assertEqual(sensor.value, expected)
                        self.assertEqual(sensor.available, position is not None)

    def test_short_known_control_payloads_mark_missing_flags_unavailable(self):
        reader = self.make_reader()
        for length in range(25):
            reader._decode_ctrl_packet(33, bytes((255,)) * 24)
            reader._decode_ctrl_packet(33, bytes(length))
            for sensor in reader._sensors[PROP_PACKET_CTRL][1:]:
                if sensor.sensor_type != PROP_SENSOR_FLAG:
                    continue
                present = sensor.index < length
                with self.subTest(sensor=sensor.name, length=length):
                    self.assertEqual(sensor.value, 0 if present else None)
                    self.assertEqual(sensor.available, present)

    def test_unrelated_messages_do_not_overwrite_boiler_temperatures(self):
        reader = self.make_reader()
        payload = bytearray(32)
        payload[12:14] = b'\x02\xe5'
        reader._decode_sense_packet(32, payload)
        reader._decode_sense_packet(64, bytes(24))
        self.assertEqual(next(s for s in reader.get_sensors() if s.key == 'heater_temp').value, 74.1)

    def test_message_64_wire_decoding_and_missing_temperatures(self):
        reader = self.make_reader()
        payload = bytearray(23)
        payload[19:21] = b'\x02\x5f'
        payload[21:23] = b'\xff\xc9'
        wire = iter(frame(64, payload))
        with patch.object(reader, '_read_ord_byte', side_effect=lambda: next(wire)):
            reader._decode_packet(*reader._read_packet())
        sensors = {s.key: s for s in reader.get_sensors() if s.key}
        loop_4 = sensors['loop_4_out_temp']
        loop_3 = sensors['loop_3_out_temp']
        self.assertEqual((loop_4.value, loop_3.value), (60.7, -5.5))
        self.assertEqual((loop_4.unit_of_measurement, loop_3.unit_of_measurement),
                         ('°C', '°C'))
        self.assertEqual(reader._sensors[64][0].value, payload)
        self.assertIsNone(sensors['heater_temp'].value)

        # Boiler frames and control frames with ID 64 cannot change these values.
        reader._decode_packet(PROP_PACKET_SENSE, 32, bytes(32))
        reader._decode_packet(PROP_PACKET_CTRL, 64, bytes(23))
        self.assertEqual((loop_4.value, loop_3.value), (60.7, -5.5))

        payload[19:21] = b'\x05\x14'
        reader._decode_sense_packet(64, payload[:22])
        self.assertEqual((loop_4.value, loop_3.value), (None, None))
        self.assertFalse(loop_4.available)
        self.assertFalse(loop_3.available)
        reader._decode_sense_packet(64, bytes(23))
        self.assertEqual((loop_4.value, loop_3.value), (0, 0))
        self.assertTrue(loop_4.available)
        self.assertTrue(loop_3.available)

    def test_unconfigured_packets_get_only_a_summary(self):
        reader = KWBEasyfire(-1)
        wire = iter(frame(87, bytes(24), sense=False)
                    + frame(48, bytes(34))
                    + frame(32, bytes(32))
                    + frame(33, bytes(24), sense=False))

        def read_byte():
            try:
                return next(wire)
            except StopIteration:
                raise EOFError from None

        with self.assertLogs('pykwb', 'DEBUG') as logs, \
                patch.object(reader, '_read_ord_byte', side_effect=read_byte), \
                patch.object(reader, '_decode_sense_packet') as sense, \
                patch.object(reader, '_decode_ctrl_packet') as ctrl:
            reader.run()
        self.assertEqual(
            [line for r in logs.records for line in r.getMessage().splitlines() if line],
            ['Packet ID 87 CTRL counter=1 length=24',
             'Packet ID 48 SENSE counter=1 length=34',
             'Packet ID 32 SENSE counter=1 length=32',
             'Packet ID 33 CTRL counter=1 length=24'])
        sense.assert_called_once_with(32, bytes(32))
        ctrl.assert_called_once_with(33, bytes(24))

    def test_only_explicit_message_ids_are_decoded(self):
        reader = self.make_reader()
        packets = [
            (PROP_PACKET_SENSE, 16, bytes(40)),
            (PROP_PACKET_CTRL, 17, bytes(24)),
            (PROP_PACKET_SENSE, 64, bytes(34)),
            (PROP_PACKET_CTRL, 65, bytes(8)),
            (PROP_PACKET_CTRL, 64, bytes(34)),
            (PROP_PACKET_SENSE, 32, bytes(32)),
            (PROP_PACKET_CTRL, 33, bytes(24)),
            EOFError(),
        ]
        with patch.object(reader, '_read_packet', side_effect=packets), \
                patch.object(reader, '_decode_sense_packet') as sense, \
                patch.object(reader, '_decode_ctrl_packet') as ctrl:
            reader.run()
        self.assertEqual(sense.call_args_list,
                         [call(64, bytes(34)), call(32, bytes(32))])
        ctrl.assert_called_once_with(33, bytes(24))

    def test_disconnected_sensor_clears_previous_reading(self):
        reader = self.make_reader()
        payload = bytearray(32)
        payload[12:14] = b'\x02\xe5'
        reader._decode_sense_packet(32, payload)
        sensor = next(s for s in reader.get_sensors() if s.key == 'heater_temp')
        self.assertTrue(sensor.available)
        payload[12:14] = b'\x05\x14'
        reader._decode_sense_packet(32, payload)
        self.assertIsNone(sensor.value)
        self.assertFalse(sensor.available)


if __name__ == '__main__':
    unittest.main()
