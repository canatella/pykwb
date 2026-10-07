"""Exploratory decoding is opt-in, bounded, and independent of sensor maps."""
from contextlib import redirect_stdout
from io import StringIO
import unittest
from unittest.mock import patch

from pykwb.kwb import KWBEasyfire, PROP_PACKET_CTRL, main
from pykwb.decode import decode_pairs


def logged_lines(logs):
    return '\n'.join(r.getMessage() for r in logs.records).splitlines()


class PairDecodeTests(unittest.TestCase):
    def test_both_alignments_and_trailing_byte(self):
        reader = KWBEasyfire(-1, _config={'decode': [87]})
        payload = bytes.fromhex('00 00 00 02 5f ff c9 05 14')
        before = [s.value for s in reader.get_sensors()]
        with self.assertLogs('pykwb', 'DEBUG') as logs:
            reader._decode_packet(PROP_PACKET_CTRL, 87, payload)
        self.assertEqual(logged_lines(logs), [
            'ID 87 two-byte decode from offset 3:',
            '  Offset 3: raw=607 temperature=60.7 mbar=0.607 rpm=364.2 ms=6070',
            '  Offset 5: raw=-55 temperature=-5.5 mbar=65.481 rpm=39288.6 ms=654810',
            '  Offset 7: raw=1300 temperature=None mbar=1.3 rpm=780.0 ms=13000',
            'ID 87 two-byte decode from offset 4:',
            '  Offset 4: raw=24575 temperature=2457.5 mbar=24.575 rpm=14745.0 ms=245750',
            '  Offset 6: raw=-14075 temperature=-1407.5 mbar=51.461 rpm=30876.6 ms=514610',
        ])
        self.assertEqual([s.value for s in reader.get_sensors()], before)

    def test_unsigned_units_at_zero_and_maximum(self):
        for pair, expected in (
                (b'\x00\x00', 'mbar=0.0 rpm=0.0 ms=0'),
                (b'\xff\xff', 'mbar=65.535 rpm=39321.0 ms=655350')):
            with self.subTest(pair=pair):
                lines = list(decode_pairs(64, bytes(3) + pair))
                self.assertTrue(lines[1].endswith(expected), lines[1])

    def test_default_and_nonselected_ids_do_not_decode(self):
        for config in (None, {'decode': []}, {'decode': [65]}):
            reader = KWBEasyfire(-1, _config=config)
            with patch('pykwb.kwb.decode_pairs') as decode:
                reader._decode_packet(PROP_PACKET_CTRL, 87, bytes(10))
            decode.assert_not_called()

    def test_short_packets_and_logging_disabled(self):
        reader = KWBEasyfire(-1, _config={'decode': [87]})
        reader._debug_level = 0
        output = StringIO()
        with redirect_stdout(output):
            for length in range(10):
                reader._decode_packet(PROP_PACKET_CTRL, 87, bytes(length))
        self.assertEqual(output.getvalue(), '')

    def test_cli_list_and_default(self):
        for args, expected in (([], []), (['--decode'], []),
                               (['--decode', '64', '87'], [64, 87])):
            with self.subTest(args=args), \
                    patch('sys.argv', ['kwb', '--summary', 'false'] + args), \
                    patch('pykwb.kwb.KWBEasyfire') as factory, \
                    patch('pykwb.kwb.time.sleep'):
                main()
            self.assertEqual(factory.call_args.kwargs['_config']['decode'], expected)

    def test_cli_rejects_invalid_ids(self):
        for value in ('-1', '256', 'abc', '32.5'):
            with self.subTest(value=value), patch('sys.argv', ['kwb', '--decode', value]), \
                    patch('sys.stderr'), patch('pykwb.kwb.KWBEasyfire') as factory:
                with self.assertRaises(SystemExit) as error:
                    main()
                self.assertEqual(error.exception.code, 2)
                factory.assert_not_called()
