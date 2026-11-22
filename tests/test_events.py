import json
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import consumer
from event_contract import validate_event


class EventsTest(unittest.TestCase):
    def setUp(self):
        consumer.seen_ids.clear()

    def message(self, value, offset):
        return SimpleNamespace(value=value, topic='line_events', partition=0, offset=offset)

    def test_bad_messages_do_not_stop_next_event(self):
        valid = dict(event_id='good', event_type='station_state', line_id='A',
                     event_time='2026-01-01T00:00:00Z', station_id='SEALER',
                     batch_id='b1', unit_id='u1', action='ENTER')
        missing = valid.copy()
        del missing['event_time']
        messages = [self.message(b'{', 0), self.message(b'{"event_type":"oops"}', 1),
                    self.message(json.dumps(missing).encode(), 2),
                    self.message(json.dumps(valid).encode(), 3)]
        with patch.object(consumer, 'append') as write:
            for message in messages:
                consumer.process_message(message)
            consumer.process_message(messages[0])
            calls = write.call_args_list
            self.assertEqual([c.args[1] for c in calls],
                             ['dead_letter_queue'] * 3 + ['station_events', 'dead_letter_queue'])
            bad = [c.args[0] for c in calls[:3]]
            self.assertEqual(len({x['rejection_reason'] for x in bad}), 3)
            self.assertEqual(bad[0]['event_id'], calls[-1].args[0]['event_id'])
            self.assertEqual(bad[1]['kafka']['offset'], 1)

    def test_invalid_fields(self):
        base = dict(event_id='e', event_type='production', line_id='A',
                    event_time=0, station_id='s', batch_id='b', good_count_inc=1,
                    cycles_count_inc=1, unit_id='u', material_lot_id='lot',
                    cycle_id='cycle', result='PASS', reject_reason=None)
        self.assertEqual(validate_event(base)['event_time'], '1970-01-01T00:00:00Z')
        for count in [-1, True, 1.5]:
            with self.subTest(count=count), self.assertRaises(ValueError):
                validate_event(dict(base, good_count_inc=count))
        for ts in [None, 'broken', '2026-01-01T00:00:00', float('nan')]:
            with self.subTest(ts=ts), self.assertRaises(ValueError):
                validate_event(dict(base, event_time=ts))
        with self.assertRaises(ValueError):
            validate_event([])

    def test_all_producer_event_types(self):
        common = dict(event_id='e', line_id='A', event_time='2026-01-01T00:00:00Z')
        examples = {
            'line_state': dict(state='RUN', state_start_time=0, reason_code=None),
            'production': dict(station_id='s', batch_id='b', good_count_inc=1, cycles_count_inc=0,
                               unit_id='u', material_lot_id='l', cycle_id='c',
                               result='PASS', reject_reason=None),
            'reject': dict(station_id='s', batch_id='b', unit_id='u', reject_reason='UNDERFILL', material_lot_id='l'),
            'qc_inspection': dict(station_id='s', batch_id='b', unit_id='u', inspection_type='VISION', result='FAIL'),
            'station_state': dict(station_id='s', batch_id='b', unit_id='u', action='EXIT'),
        }
        for kind, fields in examples.items():
            with self.subTest(kind=kind):
                validate_event(dict(common, event_type=kind, **fields))


if __name__ == '__main__':
    unittest.main()
