import unittest
from unittest.mock import patch
import producer
from event_contract import validate_event


class ProducerTest(unittest.TestCase):
    def test_capsules_share_outcomes_and_ids(self):
        events = []
        with patch.object(producer, 'unit_counter', 10), \
             patch.object(producer, 'send', side_effect=events.append), \
             patch.object(producer.random, 'random', side_effect=[0, 0] + [1] * 8):
            producer.generate_cycle(10)
        for event in events:
            validate_event(event)
        units = {e['unit_id']: e for e in events if e['event_type'] == 'production'}
        rejects = [e for e in events if e['event_type'] == 'reject']
        self.assertEqual(len(units), 10)
        self.assertEqual(len(rejects), 2)
        self.assertEqual(sum(e['good_count_inc'] for e in units.values()), 8)
        self.assertEqual(sum(e['cycles_count_inc'] for e in units.values()), 1)
        for reject in rejects:
            production = units[reject['unit_id']]
            self.assertEqual(production['result'], 'FAIL')
            self.assertEqual(production['reject_reason'], reject['reject_reason'])
            qc = [e for e in events if e['event_type'] == 'qc_inspection'
                  and e['unit_id'] == reject['unit_id'] and e['result'] == 'FAIL']
            self.assertEqual(len(qc), 1)
            self.assertEqual(qc[0]['station_id'], reject['station_id'])
