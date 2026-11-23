import unittest
from unittest.mock import Mock
from event_contract import validate_event
from rejects import normalize, pareto, distribution, distribution_query


class RejectTest(unittest.TestCase):
    def test_unknown_and_missing_codes(self):
        base = dict(event_id='e', event_type='reject', line_id='A', unit_id='u',
                    batch_id='b', material_lot_id='lot', event_time='2026-01-01T00:00:00Z')
        for fields in ({}, {'station_id': 'nope', 'reject_reason': 'nope'},
                       {'station_id': [], 'reject_reason': None}):
            with self.subTest(fields=fields):
                event = validate_event(dict(base, **fields))
                self.assertEqual(event['station_id'], 'UNKNOWN_STATION')
                self.assertEqual(event['reject_reason'], 'UNKNOWN_REASON')
        self.assertEqual(normalize(dict(base, station_id='SEALER', reject_reason='UNDERFILL'))['station_id'], 'SEALER')

    def test_pareto(self):
        rows = pareto([{'key': 'B', 'doc_count': 1}, {'key': 'A', 'doc_count': 2},
                       {'key': 'UNKNOWN_REASON', 'doc_count': 1}])
        self.assertEqual([row['doc_count'] for row in rows], [2, 1, 1])
        self.assertEqual([row['cumulative_pct'] for row in rows], [50, 75, 100])
        self.assertEqual(pareto([]), [])

    def test_incomplete_buckets_fail_reconciliation(self):
        client = Mock()
        client.post.return_value.status_code = 200
        client.post.return_value.json.return_value = {
            'hits': {'total': {'value': 4}}, 'aggregations': {
                'reasons': {'buckets': [{'key': 'A', 'doc_count': 3}]}}}
        with self.assertRaisesRegex(ValueError, 'reasons does not reconcile'):
            distribution(client, 'A')

    def test_filters_match_release_counts(self):
        filters = distribution_query('A', '2026-01-01T00:00:00Z', '2026-01-02T00:00:00Z')['query']['bool']['filter']
        self.assertIn({'term': {'produced': True}}, filters)
        self.assertIn({'term': {'line_id': 'A'}}, filters)
        self.assertEqual(filters[-1]['range']['event_time']['lt'], '2026-01-02T00:00:00Z')
