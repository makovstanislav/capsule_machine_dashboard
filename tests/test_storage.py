"""Run against local OpenSearch with RUN_INTEGRATION=1; only temporary indices are used."""
import copy
import os
import unittest
import uuid
from unittest.mock import patch
from urllib.parse import urlsplit, urlunsplit
import requests
import consumer
from production import counts, save_unit


class TestClient:
    def __init__(self):
        self.prefix = 'coffee_test_' + uuid.uuid4().hex + '_'

    def request(self, method, url, **kwargs):
        parts = urlsplit(url)
        path = parts.path.split('/')
        if path[1] == '_index_template':
            path[2] = self.prefix + path[2]
            if 'json' in kwargs:
                body = copy.deepcopy(kwargs['json'])
                body['index_patterns'] = [self.prefix + name for name in body['index_patterns']]
                kwargs['json'] = body
        else:
            path[1] = self.prefix + path[1]
        parts = parts._replace(path='/'.join(path))
        target = os.environ.get('TEST_OPENSEARCH_URL', 'http://localhost:9200')
        return requests.request(method, target + parts.path + ('?' + parts.query if parts.query else ''), **kwargs)

    def post(self, url, **kwargs):
        return self.request('POST', url, **kwargs)

    def put(self, url, **kwargs):
        return self.request('PUT', url, **kwargs)


@unittest.skipUnless(os.environ.get('RUN_INTEGRATION') == '1', 'set RUN_INTEGRATION=1 for local OpenSearch')
class OpenSearchTest(unittest.TestCase):
    def setUp(self):
        self.client = TestClient()
        self.mock = patch.object(consumer, 'requests', self.client)
        self.mock.start()
        self.quiet = patch('builtins.print')
        self.quiet.start()
        consumer.install_mappings()
        for value in (consumer.previous_state, consumer.last_event_time, consumer.last_seen,
                      consumer.total_run_minutes, consumer.total_good_units, consumer.total_cycles):
            value.clear()

    def tearDown(self):
        self.quiet.stop()
        self.mock.stop()
        requests.delete(os.environ.get('TEST_OPENSEARCH_URL', 'http://localhost:9200') + f'/{self.client.prefix}*', timeout=10).raise_for_status()
        requests.delete(os.environ.get('TEST_OPENSEARCH_URL', 'http://localhost:9200') + f'/_index_template/{self.client.prefix}coffee_events', timeout=10).raise_for_status()

    def production(self, number, result='PASS', line='A', time='09:00'):
        return dict(event_id=f'{line}:{number}', event_type='production', line_id=line,
                    unit_id=f'unit_{number}', batch_id='batch', material_lot_id='lot',
                    cycle_id='cycle_1', cycles_count_inc=1 if number == 0 else 0,
                    station_id='PACKER', event_time=f'2026-01-01T{time}:00Z',
                    result=result, good_count_inc=int(result == 'PASS'),
                    reject_reason='UNDERFILL' if result == 'FAIL' else None)

    def test_unique_capsules_reject_wins_and_window(self):
        for number in range(10):
            consumer.handle_production(self.production(number, 'FAIL' if number < 2 else 'PASS'))
        self.assertEqual(counts(self.client, 'A'), dict(total_produced=10, total_good_units=8,
                                                      total_rejects=2, reject_rate=20.0))
        reject = dict(self.production(0, 'FAIL'), event_type='reject', event_id='reject1')
        consumer.handle_reject(reject)
        consumer.handle_reject(dict(reject, event_id='reject2'))
        consumer.handle_production(self.production(0))
        self.assertEqual(counts(self.client, 'A')['reject_rate'], 20)
        for number in range(10, 20):
            consumer.handle_production(self.production(number, time='09:01'))
        self.assertEqual(counts(self.client, 'A')['reject_rate'], 10)
        self.assertEqual(counts(self.client, 'A', end='2026-01-01T09:01:00Z')['total_produced'], 10)
        self.assertEqual(counts(self.client, 'A', start='2026-01-01T09:01:00Z')['reject_rate'], 0)
        self.assertIsNone(counts(self.client, 'B')['reject_rate'])
        # Rejects received before release do not invent produced units.
        orphan = dict(self.production(100, 'FAIL'), event_type='reject')
        consumer.handle_reject(orphan)
        self.assertEqual(counts(self.client, 'A')['total_produced'], 20)
        consumer.handle_production(self.production(100))
        self.assertEqual(counts(self.client, 'A')['total_rejects'], 3)
        # Clearing process counters and replaying does not change stored totals.
        consumer.total_good_units.clear()
        consumer.total_cycles.clear()
        consumer.handle_production(self.production(0))
        self.assertEqual(counts(self.client, 'A')['total_produced'], 21)
        self.assertEqual(consumer.total_cycles['A'], 1)

    def test_stored_intervals_and_hour_buckets(self):
        def event(state, start, end, event_id):
            return dict(event_id=event_id, line_id='A', state=state, reason_code=None,
                        event_time=f'2026-01-01T{end}:00Z', state_start_time=f'2026-01-01T{start}:00Z')
        consumer.handle_line_state(event('DOWN', '08:50', '09:00', 'a'))
        consumer.handle_line_state(event('DOWN', '08:50', '09:05', 'b'))
        consumer.handle_line_state(event('RUN', '09:10', '09:10', 'c'))
        self.client.post('http://localhost:9200/state_hours/_refresh', timeout=10).raise_for_status()
        response = self.client.post('http://localhost:9200/state_hours/_search', json={
            'query': {'term': {'state': 'DOWN'}}, 'sort': ['hour']}, timeout=10)
        response.raise_for_status()
        docs = [hit['_source'] for hit in response.json()['hits']['hits']]
        self.assertEqual([doc['duration_minutes'] for doc in docs], [10, 10])
        import json
        from pathlib import Path
        spec = json.loads(Path('docs/visualizations/downtime.json').read_text())
        body = spec['data']['url']['body']
        body['query']['bool'] = {'filter': [{'term': {'state': 'DOWN'}}]}
        body = json.loads(json.dumps(body).replace('{"%timefilter%": "min"}', '1767257700000')
                          .replace('{"%timefilter%": "max"}', '1767258300000'))
        clipped = self.client.post('http://localhost:9200/state_hours/_search', json=body, timeout=10)
        clipped.raise_for_status()
        self.assertEqual(sum(bucket['minutes']['value'] for bucket in clipped.json()['aggregations']['hours']['buckets']), 10)

    def test_reject_breakdowns_reconcile_and_survive_replay(self):
        from rejects import distribution
        for number in range(5):
            consumer.handle_production(self.production(number))
        events = []
        for number, reason, station in [(0, 'UNDERFILL', 'SEALER'), (1, 'UNDERFILL', 'SEALER'),
                                        (2, 'SEAL_DEFECT', 'FILLER'), (3, 'new-code', 'no-station')]:
            event = dict(self.production(number), event_type='reject', result='FAIL',
                         event_id=f'reject-{number}', event_time='2026-01-01T09:01:00Z',
                         reject_reason=reason, station_id=station)
            consumer.handle_reject(event)
            events.append(event)
        result = distribution(self.client, 'A')
        self.assertEqual(result['total_rejects'], counts(self.client, 'A')['total_rejects'])
        self.assertEqual([row['doc_count'] for row in result['reasons']], [2, 1, 1])
        self.assertEqual(result['reasons'][-1]['cumulative_pct'], 100)
        self.assertEqual(sum(row['doc_count'] for row in result['stations']), 4)
        self.assertEqual({row['key']: row['doc_count'] for row in result['stations']},
                         {'SEALER': 2, 'FILLER': 1, 'UNKNOWN_STATION': 1})
        # A later defect must not replace the primary pair, even when replayed first.
        later = dict(events[0], event_id='later', event_time='2026-01-01T09:02:00Z',
                     reject_reason='CAPSULE_CRACK', station_id='PACKER')
        consumer.handle_reject(later)
        for event in reversed(events):
            consumer.handle_reject(event)
        consumer.total_good_units.clear()
        consumer.handle_production(self.production(0))
        self.assertEqual(distribution(self.client, 'A'), result)
        self.assertEqual(distribution(self.client, 'B')['total_rejects'], 0)
        self.assertEqual(distribution(self.client, 'A', start='2026-01-01T09:01:00Z')['total_rejects'], 0)
        # An earlier failure delivered last becomes the primary reason and station together.
        earlier = dict(later, event_id='earlier', event_time='2026-01-01T09:00:30Z')
        consumer.handle_reject(earlier)
        consumer.handle_reject(events[0])
        changed = distribution(self.client, 'A')
        self.assertIn({'key': 'PACKER', 'doc_count': 1}, changed['stations'])
        self.assertEqual(changed['total_rejects'], 4)
        # Range end is exclusive.
        self.assertEqual(distribution(self.client, 'A', end='2026-01-01T09:00:00Z')['total_rejects'], 0)

    def test_station_acceptance_and_visualization_queries(self):
        import json
        from pathlib import Path
        from rejects import distribution
        for number, station in enumerate(['SEALER', 'SEALER', None]):
            event = dict(self.production(number, 'FAIL'), reject_station_id=station)
            consumer.handle_production(event)
        result = distribution(self.client, 'A')
        self.assertEqual(result['stations'], [{'key': 'SEALER', 'doc_count': 2},
                                             {'key': 'UNKNOWN_STATION', 'doc_count': 1}])
        for field in ['reasons', 'stations']:
            spec = json.loads(Path(f'docs/visualizations/reject_{field}.json').read_text())
            body = spec['data']['url']['body']
            body['query']['bool']['must'] = []
            body['query']['bool']['must_not'] = []
            body['query']['bool']['filter'].remove('%dashboard_context-filter_clause%')
            body = json.loads(json.dumps(body).replace('{"%timefilter%": "min"}', '"2026-01-01T09:00:00Z"')
                              .replace('{"%timefilter%": "max"}', '"2026-01-01T09:01:00Z"'))
            response = self.client.post('http://localhost:9200/capsule_units/_search', json=body, timeout=10)
            response.raise_for_status()
            buckets = response.json()['aggregations'][field]['buckets']
            self.assertEqual(sum(row['doc_count'] for row in buckets), result['total_rejects'])
