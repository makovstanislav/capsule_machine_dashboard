"""Normalize defects and build complete, reconcilable reject distributions."""
import json
from pathlib import Path

CONFIG = Path(__file__).parent / 'config'
REASONS = set(json.loads((CONFIG / 'reject_reasons.json').read_text()))
STATIONS = {item['station_id'] for item in json.loads((CONFIG / 'stations.json').read_text())}


def code(value, valid, fallback):
    return value if isinstance(value, str) and value in valid else fallback


def normalize(event):
    event = event.copy()
    kind = event.get('event_type')
    if kind == 'reject' or (kind == 'production' and event.get('result') == 'FAIL'):
        event['reject_reason'] = code(event.get('reject_reason'), REASONS, 'UNKNOWN_REASON')
        field = 'station_id' if event.get('event_type') == 'reject' else 'reject_station_id'
        event[field] = code(event.get(field), STATIONS, 'UNKNOWN_STATION')
    return event


def distribution_query(line_id=None, start=None, end=None):
    filters = [{'term': {'produced': True}}, {'term': {'result': 'FAIL'}}]
    if line_id is not None:
        filters.append({'term': {'line_id': line_id}})
    if start or end:
        filters.append({'range': {'event_time': dict(
            **({'gte': start} if start else {}), **({'lt': end} if end else {}))}})
    return {'size': 0, 'track_total_hits': True, 'query': {'bool': {'filter': filters}},
            'aggs': {name: {'terms': {'field': field, 'missing': missing,
                                    'size': size, 'order': [{'_count': 'desc'}, {'_key': 'asc'}]}}
                     for name, field, missing, size in (
                         ('reasons', 'reject_reason', 'UNKNOWN_REASON', len(REASONS) + 1),
                         ('stations', 'reject_station_id', 'UNKNOWN_STATION', len(STATIONS) + 1))}}


def pareto(buckets):
    total = sum(bucket['doc_count'] for bucket in buckets)
    cumulative = 0
    result = []
    for bucket in sorted(buckets, key=lambda row: (-row['doc_count'], row['key'])):
        cumulative += bucket['doc_count']
        result.append(dict(bucket, cumulative_pct=100 * cumulative / total if total else None))
    return result


def distribution(client, line_id, start=None, end=None):
    response = client.post('http://localhost:9200/capsule_units/_search',
                           json=distribution_query(line_id, start, end), timeout=10)
    if response.status_code == 404:
        return {'total_rejects': 0, 'reasons': [], 'stations': []}
    response.raise_for_status()
    body = response.json()
    total = body['hits']['total']['value']
    result = {'total_rejects': total}
    for field in ('reasons', 'stations'):
        aggregation = body['aggregations'][field]
        rows = aggregation['buckets']
        if aggregation.get('sum_other_doc_count', 0) or sum(row['doc_count'] for row in rows) != total:
            raise ValueError(f'{field} does not reconcile with total rejects')
        result[field] = pareto(rows) if field == 'reasons' else rows
    return result


if __name__ == '__main__':
    import argparse
    import requests
    parser = argparse.ArgumentParser()
    parser.add_argument('line_id')
    parser.add_argument('--start')
    parser.add_argument('--end')
    args = parser.parse_args()
    print(json.dumps(distribution(requests, args.line_id, args.start, args.end), indent=2))
