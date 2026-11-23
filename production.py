"""Persist capsule outcomes and calculate counts for a line and time range."""
import hashlib
from datetime import datetime, timezone
from rejects import normalize


def document_id(*parts):
    return hashlib.sha256('\0'.join(parts).encode()).hexdigest()


def counts(client, line_id, start=None, end=None):
    filters = [{'term': {'line_id': line_id}}, {'term': {'produced': True}}]
    if start or end:
        bounds = {}
        if start:
            bounds['gte'] = start
        if end:
            bounds['lt'] = end
        filters.append({'range': {'event_time': bounds}})
    response = client.post('http://localhost:9200/capsule_units/_search', json={
        'size': 0, 'query': {'bool': {'filter': filters}},
        'aggs': {'outcomes': {'filters': {'filters': {
            'good': {'term': {'result': 'PASS'}},
            'rejected': {'term': {'result': 'FAIL'}},
        }}}}}, timeout=10)
    if response.status_code == 404:
        return {'total_produced': 0, 'total_good_units': 0, 'total_rejects': 0, 'reject_rate': None}
    response.raise_for_status()
    buckets = response.json()['aggregations']['outcomes']['buckets']
    good, rejected = buckets['good']['doc_count'], buckets['rejected']['doc_count']
    produced = good + rejected
    return {'total_produced': produced, 'total_good_units': good,
            'total_rejects': rejected,
            'reject_rate': round(100 * rejected / produced, 1) if produced else None}


def save_unit(client, event, produced):
    event = normalize(event)
    unit = {key: event[key] for key in ('line_id', 'unit_id', 'batch_id', 'material_lot_id')}
    unit.update(event_time=event['event_time'], produced=produced,
                result=event.get('result', 'FAIL') if produced else 'FAIL', reject_reason=event.get('reject_reason'),
                reject_station_id=event.get('reject_station_id', event.get('station_id')))
    if unit['result'] == 'FAIL':
        failure_time = datetime.fromisoformat(event['event_time'].replace('Z', '+00:00'))
        unit['reject_key'] = failure_time.astimezone(timezone.utc).isoformat(timespec='microseconds') + ':' + event['event_id']
    # Production owns the identity and release time. A reject can only make FAIL sticky.
    script = """
        boolean failed = ctx._source.result == 'FAIL';
        def key = ctx._source.reject_key;
        def reason = ctx._source.reject_reason;
        def station = ctx._source.reject_station_id;
        if (params.unit.result == 'FAIL' &&
            (!failed || key == null || params.unit.reject_key.compareTo(key) < 0)) {
            failed = true;
            key = params.unit.reject_key;
            reason = params.unit.reject_reason;
            station = params.unit.reject_station_id;
        }
        if (params.unit.produced) {
            ctx._source.putAll(params.unit);
        }
        if (failed) {
            ctx._source.result = 'FAIL';
            ctx._source.reject_key = key;
            ctx._source.reject_reason = reason;
            ctx._source.reject_station_id = station;
        }
    """
    doc_id = document_id(event['line_id'], event['unit_id'])
    response = client.post(f'http://localhost:9200/capsule_units/_update/{doc_id}?refresh=wait_for',
                           json={'script': {'source': script, 'params': {'unit': unit}},
                                 'upsert': unit}, timeout=10)
    response.raise_for_status()


if __name__ == '__main__':
    import argparse
    import json
    import requests
    parser = argparse.ArgumentParser()
    parser.add_argument('line_id')
    parser.add_argument('--start')
    parser.add_argument('--end')
    args = parser.parse_args()
    print(json.dumps(counts(requests, args.line_id, args.start, args.end), indent=2))
