"""Validation and timestamp normalization for Kafka input events."""
from datetime import datetime, timezone
import math

REQUIRED = {
    'line_state': ('state', 'state_start_time', 'reason_code'),
    'production': ('station_id', 'batch_id', 'good_count_inc', 'cycles_count_inc',
                   'unit_id', 'material_lot_id', 'cycle_id', 'result', 'reject_reason'),
    'reject': ('station_id', 'batch_id', 'unit_id', 'reject_reason', 'material_lot_id'),
    'qc_inspection': ('station_id', 'batch_id', 'unit_id', 'inspection_type', 'result'),
    'station_state': ('station_id', 'batch_id', 'unit_id', 'action'),
}
ENUMS = {'state': {'RUN', 'IDLE', 'DOWN'},
         'inspection_type': {'VISION', 'CHECKWEIGHER'},
         'result': {'PASS', 'FAIL'}, 'action': {'ENTER', 'EXIT'}}


def timestamp(value):
    if isinstance(value, bool):
        raise ValueError('timestamp must be UTC ISO 8601 or epoch seconds')
    if isinstance(value, (int, float)):
        if not math.isfinite(value):
            raise ValueError('timestamp must be finite')
        parsed = datetime.fromtimestamp(value, timezone.utc)
    elif isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if parsed.tzinfo is None or parsed.utcoffset().total_seconds() != 0:
            raise ValueError('timestamp must include UTC timezone')
    else:
        raise ValueError('timestamp must be UTC ISO 8601 or epoch seconds')
    return parsed.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')


def validate_event(event):
    if not isinstance(event, dict):
        raise ValueError('event must be a JSON object')
    event = event.copy()
    kind = event.get('event_type')
    if not isinstance(kind, str) or kind not in REQUIRED:
        raise ValueError('unknown event_type')
    for key in ('event_id', 'line_id', 'event_time') + REQUIRED[kind]:
        if key not in event:
            raise ValueError(f'missing required field: {key}')
        value = event[key]
        if key.endswith('_time'):
            try:
                event[key] = timestamp(value)
            except (ValueError, TypeError, OverflowError, OSError) as exc:
                raise ValueError(f'invalid {key}: {exc}') from exc
        elif key.endswith('_inc'):
            if type(value) is not int or value < 0:
                raise ValueError(f'{key} must be a nonnegative integer')
        elif value is None and (key == 'reason_code' or (kind == 'production' and key == 'reject_reason')):
            continue
        elif not isinstance(value, str) or not value.strip():
            raise ValueError(f'{key} must be a nonempty string')
        if key in ENUMS and event[key] not in ENUMS[key]:
            raise ValueError(f'invalid {key}: {event[key]}')
    if kind == 'production':
        if event['good_count_inc'] != (1 if event['result'] == 'PASS' else 0):
            raise ValueError('good_count_inc must match the capsule result')
        if event['result'] == 'FAIL' and not event['reject_reason']:
            raise ValueError('failed production requires reject_reason')
    return event
