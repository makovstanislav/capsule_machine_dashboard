from datetime import datetime, timedelta, timezone
from itertools import count
import unittest
from unittest.mock import patch
import consumer
from intervals import hourly


class LineStateTest(unittest.TestCase):
    def setUp(self):
        for values in (consumer.previous_state, consumer.last_seen, consumer.last_event_time,
                       consumer.total_run_minutes):
            values.clear()

    def event(self, line, state, start, time, reason=None):
        return dict(event_id=f'{line}:{time}', line_id=line, event_type='line_state',
                    state=state, state_start_time=f'2026-01-01T{start}:00Z',
                    event_time=f'2026-01-01T{time}:00Z', reason_code=reason)

    def test_hours_and_window(self):
        start, end = '2026-01-01T08:50:00Z', '2026-01-01T09:10:00Z'
        self.assertEqual([x['duration_minutes'] for x in hourly(start, end)], [10, 10])
        clipped = list(hourly(start, end, '2026-01-01T08:55:00Z', '2026-01-01T09:05:00Z'))
        self.assertEqual(sum(x['duration_minutes'] for x in clipped), 10)
        self.assertEqual(list(hourly(start, end, end)), [])

    def test_lines_heartbeat_and_transition(self):
        records = {}
        def save(event, index):
            records[index, event['event_id']] = event.copy()
        with patch.object(consumer, 'append', side_effect=save), patch.object(consumer, 'upsert'):
            consumer.handle_line_state(self.event('A', 'DOWN', '08:50', '08:50', 'bad'))
            consumer.handle_line_state(self.event('B', 'RUN', '08:55', '09:00'))
            consumer.handle_line_state(self.event('A', 'DOWN', '08:50', '09:00', 'changed'))
            consumer.handle_line_state(self.event('A', 'RUN', '09:10', '09:10'))
        down = [value for (index, _), value in records.items()
                if index == 'state_intervals' and value['state'] == 'DOWN']
        self.assertEqual(len(down), 1)
        self.assertTrue(down[0]['closed'])
        self.assertEqual(down[0]['duration_minutes'], 20)
        self.assertEqual(down[0]['reason_code'], 'UNKNOWN_REASON')
        hours = [value for (index, _), value in records.items()
                 if index == 'state_hours' and value['state'] == 'DOWN']
        self.assertEqual([x['duration_minutes'] for x in hours], [10, 10])
        self.assertEqual(consumer.total_run_minutes['B'], 5)
        self.assertEqual(consumer.previous_state['B']['state'], 'RUN')

    def test_stale_one_line_and_recovery(self):
        with patch.object(consumer, 'append'), patch.object(consumer, 'upsert') as update:
            consumer.handle_line_state(self.event('A', 'RUN', '08:50', '09:00'))
            consumer.handle_line_state(self.event('B', 'RUN', '08:50', '09:01'))
            update.reset_mock()
            consumer.check_stale(datetime(2026, 1, 1, 9, 1, 1, tzinfo=timezone.utc))
            self.assertEqual(update.call_count, 1)
            self.assertEqual(update.call_args.args[0]['line_id'], 'A')
            self.assertEqual(update.call_args.args[0]['state'], 'STALE')
            self.assertEqual(consumer.previous_state['A']['state'], 'RUN')
            consumer.handle_line_state(self.event('A', 'RUN', '08:50', '09:02'))
            self.assertEqual(update.call_args.args[0]['state'], 'RUN')

    def test_busy_poll_still_checks_silent_line(self):
        start = datetime(2026, 1, 1, 9, tzinfo=timezone.utc)
        consumer.last_seen['A'] = start.isoformat()
        consumer.previous_state['A'] = dict(state='RUN', start_time=start.isoformat())
        consumer.previous_state['B'] = dict(state='RUN', start_time=start.isoformat())
        elapsed = [0]
        def process(_):
            elapsed[0] += 1
            consumer.last_seen['B'] = (start + timedelta(seconds=elapsed[0])).isoformat()
        stale_check = consumer.check_stale
        with patch.object(consumer, 'install_mappings'), \
             patch.object(consumer, 'KafkaConsumer') as factory, \
             patch.object(consumer, 'process_message', side_effect=process), \
             patch.object(consumer.time, 'monotonic', side_effect=count()), \
             patch.object(consumer, 'check_stale', side_effect=lambda: stale_check(start + timedelta(seconds=elapsed[0]))), \
             patch.object(consumer, 'upsert') as update:
            factory.return_value.poll.side_effect = [{'partition': [object()] * 35}, StopIteration]
            with self.assertRaises(StopIteration):
                consumer.main()
            self.assertTrue(update.called)
            self.assertTrue(all(call.args[0]['line_id'] == 'A' for call in update.call_args_list))
            self.assertEqual(update.call_args.args[0]['state'], 'STALE')
            factory.return_value.close.assert_called_once()
