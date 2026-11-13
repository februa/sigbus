"""Narrow error translation and metadata checks; ordinary I/O errors propagate."""
from contextlib import contextmanager
import json
import math
import numpy as np

from ..errors import StreamCorruptedError, StreamPermissionError
from ..policy import AckSettings


@contextmanager
def permissions(name, operation):
    try:
        yield
    except StreamPermissionError:
        raise
    except PermissionError as exc:
        raise StreamPermissionError(f"Stream {name!r}: {operation}: permission denied") from exc


def read_metadata(path, name, operation):
    try:
        state = json.loads(path.read_text())
        if not isinstance(state, dict):
            raise ValueError("metadata must be an object")
        return state
    except (ValueError, UnicodeError) as exc:
        raise StreamCorruptedError(f"Stream {name!r}: {operation}: invalid metadata") from exc


def check_metadata(state, name, operation, *, legacy=False):
    try:
        if not isinstance(state['endpoints'], dict):
            raise ValueError('invalid endpoints')
        if not isinstance(state['kind'], str) or not isinstance(state['identity'], str):
            raise ValueError('invalid identity or kind')
        if state['spec']['name'] != name:
            raise ValueError('stream name does not match metadata')
        if not isinstance(state['slots'], list) or not state['slots']:
            raise ValueError('invalid slots')
        if legacy or state['version'] != 7:
            return
        spec = state['spec']
        if spec['kind'] not in ('stream', 'state'):
            raise ValueError('invalid data kind')
        if not isinstance(spec['shape'], list) or any(not isinstance(n, int) or n <= 0 for n in spec['shape']):
            raise ValueError('invalid shape')
        dtype = np.lib.format.descr_to_dtype(spec['dtype'])
        if dtype.hasobject or dtype.itemsize <= 0 or spec['layout'] != 'row_major':
            raise ValueError('invalid dtype or layout')
        delivery = state['delivery']
        if delivery is not None:
            if delivery['reuse'] not in ('fifo', 'available', 'all_readers', 'sync', 'state'):
                raise ValueError('invalid delivery')
            if delivery['no_readers'] not in ('hold', 'wait', 'proceed'):
                raise ValueError('invalid reader rule')
            if delivery['overflow'] not in ('drop_oldest', 'block'):
                raise ValueError('invalid overflow rule')
            if delivery['reuse'] == 'sync' and (not isinstance(delivery['group'], str) or not delivery['group']):
                raise ValueError('invalid sync group')
        batch = spec['batch_size']
        if not isinstance(batch, int) or batch < 1:
            raise ValueError('invalid batch size')
        if spec['kind'] == 'state' and batch != 1:
            raise ValueError('states cannot batch')
        if state['latest'] is not None and (not isinstance(state['latest'], int) or
                                           not 0 <= state['latest'] < len(state['slots'])):
            raise ValueError('invalid latest slot')
        if not isinstance(state['generation'], int) or state['generation'] < 0:
            raise ValueError('invalid generation')
        for endpoint in state['endpoints'].values():
            if not isinstance(endpoint['waiting_write'], bool):
                raise ValueError('invalid writer intent')
            if (spec['kind'] == 'state') != (endpoint['reuse'] == 'state'):
                raise ValueError('endpoint kind mismatch')
            if endpoint['reuse'] not in ('fifo', 'available', 'all_readers', 'sync', 'state'):
                raise ValueError('invalid endpoint policy')
            if endpoint['reuse'] == 'sync' and (not isinstance(endpoint['group'], str) or not endpoint['group']):
                raise ValueError('invalid participant group')
            if endpoint['status'] not in ('joined', 'excluded', 'timed_out'):
                raise ValueError('invalid participant status')
            if endpoint['ack'] is not None:
                AckSettings(**endpoint['ack'])
            if not math.isfinite(endpoint['last_ack']):
                raise ValueError('invalid ACK time')
            if endpoint['mode'] not in ('read', 'write') or not isinstance(endpoint['active'], bool):
                raise ValueError('invalid endpoint')
            if not isinstance(endpoint['cursor'], int):
                raise ValueError('invalid cursor')
            if endpoint['buffer'] is not None and not 0 <= endpoint['buffer'] < len(state['slots']):
                raise ValueError('invalid buffer index')
        for slot in state['slots']:
            if not isinstance(slot['count'], int) or not 0 <= slot['count'] <= batch or len(slot['timestamps']) != slot['count']:
                raise ValueError('invalid valid length')
            if not all(isinstance(x, int) for x in slot['timestamps']):
                raise ValueError('invalid timestamps')
            if not isinstance(slot['barrier'], list) or any(x not in state['endpoints'] for x in slot['barrier']):
                raise ValueError('invalid barrier')
            if not isinstance(slot['readers'], list) or not isinstance(slot['pending'], list):
                raise ValueError('invalid reader registration')
            if not isinstance(slot['progress'], dict) or not isinstance(slot['generation'], int):
                raise ValueError('invalid reader progress')
            if not isinstance(slot['occupied'], bool):
                raise ValueError('invalid slot state')
            if slot['writer'] is not None and slot['writer'] not in state['endpoints']:
                raise ValueError('unknown writer')
            if any(x not in state['endpoints'] for x in slot['readers'] + slot['pending']):
                raise ValueError('unknown reader')
            if any(not isinstance(x, int) or not 0 <= x <= slot['count'] for x in slot['progress'].values()):
                raise ValueError('invalid progress index')
    except (KeyError, TypeError, ValueError, IndexError) as exc:
        raise StreamCorruptedError(f"Stream {name!r}: {operation}: malformed stream state") from exc
