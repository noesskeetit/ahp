"""ADK 2.14 session adapter for an explicitly task-bound AHP host.

The host supplies identity, thread, current lease and reviewed shared namespaces.
The adapter never chooses authority from native app/user strings. A bound service
owns one native session. Fleet-wide discovery/routing belongs to the host.
"""
from __future__ import annotations
import asyncio
import base64
import copy
import hashlib
import json
import time
from importlib.metadata import version

from google.adk.sessions.base_session_service import BaseSessionService, ListSessionsResponse
from google.adk.sessions.session import Session
from ahp_state.client import StateValidationError, StateConflictError
from ahp_state.shared import SharedValues

PROFILE = 'adk-2.14-owned-values/1'
COMMON_FORMAT = 'ahp-common-semantic-records/research-2'


def _fail(message):
    raise StateValidationError('INVALID_ARGUMENT', message)


def _native(value):
    raw = json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(',', ':')).encode()
    return {'json_base64': base64.b64encode(raw).decode()}


def _restore(record):
    try:
        return json.loads(base64.b64decode(record['attributes']['json_base64'], validate=True))
    except (KeyError, ValueError, TypeError):
        _fail('incompatible native ADK record')


class AHPSessionService(BaseSessionService):
    def __init__(self, client, *, thread_id, fence, app_name, user_id, session_id,
                 user_namespace, app_namespace, namespace='adk', expected_build='2.14.0'):
        if version('google-adk') != expected_build or expected_build != '2.14.0':
            _fail('ADK native build requires a reviewed adapter version')
        self.client, self.thread, self.fence = client, thread_id, dict(fence)
        self.app, self.user, self.sid, self.namespace = app_name, user_id, session_id, namespace
        self.user_values = SharedValues(client, user_namespace, 'subject', 'adk-user-state', binding=[app_name, user_id])
        self.app_values = SharedValues(client, app_namespace, 'deployment', 'adk-app-state', binding=[app_name])
        self.lock, self.cache, self.poison = asyncio.Lock(), None, None

    def _host(self, app, user, sid=None):
        if (app, user) != (self.app, self.user) or sid is not None and sid != self.sid:
            _fail('ADK identity differs from the trusted host binding')

    def _keys(self):
        return [self.user_values._key(self.user_values.key), self.app_values._key(self.app_values.key)]

    async def _bundle(self):
        if self.poison:
            raise self.poison
        bundle = await self.client.read_records_bundle(self.thread, self.namespace, self._keys(),
                                                      self.cache['receipt']['revision'] if self.cache else 0)
        if bundle.get('common_unchanged'):
            fields = ['cut_id', 'revision', 'namespace', 'run_id', 'record_count']
            if self.cache is None or any(bundle['receipt'].get(k) != self.cache['receipt'].get(k) for k in fields):
                _fail('ADK cached native revision differs from the State receipt')
            bundle['common'] = {'document': copy.deepcopy(self.cache['document']), 'receipt': bundle['receipt']}
        if bundle.get('common'):
            if bundle['common']['document']['profile'] != PROFILE:
                _fail('native ADK profile migration required')
            self.cache = copy.deepcopy(bundle['common'])
        elif self.cache:
            _fail('ADK common head disappeared')
        return bundle

    async def _session(self, bundle):
        read = bundle.get('common')
        if not read:
            return None
        doc = read['document']
        root = doc['records'].get('session')
        if doc['root'] != 'session' or root is None:
            _fail('invalid ADK root')
        native = _restore(root)
        ids = root['relations'].get('events')
        if not isinstance(ids, list) or len(set(ids)) != len(ids) or len(doc['records']) != len(ids) + 1:
            _fail('invalid ADK event sequence')
        native['events'] = []
        for i, key in enumerate(ids):
            row = doc['records'].get(key)
            if row is None or row.get('sequence') != i or row['kind'] != 'event':
                _fail('invalid ADK event position')
            native['events'].append(_restore(row))
        session = Session.model_validate(native)
        self._host(session.app_name, session.user_id, session.id)
        if any(k.startswith(('app:', 'user:', 'temp:')) for k in session.state):
            _fail('shared ADK data stored with the wrong owner')
        for storage, row, prefix in [(self.user_values, bundle['kv'][0], 'user:'), (self.app_values, bundle['kv'][1], 'app:')]:
            values, _ = await storage.read(row)
            session.state.update({prefix + k: v for k, v in values.items()})
        session._storage_update_marker = str(read['receipt']['revision'])
        return session

    async def _publish(self, native, changes, bundle):
        read = bundle.get('common')
        previous = read['document'] if read else None
        old_count = len(previous['records']['session']['relations']['events']) if previous else 0
        private = native.model_dump(mode='json')
        private['state'] = {k: v for k, v in private['state'].items() if not k.startswith(('app:', 'user:', 'temp:'))}
        events = private.pop('events')
        # Large shared values live outside the session. Events still preserve
        # their native deltas; the current inline recovery profile is bounded.
        records = {'session': {'kind': 'session', 'attributes': _native(private), 'relations': {}}}
        new_ids = []
        for i, event in enumerate(events[old_count:], old_count):
            key = 'event-' + hashlib.sha256(event['id'].encode()).hexdigest()
            new_ids.append(key)
            records[key] = {'kind': 'event', 'native_id': event['id'], 'sequence': i, 'attributes': _native(event), 'relations': {}}
        delta = {'format': COMMON_FORMAT, 'profile': PROFILE, 'root': 'session', 'put': records,
                 'relations': [{'source': 'session', 'role': 'events', 'mode': 'append' if previous else 'replace', 'expected_length': old_count, 'targets': new_ids}]}
        if not previous:
            delta['relations'][0].pop('expected_length')
        puts = []
        for storage, row, prefix in [(self.user_values, bundle['kv'][0], 'user:'), (self.app_values, bundle['kv'][1], 'app:')]:
            updates = {k[len(prefix):]: v for k, v in changes.items() if k.startswith(prefix)}
            if updates:
                puts.append(await storage.prepare(self.fence, updates, current=row))
        identity = ['event', events[-1]['id']] if events else ['create', self.sid]
        key = 'adk-' + hashlib.sha256(json.dumps(identity, separators=(',', ':')).encode()).hexdigest()
        try:
            receipt = await self.client.commit_records(self.fence, self.namespace, key,
                        read['receipt']['revision'] if read else 0, delta=delta, kv=puts)
        except BaseException as exc:
            # Unknown outcomes retain the exact request for host reconciliation;
            # no changed key can silently publish the native event twice.
            self.pending = {'fence': self.fence, 'namespace': self.namespace, 'key': key,
                            'expected_revision': read['receipt']['revision'] if read else 0, 'delta': delta, 'kv': puts}
            self.poison = exc
            raise
        self.cache = None
        native._storage_update_marker = str(receipt['revision'])

    async def create_session(self, *, app_name, user_id, state=None, session_id=None):
        self._host(app_name, user_id, session_id)
        async with self.lock:
            bundle = await self._bundle()
            if bundle.get('common'):
                _fail('ADK session already exists')
            native = Session(app_name=self.app, user_id=self.user, id=self.sid, state=copy.deepcopy(state or {}), last_update_time=time.time())
            await self._publish(native, native.state, bundle)
            return await self._session(await self._bundle())

    async def get_session(self, *, app_name, user_id, session_id, config=None):
        self._host(app_name, user_id, session_id)
        async with self.lock:
            native = await self._session(await self._bundle())
            if native and config:
                if config.after_timestamp is not None:
                    native.events = [e for e in native.events if e.timestamp >= config.after_timestamp]
                if config.num_recent_events is not None:
                    native.events = native.events[-config.num_recent_events:] if config.num_recent_events else []
            return native

    async def append_event(self, session, event):
        self._host(session.app_name, session.user_id, session.id)
        if event.partial:
            return event
        async with self.lock:
            bundle = await self._bundle()
            native = await self._session(bundle)
            if native is None:
                raise StateConflictError('CONFLICT', 'ADK session no longer exists')
            native.state.update({k: copy.deepcopy(v) for k, v in session.state.items() if k.startswith('temp:')})
            prior = next((e for e in native.events if e.id == event.id), None)
            if prior is not None:
                candidate = self._trim_temp_delta_state(event.model_copy(deep=True))
                if candidate.model_dump(mode='json') != prior.model_dump(mode='json'):
                    raise StateValidationError('KEY_REUSE_MISMATCH', 'native ADK event ID reused with different data')
                session.state, session.events = copy.deepcopy(native.state), copy.deepcopy(native.events)
                session.last_update_time = native.last_update_time
                session._storage_update_marker = native._storage_update_marker
                return prior.model_copy(deep=True)
            if session._storage_update_marker != native._storage_update_marker:
                raise StateConflictError('CONFLICT', 'ADK session revision changed')
            changes = copy.deepcopy(event.actions.state_delta if event.actions else {})
            result = await super().append_event(native, event.model_copy(deep=True))
            native.last_update_time = max(native.last_update_time, result.timestamp)
            await self._publish(native, changes, bundle)
            session.state, session.events = copy.deepcopy(native.state), copy.deepcopy(native.events)
            session.last_update_time = native.last_update_time
            session._storage_update_marker = native._storage_update_marker
            return result

    async def get_user_state(self, *, app_name, user_id):
        self._host(app_name, user_id)
        return (await self.user_values.read())[0]

    async def list_sessions(self, *, app_name, user_id=None):
        self._host(app_name, user_id if user_id is not None else self.user)
        user_id = self.user if user_id is None else user_id
        session = await self.get_session(app_name=app_name, user_id=user_id, session_id=self.sid)
        return ListSessionsResponse(sessions=[] if session is None else [session.model_copy(update={'events': [], 'state': {}})])

    async def delete_session(self, *, app_name, user_id, session_id):
        self._host(app_name, user_id, session_id)
        # This host binds exactly one native session to its platform thread.
        await self.client.delete_thread(self.thread)
        self.cache = None

    async def flush(self):
        if self.poison:
            raise self.poison
