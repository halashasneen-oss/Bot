"""Public BTC stream watchdog. Transport recovery never fabricates reference ticks."""
import asyncio
import hashlib
import json
import math
import time
from pathlib import Path

POLICY_SHA = '738381152c95f9e89e7cfbb139c1252270438dd1834bb64e173dc0ffb58ab8a7'
FREEZE_COMMIT = '7bdb41720ed71201d308903ba2cf474184a774bd'
TOPICS = ('crypto_prices_chainlink', 'crypto_prices_twap_sixty')


def load_transport_policy():
    raw=(Path(__file__).resolve().parents[1]/'docs/unified/transport-policy.json').read_bytes()
    if hashlib.sha256(raw).hexdigest()!=POLICY_SHA:raise ValueError('Transport policy changed')
    return json.loads(raw)


class SilentReference(Exception):
    pass


class Progress:
    def __init__(self,opened):
        self.last={topic:opened for topic in TOPICS}
        self.timestamps={topic:None for topic in TOPICS}
    def observe(self,message,received,mono):
        if not isinstance(message,dict):return False
        topic=message.get('topic');p=message.get('payload')
        if topic not in TOPICS or message.get('type')!='update' or not isinstance(p,dict) or p.get('symbol')!='btc/usd':return False
        if topic==TOPICS[1] and p.get('window_s')!=60:return False
        try:
            t=float(p['timestamp'])/1000;v=float(p['value'])
            if not math.isfinite(t+v) or v<=0 or not received-10<=t<=received+2:return False
        except (TypeError,KeyError,ValueError):return False
        previous=self.timestamps[topic]
        if previous is not None and t<=previous:return False
        self.timestamps[topic]=t;self.last[topic]=mono;return True
    def silent(self,mono,timeout):return [t for t in TOPICS if mono-self.last[t]>=timeout]


async def consume(ws,journal,ingest,policy):
    """Monitor both advancing BTC topics, even when other symbols/PONG keep flowing."""
    state=Progress(time.monotonic())
    await ws.send(json.dumps({'action':'subscribe','subscriptions':[{'topic':t,'type':'update'} for t in TOPICS]}))
    journal.write({'type':'feed_subscription','at':time.time(),'topics':list(TOPICS),'transport_policy_sha256':POLICY_SHA})
    async def heartbeat():
        while True:
            await asyncio.sleep(policy['application_ping_seconds']);await ws.send('PING')
    task=asyncio.create_task(heartbeat())
    try:
        while True:
            if task.done():task.result();raise RuntimeError('Heartbeat stopped')
            silent=state.silent(time.monotonic(),policy['btc_progress_timeout_seconds'])
            if silent:
                journal.write({'type':'feed_silent','at':time.time(),'topics':silent,'last_advancing_exchange_at':state.timestamps,'last_progress_age_seconds':{t:time.monotonic()-state.last[t] for t in TOPICS}})
                raise SilentReference('BTC update progression stopped')
            try:raw=await asyncio.wait_for(ws.recv(),policy['receive_check_seconds'])
            except asyncio.TimeoutError:continue
            received=time.time();mono=time.monotonic()
            try:m=json.loads(raw)
            except (ValueError,TypeError):continue
            journal.write({'type':'feed_message','received_at':received,'received_monotonic':mono,'data':m})
            if isinstance(m,dict) and any(w in str(m).lower() for w in ('unauthorized','authentication required','invalid api key')):
                journal.write({'type':'feed_blocked_auth','at':received});return 'AUTH_REQUIRED'
            state.observe(m,received,mono);ingest(m,received)
    finally:
        task.cancel();await asyncio.gather(task,return_exceptions=True)


async def wait_for_reference(feed,start,clock_fn,policy):
    """Wait only inside the EXISTING fixed decision tolerance for a fresh live tick."""
    waited=False
    while True:
        corrected,width=clock_fn()
        if abs(corrected-(start+policy['decision_age_seconds']))+width>policy['decision_tolerance_seconds']:
            raise ValueError('MISSED_FIXED_DECISION_WINDOW')
        try:return feed.features(start,corrected,policy),corrected
        except ValueError as exc:
            if str(exc)!='STALE_UNDERLYING_REFERENCE':raise
            if not waited:
                feed.journal.write({'type':'reference_wait','start':start,'at':time.time(),'reason':str(exc),'deadline':start+policy['decision_age_seconds']+policy['decision_tolerance_seconds']-width});waited=True
            if corrected+.05+width>=start+policy['decision_age_seconds']+policy['decision_tolerance_seconds']:raise
            await asyncio.sleep(.05)
