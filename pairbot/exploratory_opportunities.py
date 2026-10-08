"""Descriptive visible-book opportunities. No orders, fills or protocol acceptance."""
import argparse
from collections import Counter, defaultdict
import hashlib
import json
import math
from pathlib import Path
from .config import Config
from .engine import Engine
from .feed import read_entries
from .research import BoundedMeasurements, input_hash, research_apply
from .research_stats import FeeSchedule

KINDS=('taker_pair','mint_sell')


def walk(book, quantity, fee, sell):
    levels=reversed(book.bids.items()) if sell else iter(book.asks.items())
    left=quantity
    value=fees=0.
    used=[]
    for price,size in levels:
        take=min(size,left)
        value+=price*take
        cost=fee.fee(take,price)
        fees+=cost
        used.append({'price':price,'quantity':take,'fee':cost})
        left-=take
        if left<=1e-9:
            return {'value':value,'fees':fees,'levels':used}
    return None


def visible_candidate(engine, fee, kind, spec, reported_touch=None):
    if not engine.active or fee is None:
        return None
    m=engine.markets[engine.active]
    if not m.start<=engine.now<m.end or any(t not in engine.received or t in engine.resync_tokens for t in m.tokens):
        return None
    sell=kind=='mint_sell'
    levels=[engine.books[t].best_bid() if sell else engine.books[t].best_ask() for t in m.tokens]
    if any(x is None or not 0<x.price<1 for x in levels):
        return None
    q=spec['quantity_shares']
    prices=[x.price for x in levels]
    coherent=True
    touch_matches=True
    for token in m.tokens:
        book=engine.books[token]
        bid,ask=book.best_bid(),book.best_ask()
        coherent &= bid is not None and ask is not None and bid.price<=ask.price
        if reported_touch and token in reported_touch:
            reported=reported_touch[token]
            touch_matches &= bid is not None and ask is not None and bid.price==reported['bid'] and ask.price==reported['ask']
    fees=sum(fee.fee(q,p) for p in prices)
    gross=q*(sum(prices)-1 if sell else 1-sum(prices))
    edge=(gross-fees)/q
    walked=[walk(engine.books[t],q,fee,sell) for t in m.tokens]
    walked_net=None
    if all(x is not None for x in walked):
        value=sum(x['value'] for x in walked)
        walked_net=(value-q if sell else q-value)-sum(x['fees'] for x in walked)
    return {'kind':kind,'market':m.condition_id,'slug':m.slug,'market_start':m.start,
            'top_prices':prices,'top_depths':[x.size for x in levels],
            'requested_quantity':q,'taker_fees_at_top':fees,
            'quote_edge_after_taker_fees':edge,'quote_qualifies':edge>spec['edge_per_share'],
            'individual_books_coherent':coherent,'reported_touch_matches_reconstruction':touch_matches,
            'source_reported_top_quotes':reported_touch,
            'top_depth_minimum':min(x.size for x in levels),'walked_legs':walked,
            'walked_net_before_operation_cost':walked_net,
            'walked_depth_and_edge_qualifies':walked_net is not None and walked_net>q*spec['edge_per_share'],
            'receipt_lag':max(0.,*(engine.received[t]-engine.exchange_ts[t] for t in m.tokens)),
            'oldest_book_exchange_ts':min(engine.exchange_ts[t] for t in m.tokens),
            'operation_cost':None,'net_after_all_costs':None,'fee_provenance':fee.provenance}


def empty_stat():
    return {'observed_seconds':0.,'book_evaluable_seconds':0.,'quote_opportunity_seconds':0.,
            'depth_and_edge_seconds':0.,'coherent_depth_and_edge_seconds':0.,'frames':0,'events_started':0}


class Cohort:
    def __init__(self,name,spec):
        self.name=name
        self.spec=spec
        self.totals={k:empty_stat() for k in KINDS}
        self.minutes=defaultdict(lambda:{k:empty_stat() for k in KINDS})
        self.active={}
        self.events=[]

    def close(self,kind):
        event=self.active.pop(kind,None)
        if event is None:
            return
        event['duration_seconds']=event['end']-event['start']
        event['required_duration_seconds']=self.spec['minimum_persistence_seconds']+event['max_receipt_lag_seconds']
        event['survives_recorded_latency']=event['duration_seconds']>event['required_duration_seconds']
        event['depth_edge_and_latency_bound']=event['all_intervals_depth_and_edge'] and event['survives_recorded_latency']
        event['coherent_depth_edge_and_latency_bound']=event['depth_edge_and_latency_bound'] and event['all_intervals_book_coherent_and_touch_matches']
        event['verified_net_positive']=False
        self.events.append(event)

    def segment(self,a,b,market,candidates):
        key=f"{market['slug']}:minute_{int((a-market['start'])//60)}"
        for kind in KINDS:
            stat=self.minutes[key][kind]
            total=self.totals[kind]
            for x in (stat,total):x['observed_seconds']+=b-a
            row=candidates.get(kind)
            if row is not None:
                for x in (stat,total):x['book_evaluable_seconds']+=b-a
            if row is None or not row['quote_qualifies']:
                self.close(kind)
                continue
            for x in (stat,total):
                x['quote_opportunity_seconds']+=b-a
                if row['walked_depth_and_edge_qualifies']:x['depth_and_edge_seconds']+=b-a
                if row['walked_depth_and_edge_qualifies'] and row['individual_books_coherent'] and row['reported_touch_matches_reconstruction']:
                    x['coherent_depth_and_edge_seconds']+=b-a
            event=self.active.get(kind)
            if event is not None and (event['market']!=market['condition_id'] or event['end']!=a):
                self.close(kind)
                event=None
            if event is None:
                event={**row,'cohort':self.name,'start':a,'end':a,
                       'minimum_top_depth':row['top_depth_minimum'],
                       'minimum_quote_edge':row['quote_edge_after_taker_fees'],
                       'minimum_walked_net_before_operation_cost':row['walked_net_before_operation_cost'],
                       'maximum_walked_net_before_operation_cost':row['walked_net_before_operation_cost'],
                       'max_receipt_lag_seconds':row['receipt_lag'],
                       'maximum_book_age_seconds':0.,'all_intervals_depth_and_edge':True,
                       'all_intervals_book_coherent_and_touch_matches':True}
                self.active[kind]=event
                for x in (stat,total):x['events_started']+=1
            event['end']=b
            event['minimum_top_depth']=min(event['minimum_top_depth'],row['top_depth_minimum'])
            event['minimum_quote_edge']=min(event['minimum_quote_edge'],row['quote_edge_after_taker_fees'])
            event['max_receipt_lag_seconds']=max(event['max_receipt_lag_seconds'],row['receipt_lag'])
            event['maximum_book_age_seconds']=max(event['maximum_book_age_seconds'],b-row['oldest_book_exchange_ts'])
            event['all_intervals_depth_and_edge'] &= row['walked_depth_and_edge_qualifies']
            event['all_intervals_book_coherent_and_touch_matches'] &= row['individual_books_coherent'] and row['reported_touch_matches_reconstruction']
            v=row['walked_net_before_operation_cost']
            if v is None:
                event['minimum_walked_net_before_operation_cost']=None
            elif event['minimum_walked_net_before_operation_cost'] is not None:
                event['minimum_walked_net_before_operation_cost']=min(event['minimum_walked_net_before_operation_cost'],v)
            if v is not None:
                old=event['maximum_walked_net_before_operation_cost']
                event['maximum_walked_net_before_operation_cost']=v if old is None else max(old,v)

    def finish(self):
        for kind in KINDS:self.close(kind)
        return {'cohort':self.name,'totals':self.totals,'per_market_minute':dict(self.minutes),
                'events':self.events,'durable_depth_bounds':sum(e['depth_edge_and_latency_bound'] for e in self.events),
                'verified_net_positive_events':0,'net_after_all_costs':None}


def analyze(source,policy_path,quality_path):
    policy_bytes=Path(policy_path).read_bytes()
    policy=json.loads(policy_bytes)
    quality=json.loads(Path(quality_path).read_text())
    digest=input_hash(source)
    if quality['input_sha256']!=digest or quality['policy_sha256']!=hashlib.sha256(policy_bytes).hexdigest():
        raise ValueError('Quality mask does not match recording and frozen policy')
    spec=policy['opportunities']
    full,clean=Cohort('full',spec),Cohort('clean',spec)
    clean_bins=set(quality['quality']['clean_bins'])
    bin_size=policy['clean_window']['bin_seconds']
    origin=quality['start']
    engine=Engine(Config())
    engine.record_only=True
    engine.measurements=BoundedMeasurements()
    markets={}
    fee=None
    fee_rows=[]
    missing_fee=Counter()
    previous=origin
    current=None
    candidates={}
    reported_touch={}
    frame_count=Counter()

    def is_clean(t):
        return int((t-origin)//bin_size) in clean_bins

    for row in read_entries(source):
        ts=row['ts']
        if current is not None and ts>previous:
            m=markets[current]
            a=max(previous,m['start'])
            stop=min(ts,m['end'],quality['end'])
            while a<stop:
                minute_boundary=m['start']+(int((a-m['start'])//60)+1)*60
                bin_boundary=origin+(int((a-origin)//bin_size)+1)*bin_size
                b=min(stop,minute_boundary,bin_boundary)
                full.segment(a,b,m,candidates)
                if is_clean((a+b)/2):
                    clean.segment(a,b,m,candidates)
                else:
                    for kind in KINDS:clean.close(kind)
                a=b
        previous=ts
        kind=row['type']
        if kind=='market':
            for cohort in (full,clean):
                for k in KINDS:cohort.close(k)
            current=row['market']['condition_id']
            markets[current]=row['market']
            fee=None
            reported_touch={}
        elif kind=='metadata':
            # Every connection needs new books. Never carry prices across resubscription.
            engine.received.clear()
            engine.exchange_ts.clear()
            reported_touch={}
            try:
                fee=FeeSchedule.from_gamma(row['raw'])
                fee_rows.append({'market':current,'ts':ts,'feeSchedule':row['raw'].get('feeSchedule'),
                                 'feesEnabled':row['raw'].get('feesEnabled'),'source':fee.provenance})
            except (ValueError,KeyError,TypeError):
                fee=None
                missing_fee[current]+=1
        research_apply(engine,row)
        engine.audit.clear()
        if kind=='frame' and engine.active:
            msg=row['message']
            if msg.get('event_type')=='price_change':
                for change in msg.get('price_changes',[]):
                    token=change.get('asset_id')
                    if token in engine.received and 'best_bid' in change and 'best_ask' in change:
                        reported_touch[token]={'bid':float(change['best_bid']),'ask':float(change['best_ask'])}
            elif msg.get('event_type')=='book':
                reported_touch.pop(msg.get('asset_id'),None)
        if kind=='frame' and current is not None:
            m=markets[current]
            key=f"{m['slug']}:minute_{int((row.get('received_at',ts)-m['start'])//60)}"
            frame_count[current]+=1
            for cohort in (full,clean):
                if cohort is full or is_clean(row.get('received_at',ts)):
                    for k in KINDS:
                        cohort.totals[k]['frames']+=1
                        cohort.minutes[key][k]['frames']+=1
        candidates={k:visible_candidate(engine,fee,k,spec,dict(reported_touch)) for k in KINDS}
        if kind in ('gap','stop'):
            candidates={}
            reported_touch={}
            for cohort in (full,clean):
                for k in KINDS:cohort.close(k)
    results=[full.finish(),clean.finish()]
    if engine.fills or engine.merges or engine.orders:
        raise RuntimeError('Descriptive analysis must never execute an order or fill')
    if input_hash(source)!=digest:
        raise RuntimeError('Recording changed during analysis')
    return {'scope':'EXPLORATORY_VISIBLE_BOOK_ONLY','input_sha256':digest,
            'freeze_commit':quality['freeze_commit'],'policy_sha256':quality['policy_sha256'],
            'threshold_sets_tried':1,'quality':quality['quality'],'cohorts':results,
            'session_seconds':quality['end']-quality['start'],
            'market_context_seconds':full.totals['taker_pair']['observed_seconds'],
            'unassigned_or_market_rollover_seconds':quality['end']-quality['start']-full.totals['taker_pair']['observed_seconds'],
            'markets':list(markets.values()),'frames_per_market':dict(frame_count),
            'fee_observations':fee_rows,'missing_fee_observations':dict(missing_fee),
            'operation_cost_status':'UNKNOWN_NO_ZERO_ASSUMPTION','net_pnl':None,'fills':0,'orders':0,
            'acceptance_protocol_evaluated':False,'old_holdout_opened':False,
            'decision':'NO-GO','quality_gate_failures':quality['numeric_gate_failures'],
            'cost_gate_failure':'VERIFIED_OPERATION_COST_AND_NET_POSITIVE_OPPORTUNITIES_MISSING',
            'full_cohort_warning':'All recorded book states including stale states until explicit gaps; descriptive upper bounds only. Clean cohort uses frozen mask. No inferred state across gaps.',
            'sources':['https://docs.polymarket.com/trading/fees','https://docs.polymarket.com/market-data/market-details','https://docs.polymarket.com/trading/positions/manage']}


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('source');p.add_argument('--policy',required=True);p.add_argument('--quality',required=True);p.add_argument('--out',required=True)
    a=p.parse_args();result=analyze(a.source,a.policy,a.quality)
    with Path(a.out).open('x') as f:json.dump(result,f,indent=2,allow_nan=False)
    print(json.dumps([{'cohort':c['cohort'],'totals':c['totals'],'events':len(c['events']),'durable_depth_bounds':c['durable_depth_bounds']} for c in result['cohorts']],indent=2))


if __name__=='__main__':main()
