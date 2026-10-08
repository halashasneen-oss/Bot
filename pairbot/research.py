"""Offline frozen research. All strategies share the same data and execution tool."""
import argparse
from bisect import bisect_left
from collections import Counter, defaultdict, deque
import gzip
import hashlib
import json
from pathlib import Path
from .config import Config
from .engine import Engine
from .feed import apply, read_entries
from .measure import Measurements
from .research_engine import ResearchEngine
from .research_stats import (FeeSchedule, acceptance, bootstrap_ci, calibration,
    chronological_split, claim_holdout, load_protocol, path_integral,
    realized_volatility, up_probability, upper_opportunity, window_scores)


class BoundedMeasurements(Measurements):
    def frame(self,*args):
        pass
    def trade(self,*args):
        pass
    def depth(self,*args):
        pass


def input_hash(source):
    path=Path(source)
    digest=hashlib.sha256()
    if path.is_dir():
        manifest=json.loads((path/'journal.json').read_text())
        if manifest.get('active_shard'):
            raise ValueError('Cannot evaluate an active/incomplete recording')
        files=[]
        for name in manifest['shards']:
            file=path/name
            if file.parent!=path or file.name!=name:
                raise ValueError('Unsafe shard name')
            files.append(file)
            digest.update(name.encode())
        files.append(path/'journal.json')
    else:
        files=[path]
    for file in files:
        with file.open('rb') as f:
            for chunk in iter(lambda:f.read(1024*1024),b''):
                digest.update(chunk)
    return digest.hexdigest()


def market_index(source,complete_only=False):
    windows={}
    start=end=None
    for row in read_entries(source):
        if row['type']=='research_header':
            start=row['ts']
        elif row['type']=='stop' and row.get('reason')=='record_only_finished':
            end=row['ts']
        elif row['type']=='market':
            windows[row['market']['condition_id']]=row['market']
    ordered=sorted(windows.values(),key=lambda m:(m['start'],m['condition_id']))
    if complete_only:
        if start is None or end is None:
            raise ValueError('Completed research recording required; older data needs --exploratory')
        ordered=[m for m in ordered if start<=m['start'] and m['end']<=end]
    return ordered


def selected_entries(source,selected):
    active=None
    for row in read_entries(source):
        kind=row['type']
        if kind=='market':
            active=row['market']['condition_id']
        if kind=='settlement':
            if row['condition_id'] in selected:
                yield row
        elif (kind in ('research_header','header')
              or (kind=='source_error' and row.get('source') in ('Binance','clock'))
              or active in selected
              or (active is None and kind in ('source_error','clock','gap'))):
            yield row


def research_apply(engine,row):
    if row['type'] in ('market','frame','gap','settlement','stop','tick','status','header','error'):
        apply(engine,row)
    elif row['type'] not in ('research_header','clock','binance','metadata','source_error','socket_control','socket_lifecycle'):
        raise ValueError('Unknown research event: '+row['type'])


class Latencies:
    def __init__(self):
        self.hist=defaultdict(Counter)
        self.minutes=defaultdict(lambda:defaultdict(lambda:[0,0.]))
    def add(self,kind,received,exchange):
        if exchange is None or exchange<=0:
            return
        value=received-exchange
        self.hist[kind][round(value,2)]+=1
        item=self.minutes[kind][int(received//60)*60]
        item[0]+=1
        item[1]+=value
    def report(self):
        result={}
        for kind,hist in self.hist.items():
            n=sum(hist.values())
            def quantile(q):
                count=0
                for value,size in sorted(hist.items()):
                    count+=size
                    if count>(n-1)*q:
                        return value
            result[kind]={'n':n,'p50':quantile(.5),'p99':quantile(.99),'bin_seconds':.01,
                          'by_minute':[{'minute':t,'n':n,'mean':total/n}
                          for t,(n,total) in sorted(self.minutes[kind].items())]}
        return result


class Opportunities:
    def __init__(self):
        self.open={}
        self.events=[]
        self.fresh_seconds=0.
        self.last_ts=None
        self.last_expiry=0.
        self.last_window_start=0.
        self.last_eligible_expiry={'taker_pair':0.,'mint_sell':0.}
        self.eligible_seconds=Counter()
    def update(self,engine,fee,edge=.02,operation_cost=None,split_cost=None):
        now=engine.now
        if self.last_ts is not None:
            start=max(self.last_ts,self.last_window_start)
            self.fresh_seconds+=max(0,min(now,self.last_expiry)-start)
            for kind,expiry in self.last_eligible_expiry.items():
                self.eligible_seconds[kind]+=max(0,min(now,expiry)-start)
        self.last_ts=now
        self.last_expiry=0.
        self.last_eligible_expiry={'taker_pair':0.,'mint_sell':0.}
        candidates={}
        if engine.active and engine.fresh() and fee:
            m=engine.markets[engine.active]
            self.last_window_start=m.start
            self.last_expiry=min(m.end,*(engine.received[t]+engine.cfg.stale_seconds for t in m.tokens),
                                *(engine.exchange_ts[t]+engine.cfg.stale_seconds for t in m.tokens))
            for sell,kind in ((False,'taker_pair'),(True,'mint_sell')):
                if not m.start<=now<m.end:
                    continue
                levels=[engine.books[t].best_bid() if sell else engine.books[t].best_ask() for t in m.tokens]
                if any(x is None for x in levels):
                    continue
                if all(0<x.price<1 and x.size>0 for x in levels):
                    self.last_eligible_expiry[kind]=self.last_expiry
                row=upper_opportunity(kind,[x.price for x in levels],[x.size for x in levels],
                    fee,edge,5,split_cost if sell else operation_cost)
                if row and row['qualifies']:
                    lag=max(0,*(engine.received[t]-engine.exchange_ts[t] for t in m.tokens))
                    candidates[kind]={**row,'window':engine.active,'delay':lag,'minimum_survival_seconds':.5+lag}
        for kind,event in list(self.open.items()):
            candidate=candidates.get(kind)
            if not candidate or candidate['window']!=event['window'] or now>event['fresh_expiry']:
                self.close(kind,min(now,event['fresh_expiry']))
        for kind,row in candidates.items():
            if kind not in self.open:
                self.open[kind]={**row,'start':now,'fresh_expiry':self.last_expiry,
                                 'minimum_depth':row['available_depth']}
            else:
                event=self.open[kind]
                event['fresh_expiry']=self.last_expiry
                event['minimum_depth']=min(event['minimum_depth'],row['available_depth'])
                event['quantity']=min(event['quantity'],row['quantity'])
                event['minimum_net_usd_bound']=min(event.get('minimum_net_usd_bound',event['net_usd_bound']),row['net_usd_bound'])
                event['maximum_net_usd_bound']=max(event.get('maximum_net_usd_bound',event['net_usd_bound']),row['net_usd_bound'])
                event['minimum_survival_seconds']=max(event['minimum_survival_seconds'],row['minimum_survival_seconds'])
    def close(self,kind,end):
        event=self.open.pop(kind)
        event['end']=max(event['start'],end)
        event['duration_seconds']=event['end']-event['start']
        event['survives_latency']=event['duration_seconds']>=event['minimum_survival_seconds']
        event['mint_latency_assumption']='pre_split_inventory' if kind=='mint_sell' else None
        self.events.append(event)
    def finish(self,now):
        for kind,event in list(self.open.items()):
            self.close(kind,min(now,event['fresh_expiry']))
    def report(self):
        rows={}
        for kind in ('taker_pair','mint_sell'):
            events=[x for x in self.events if x['kind']==kind]
            rows[kind]={'intervals':len(events),'eligible_top_depth_seconds':self.eligible_seconds[kind],
                        'opportunity_seconds':sum(x['duration_seconds'] for x in events),
                        'surviving_intervals':sum(x['survives_latency'] for x in events),
                        'surviving_seconds':sum(x['duration_seconds'] for x in events if x['survives_latency']),
                        'largest_visible_net_usd_bound':max((x['net_usd_bound'] for x in events),default=None)}
        return {'fresh_seconds_with_fee_metadata':self.fresh_seconds,'kinds':rows}


def model_feature(engine,metadata,btc,scale):
    if not engine.active or not engine.fresh() or not btc or not 0<=engine.now-btc[-1][0]<=5:
        return None
    ref=metadata.get(engine.active,{}).get('reference',{})
    config=ref.get('config') or {}
    if ref.get('status')!='OBSERVED_OFFICIAL_FIELD' or ref.get('opening_price') is None:
        return None
    if config.get('twapEnabled') is not True or config.get('twapLookbackSeconds')!=60:
        return None
    sigma=realized_volatility(btc,engine.now)
    if sigma is None:
        return None
    m=engine.markets[engine.active]
    left=max(0,m.end-engine.now)
    known=path_integral(btc,m.end-60,engine.now) if left<60 else None
    return up_probability(btc[-1][1],ref['opening_price'],left,sigma,scale,known_integral=known)


def load_auxiliary(directory):
    if directory is None:
        return [],None,{}
    directory=Path(directory)
    manifest=json.loads((directory/'manifest.json').read_text())
    rows=[]
    digest=hashlib.sha256()
    for entry in manifest['files']:
        file=directory/Path(entry['path']).name
        body=file.read_bytes()
        git_sha=hashlib.sha1(b'blob '+str(len(body)).encode()+b'\0'+body).hexdigest()
        if git_sha!=entry['sha']:
            raise ValueError('Auxiliary BTC file hash mismatch')
        digest.update(file.name.encode())
        digest.update(body)
        for line in body.decode().splitlines():
            raw=json.loads(line)
            if raw['type']=='binance_aux':
                rows.append({'type':'binance','ts':raw['received_at'],'received_at':raw['received_at'],
                    'message':{'p':raw['message']['price'],'T':raw['message']['time']},
                    'source':'NATIVE_PUBLIC_POLLED_LATEST_TRADE_NOT_SOCKET'})
            elif raw['type']=='source_error':
                rows.append(raw)
            else:
                raise ValueError('Unknown auxiliary event')
    rows.sort(key=lambda r:r['ts'])
    times=[r['ts'] for r in rows if r['type']=='binance']
    report={**manifest['summary'],'largest_sample_gap_seconds':
            max((b-a for a,b in zip(times,times[1:])),default=None),
            'clock_domain':'CONNECTOR_HOST_UTC; cross-host alignment not independently proven'}
    return rows,digest.hexdigest(),report


def merged_entries(source,selected,auxiliary):
    if not auxiliary:
        yield from selected_entries(source,selected)
        return
    windows=[w for w in market_index(source) if w['condition_id'] in selected]
    allowed=[r for r in auxiliary if any(w['start']-180<=r['ts']<=w['end'] for w in windows)]
    cursor=iter(allowed)
    pending=next(cursor,None)
    for row in selected_entries(source,selected):
        while pending is not None and pending['ts']<=row['ts']:
            yield pending
            pending=next(cursor,None)
        yield row


def scan(source,selected,protocol,*,predict_scale=None,strategies=False,auxiliary=()):
    probe=Engine(Config())
    probe.record_only=True
    probe.measurements=BoundedMeasurements()
    metadata={}
    btc=deque()
    winners={}
    samples=defaultdict(list)
    timelines=defaultdict(list)
    latencies=Latencies()
    opportunities=Opportunities()
    clocks=[]
    errors=Counter()
    counts=Counter()
    fee=None
    engines={}
    audits=defaultdict(list)
    window_pnl=defaultdict(dict)
    if strategies:
        for spec in protocol['strategy_grid']:
            if spec['kind'] not in ('baseline','taker_opportunity') and predict_scale is None:
                continue
            for bound in ('conservative','optimistic_upper'):
                e=ResearchEngine(spec,protocol,bound)
                e.measurements=BoundedMeasurements()
                engines[(spec['id'],bound)]=e
    last_sample={}
    availability=defaultdict(Counter)
    last_availability={}
    cache=(None,0.,None)
    final_now=0.
    for row in merged_entries(source,selected,auxiliary):
        kind=row['type']
        counts[kind]+=1
        final_now=max(final_now,row['ts'])
        if kind=='clock':
            clocks.append(row)
        elif kind=='source_error':
            errors[row.get('source','unknown')+':'+row.get('error','')]+=1
        elif kind=='metadata':
            metadata[row['condition_id']]=dict(row)
            try:
                observed_fee=FeeSchedule.from_gamma(row['raw'])
            except (ValueError,KeyError,TypeError):
                observed_fee=None
                errors['MISSING_OR_UNSUPPORTED_FEE_SCHEDULE']+=1
            if row['condition_id']==probe.active:
                fee=observed_fee
                for e in engines.values():
                    e.fee_schedule=fee
                    e.operation_cost=protocol['merge_cost_usd']
                    e.split_cost=protocol['split_cost_usd']
        elif kind=='binance':
            msg=row['message']
            price=float(msg['p']) if 'p' in msg else (float(msg['b'])+float(msg['a']))/2
            ts=row.get('received_at',row['ts'])
            if not btc or ts>btc[-1][0]:
                btc.append((ts,price))
            while btc and ts-btc[0][0]>180:
                btc.popleft()
        elif kind=='market':
            fee=None
        elif kind=='settlement':
            winners[row['condition_id']]=row['winner']
            if row.get('evidence'):
                from .research_capture import reference_metadata
                item=metadata.setdefault(row['condition_id'],{})
                item['settled_reference']=reference_metadata(row['evidence'])
                item['settled_reference_available_at']=row.get('received_at',row['ts'])
        elif kind=='frame':
            msg=row['message']
            ex=msg.get('timestamp')
            ex=float(ex) if ex is not None else None
            if ex is not None and ex>1e12:
                ex/=1000
            latencies.add(msg.get('event_type','unknown'),row.get('received_at',row['ts']),ex)
        research_apply(probe,row)
        probe.audit.clear()
        if kind in ('market','gap'):
            cache=(None,0.,None)
        if predict_scale and probe.active and kind in ('frame','tick') and probe.now-cache[1]>=1:
            cache=(model_feature(probe,metadata,btc,predict_scale),probe.now,probe.active)
        if kind in ('market','gap','frame','tick','stop'):
            opportunities.update(probe,fee,operation_cost=protocol['merge_cost_usd'],split_cost=protocol['split_cost_usd'])
        if probe.active and kind in ('frame','tick'):
            cid=probe.active
            if probe.now-last_availability.get(cid,-1e20)>=1:
                m=probe.markets[cid]
                if any(t in probe.resync_tokens for t in m.tokens):
                    state='resync_required'
                elif any(t not in probe.received for t in m.tokens):
                    state='missing_book'
                elif not probe.fresh():
                    state='stale_book'
                elif any(probe.books[t].view().mid is None for t in m.tokens):
                    state='fresh_book_missing_two_sided_mid'
                else:
                    state='fresh_two_sided_mid'
                availability[cid][state]+=1
                last_availability[cid]=probe.now
        if probe.active and probe.fresh() and kind in ('frame','tick'):
            cid=probe.active
            if probe.now-last_sample.get(cid,-1e20)>=1:
                m=probe.markets[cid]
                up,down=probe.books[m.up].view().mid,probe.books[m.down].view().mid
                if up is not None and down is not None:
                    timelines[cid].append({'ts':probe.now,'mid_up':up,'mid_down':down,'gaps':probe.gaps,
                                          'bid_up':probe.books[m.up].best_bid().price,
                                          'bid_down':probe.books[m.down].best_bid().price})
                    for scale in protocol['model_grid_vol_scale'] if predict_scale is None else [predict_scale]:
                        p=model_feature(probe,metadata,btc,scale)
                        if p is not None:
                            samples[scale].append({'window':cid,'ts':probe.now,'model':p,'mid':up})
                    last_sample[cid]=probe.now
        for key,e in engines.items():
            p,ts,cid=cache
            e.feature(p if cid==probe.active else None,ts)
            research_apply(e,row)
            if kind=='market':
                e.fee_schedule=fee
            for event in e.audit:
                event.setdefault('condition_id',e.active)
                delta=0.
                if event['type']=='merge_confirmed':
                    delta=event['amount']-event['cost']-event['fee']
                elif event['type']=='settlement':
                    delta=event['payout']-event['cost']
                elif event['type']=='taker_fill' and event['sell']:
                    delta=event['size']*event['price']-event['fee']-event['cost_basis']
                if delta:
                    cid=event['condition_id']
                    window_pnl[key][cid]=window_pnl[key].get(cid,0)+delta
                audits[key].append(event)
            e.audit.clear()
    opportunities.finish(probe.now)
    for scale,rows in samples.items():
        for row in rows:
            m=probe.markets[row['window']]
            winner=winners.get(row['window'])
            row['winner']=None if winner is None else int(winner==m.up)
    return {'probe':probe,'metadata':metadata,'btc_n':counts['binance'],'winners':winners,
            'samples':samples,'timelines':timelines,'latencies':latencies.report(),
            'opportunities':opportunities,'clocks':clocks,'errors':dict(errors),'counts':dict(counts),
            'engines':engines,'audits':audits,'window_pnl':window_pnl,'final_now':final_now,
            'availability':dict(availability)}


def markouts(events,timelines):
    results=[]
    for fill in events:
        if fill['type'] not in ('fill','taker_fill'):
            continue
        points=timelines.get(fill['condition_id'],[])
        times=[p['ts'] for p in points]
        i=bisect_left(times,fill['ts'])
        base=points[i] if i<len(points) and times[i]-fill['ts']<=1.1 else None
        field='mid_up' if fill.get('outcome')=='Up' else 'mid_down'
        row=dict(fill)
        for h in (5,20,60):
            j=bisect_left(times,fill['ts']+h)
            future=points[j] if j<len(points) and times[j]-fill['ts']-h<=1.1 else None
            valid=base and future and base['gaps']==future['gaps']
            sign=-1 if fill.get('sell') else 1
            row[f'markout_{h}']=sign*(future[field]-base[field]) if valid else None
            row[f'value_vs_fill_{h}']=sign*(future[field]-fill['price']) if valid else None
        results.append(row)
    return results


def train_model(training,validation,protocol):
    choices=[]
    for scale in protocol['model_grid_vol_scale']:
        choices.append({'scale':scale,'calibration':calibration(training['samples'].get(scale,[]))})
    valid=[x for x in choices if x['calibration']['windows']>=protocol['minimum_model_train_windows']]
    if not valid:
        return {'status':'STOPPED_INSUFFICIENT_MODEL_DATA','chosen_scale':None,'train_grid':choices}
    chosen=min(valid,key=lambda x:(x['calibration']['brier'],x['scale']))['scale']
    rows=validation['samples'].get(chosen,[])
    scores,market=window_scores(rows),window_scores(rows,'mid')
    gain=[market[k]-scores[k] for k in sorted(scores) if k in market]
    ci=bootstrap_ci(gain,protocol['seed'],protocol['bootstrap_replicates'])
    approved=len(gain)>=protocol['minimum_model_validation_windows'] and ci['lower'] is not None and ci['lower']>0
    return {'status':'VALIDATED' if approved else 'STOPPED_NO_PROVEN_OOS_BRIER_GAIN',
            'chosen_scale':chosen if approved else None,'candidate_scale':chosen,
            'train_grid':choices,'validation':calibration(rows),'market':calibration(rows,'mid'),'gain_ci':ci}


def integrity(result,windows,protocol):
    rows=[]
    for market in windows:
        points=[p for p in result['timelines'].get(market['condition_id'],[]) if market['start']<=p['ts']<market['end']]
        first=points[0]['ts']-market['start'] if points else 300.
        last=market['end']-points[-1]['ts'] if points else 300.
        largest=max([first,last]+[b['ts']-a['ts'] for a,b in zip(points,points[1:])])
        rows.append({'condition_id':market['condition_id'],'slug':market['slug'],
                     'first_fresh_seconds_from_start':first,'last_fresh_seconds_to_end':last,
                     'largest_fresh_observation_gap_seconds':largest,'fresh_samples':len(points),
                     'passes_coverage':largest<=protocol['max_fresh_coverage_gap_seconds']})
    clocks=result['clocks']
    return {'windows':rows,'complete_coverage_windows':sum(r['passes_coverage'] for r in rows),
            'clock_samples':len(clocks),'clock_uncertainty_max':max((r['uncertainty_seconds'] for r in clocks),default=None),
            'clock_bound_pass':bool(clocks) and all(r['uncertainty_seconds']<=protocol['clock_uncertainty_seconds_max'] for r in clocks)}


def strategy_table(result,windows,protocol,split):
    rows=[]
    coverage=integrity(result,windows,protocol)
    for spec in protocol['strategy_grid']:
        for bound in ('conservative','optimistic_upper'):
            key=(spec['id'],bound)
            e=result['engines'].get(key)
            if e is None:
                rows.append({'strategy':spec['id'],'bound':bound,'split':split,
                             'status':'NOT_EVALUATED_FAIR_MODEL_GATE','pairs':None,'unpaired':None,
                             'net_pnl':None,'ci':None,'sample_windows':0})
                continue
            events=result['audits'][key]
            for event in events:
                if event['type'] in ('fill','taker_fill'):
                    m=e.markets[event['condition_id']]
                    event['outcome']='Up' if event['token']==m.up else 'Down'
            marks=markouts(events,result['timelines'])
            pair_windows={x['condition_id'] for x in events if x['type']=='merge_confirmed'}
            sales=[x for x in events if x['type']=='taker_fill' and x['job_kind']=='mint_sell']
            for cid in {x['condition_id'] for x in sales}:
                sides=Counter(x['outcome'] for x in sales if x['condition_id']==cid)
                if min(sides.get('Up',0),sides.get('Down',0)):
                    pair_windows.add(cid)
            pnl_values=[result['window_pnl'][key].get(w['condition_id'],0.) for w in windows]
            outstanding=sum(p.size for p in e.positions.values())
            unpaired=sum(abs(e.positions[m.up].size-e.positions[m.down].size) for m in e.markets.values())
            verified=(protocol['merge_cost_usd'] is not None and protocol['split_cost_usd'] is not None
                      and all(cid in result['metadata'] for cid in e.markets))
            pnl=e.merge_pnl+e.residual_pnl
            incomplete=(e.gaps or e.delayed_frames or e.rejected_frames or e.out_of_order_frames or result['errors']
                        or outstanding or e.pending or e.taker_job or not coverage['clock_bound_pass']
                        or coverage['complete_coverage_windows']<len(windows))
            ci=bootstrap_ci(pnl_values,protocol['seed'],protocol['bootstrap_replicates'])
            block=bootstrap_ci(pnl_values,protocol['seed'],protocol['bootstrap_replicates'],block=3)
            row={'strategy':spec['id'],'bound':bound,'split':split,'status':'EVALUATED_COUNTERFACTUAL',
                 'pairs':e.merges+e.completed_taker_pairs,'pair_windows':len(pair_windows),
                 'unpaired_shares_at_end':unpaired,'unpaired_episodes':e.unpaired_started_events,
                 'unpaired_seconds':e.unpaired_seconds,'peak_unpaired_cost':e.peak_unpaired_cost,
                 'single_leg_fill_events':sum(x['type']=='fill' for x in events),
                 'pending_taker_action':e.taker_job is not None,
                 'net_pnl':pnl if verified and not outstanding and not e.pending and not e.taker_job else None,
                 'pnl_zero_operation_cost_bound':pnl,'ci':ci,'block_ci':block,
                 'sample_windows':len(windows),'window_pnl_bound':pnl_values,
                 'max_drawdown':e.max_drawdown,'worst_window_bound':min(pnl_values,default=None),
                 'quality':'INCOMPLETE_DATA' if incomplete else 'COMPLETE_DATA','costs_verified':verified,'fills':e.fills,
                 'markouts':{str(h):{'n':sum(x[f'markout_{h}'] is not None for x in marks),
                     'mean':sum(x[f'markout_{h}'] for x in marks if x[f'markout_{h}'] is not None)/
                     max(1,sum(x[f'markout_{h}'] is not None for x in marks))} for h in (5,20,60)},
                 'unsettled_cost_at_end':sum(p.cost for p in e.positions.values()),
                 'time_measurements':e.measurements.report(),
                 'diagnostics':e.report()['execution_diagnostics']}
            row['acceptance']=acceptance(row,protocol)
            rows.append(row)
            result.setdefault('markout_records',{})[str(key)]=marks
    return rows


def render_report(report):
    text=f"# Paper research: {report['decision']}\n\nScope: {report['scope']}; final holdout: {report['final_status']}.\n\n"
    text+='Blockers: '+', '.join(report['blockers'])+'.\n\n'
    text+='| Strategy | Bound | Pairs | Unpaired shares | Verified net PnL | Zero operation cost PnL bound | Window CI bound | Windows | Status |\n|---|---|---:|---:|---:|---:|---|---:|---|\n'
    for row in report['strategies']+report['final_strategies']:
        ci=row.get('ci') or {}
        text+=f"| {row['strategy']} | {row['bound']} | {row['pairs']} | {row.get('unpaired_shares_at_end')} | {row['net_pnl']} | {row.get('pnl_zero_operation_cost_bound')} | {ci.get('lower')}, {ci.get('upper')} | {row['sample_windows']} | {row['status']} |\n"
    return text+'\nA missing estimate is not zero. Visible-book and optimistic results cannot establish executable profit.\n'


def evaluate(source,out,protocol_path='docs/research/protocol.json',*,exploratory=False,binance_aux_dir=None):
    protocol,protocol_sha=load_protocol(protocol_path)
    digest=input_hash(source)
    auxiliary,auxiliary_hash,auxiliary_report=load_auxiliary(binance_aux_dir)
    all_windows=market_index(source)
    windows=all_windows if exploratory else market_index(source,complete_only=True)
    splits=chronological_split(windows,protocol['split_fractions'])
    directory=Path(out)
    directory.mkdir(parents=True,exist_ok=False)
    reserved={'input_sha256':digest,'protocol_sha256':protocol_sha,
              'windows':{k:[w['condition_id'] for w in v] for k,v in splits.items()},
              'final_status':'SEALED_NOT_EVALUATED'}
    (directory/'split-reservation.json').write_text(json.dumps(reserved,indent=2))
    def run(subset,**kwargs):
        return scan(source,{w['condition_id'] for w in subset},protocol,auxiliary=auxiliary,**kwargs)
    if exploratory:
        descriptive=run(windows,strategies=True)
        model={'status':'ALREADY_SEEN_REFERENCE_EXPLORATORY_ONLY','chosen_scale':None}
        evaluated=windows
        scope='exploratory'
    else:
        train=run(splits['train'])
        validation=run(splits['validation'])
        model=train_model(train,validation,protocol)
        evaluated=splits['train']+splits['validation']
        descriptive=run(evaluated,predict_scale=model['chosen_scale'],strategies=True)
        scope='train_validation_descriptive'
    table=strategy_table(descriptive,evaluated,protocol,scope)
    quality=integrity(descriptive,evaluated,protocol)
    blockers=[]
    if len(splits['test'])<protocol['minimum_test_windows']:
        blockers.append('INSUFFICIENT_RESERVED_TEST_WINDOWS')
    if descriptive['btc_n']==0:
        blockers.append('NO_BINANCE_DATA_IN_SOCKET_JOURNAL')
    if any((descriptive['metadata'].get(w['condition_id'],{}).get('reference') or {}).get('opening_price') is None for w in evaluated):
        blockers.append('MISSING_OFFICIAL_OPENING_REFERENCE_AT_DECISION')
    if protocol['merge_cost_usd'] is None or protocol['split_cost_usd'] is None:
        blockers.append('ACTUAL_OPERATION_COST_UNVERIFIED')
    p=descriptive['probe']
    if descriptive['errors'] or p.gaps or p.delayed_frames or p.rejected_frames or p.out_of_order_frames:
        blockers.append('INCOMPLETE_DATA')
    if quality['complete_coverage_windows']<len(evaluated):
        blockers.append('INCOMPLETE_WINDOW_COVERAGE')
    if not quality['clock_bound_pass']:
        blockers.append('CLOCK_UNCERTAINTY_UNACCEPTABLE_OR_MISSING')
    if model['chosen_scale'] is None:
        blockers.append('FAIR_MODEL_NOT_VALIDATED')
    if auxiliary:
        blockers.append('AUXILIARY_CROSS_HOST_CLOCK_ALIGNMENT_UNVERIFIED')
    final=[]
    if not exploratory and not blockers:
        ledger=Path(__file__).resolve().parent.parent/'runs'/'research-holdout-ledger'
        claim_holdout(ledger/('final-'+digest+'.claim.json'),digest,protocol_sha)
        result=run(splits['test'],predict_scale=model['chosen_scale'],strategies=True)
        final=strategy_table(result,splits['test'],protocol,'test')
        reserved['final_status']='EVALUATED_ONCE'
        (directory/'split-reservation.json').write_text(json.dumps(reserved,indent=2))
    decision='GO' if any(r.get('acceptance',{}).get('decision')=='GO' for r in final) else 'NO-GO'
    report={'mode':'PAPER_ONLY_RESEARCH','decision':decision,'blockers':blockers,
            'protocol_sha256':protocol_sha,'input_sha256':digest,'scope':scope,
            'auxiliary_sha256':auxiliary_hash,'auxiliary_BTC':auxiliary_report,
            'captured_market_windows':len(all_windows),'total_windows':len(windows),
            'excluded_partial_windows':len(all_windows)-len(windows),'evaluated_windows':len(evaluated),
            'reserved_final_windows':len(splits['test']),'final_status':reserved['final_status'],
            'model':model,'strategies':table,'final_strategies':final,
            'planned_strategy_comparisons':protocol['family_comparisons'],
            'performed_conservative_strategy_runs':sum(r['bound']=='conservative' and r['status']=='EVALUATED_COUNTERFACTUAL' for r in table+final),
            'optimistic_runs':sum(r['bound']=='optimistic_upper' and r['status']=='EVALUATED_COUNTERFACTUAL' for r in table+final),
            'opportunities':descriptive['opportunities'].report(),'latency':descriptive['latencies'],
            'clocks':descriptive['clocks'],'errors':descriptive['errors'],'counts':descriptive['counts'],
            'data_integrity':quality,'quality':{'execution_quality':'INCOMPLETE_DATA' if blockers else 'COUNTERFACTUAL_MODEL_ONLY',
                'gaps':p.gaps,'delayed_frames':p.delayed_frames,'out_of_order_frames':p.out_of_order_frames,'rejected_frames':p.rejected_frames},
            'time_measurements':{'scope':'RECORD_ONLY_PROBE_NO_STRATEGY_ORDERS',
                                 'data':p.measurements.report()},
            'book_availability':{'unit':'observations_at_most_once_per_second_not_duration',
                                 'markets':{cid:dict(states) for cid,states in descriptive['availability'].items()}},
            'unproved':['real execution','joint fill probability','oracle/proxy basis','actual operation route/cost','profitability']}
    (directory/'report.json').write_text(json.dumps(report,indent=2,allow_nan=False))
    for name,rows in [('opportunity-intervals',descriptive['opportunities'].events),
                      ('fill-markouts',list(descriptive.get('markout_records',{}).values()))]:
        with gzip.open(directory/(name+'.json.gz'),'wt') as f:
            json.dump(rows,f,allow_nan=False)
    (directory/'report.md').write_text(render_report(report))
    if input_hash(source)!=digest:
        raise RuntimeError('Input changed during evaluation')
    return report


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('source')
    parser.add_argument('--out',required=True)
    parser.add_argument('--protocol',default='docs/research/protocol.json')
    parser.add_argument('--exploratory',action='store_true')
    parser.add_argument('--binance-aux-dir')
    args=parser.parse_args()
    result=evaluate(args.source,args.out,args.protocol,exploratory=args.exploratory,binance_aux_dir=args.binance_aux_dir)
    print(json.dumps({k:result[k] for k in ('decision','blockers','total_windows','final_status')},indent=2))


if __name__=='__main__':
    main()
