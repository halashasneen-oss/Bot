"""Offline S0-S4 policies. No live interface; all operations are local accounting."""
from collections import deque
from dataclasses import replace
import math
from .config import Config
from .engine import Engine, EPS
from .research_stats import upper_opportunity


class ResearchEngine(Engine):
    def __init__(self,spec,protocol,bound='conservative'):
        if bound not in ('conservative','optimistic_upper'):
            raise ValueError('Unknown execution bound')
        filtered=spec['kind'].startswith('filtered_')
        cfg=replace(Config(),min_pair_edge=spec['edge'],
                    guard_mode='combined_mid' if filtered else 'up_mid',
                    guard_warmup_seconds=protocol['warmup_seconds'] if filtered else 0,
                    queue_multiplier=1.5 if bound=='conservative' else 1.)
        super().__init__(cfg)
        self.spec,self.protocol,self.bound=spec,protocol,bound
        self.fair=None
        self.fair_ts=0.
        self.prob_vol=0.
        self.fair_history=deque()
        self.fee_schedule=None
        self.operation_cost=None
        self.split_cost=None
        self.taker_job=None
        self.depth_used={}
        self.s3_last=-math.inf
        self.opportunity_since={}
        self.taker_pnl=0.
        self.sales=0
        self.completed_taker_pairs=0
        self.peak_unpaired_cost=0.
        self.unpaired_seconds=0.
        self.unpaired_market_seconds={}
        self.unpaired_started_events=0
        self._was_unpaired=False
        self.source='OFFLINE_RESEARCH_ONLY'

    def advance(self,ts):
        if self.active and ts>self.now:
            m=self.markets[self.active]
            a,b=self.positions[m.up],self.positions[m.down]
            if abs(a.size-b.size)>EPS:
                dt=max(0,min(ts,m.end)-max(self.now,m.start))
                self.unpaired_seconds+=dt
                self.unpaired_market_seconds[self.active]=self.unpaired_market_seconds.get(self.active,0)+dt
        super().advance(ts)

    def _risk(self):
        unpaired=False
        for cid in self.exposed_markets:
            m=self.markets[cid]
            a,b=self.positions[m.up],self.positions[m.down]
            if abs(a.size-b.size)>EPS:
                unpaired=True
                held=a if a.size>b.size else b
                self.peak_unpaired_cost=max(self.peak_unpaired_cost,held.cost/held.size*abs(a.size-b.size))
        if unpaired and not self._was_unpaired:
            self.unpaired_started_events+=1
        self._was_unpaired=unpaired
        super()._risk()

    def select(self,market,ts):
        self.taker_job=None
        self.depth_used.clear()
        self.fair=None
        self.fair_history.clear()
        self.opportunity_since.clear()
        self.fee_schedule=None
        self.operation_cost=self.split_cost=None
        super().select(market,ts)

    def gap(self,ts):
        self.taker_job=None
        self.fair=None
        self.opportunity_since.clear()
        super().gap(ts)

    def feature(self,p,ts):
        if p is None or not 0<=p<=1:
            self.fair=None
            return
        self.fair,self.fair_ts=p,ts
        if not self.fair_history or ts-self.fair_history[-1][0]>=1:
            self.fair_history.append((ts,p))
            while self.fair_history and ts-self.fair_history[0][0]>60:
                self.fair_history.popleft()
            values=[p for t,p in self.fair_history]
            mean=sum(values)/len(values)
            self.prob_vol=math.sqrt(sum((p-mean)**2 for p in values)/len(values))

    def edge(self,token):
        edge=self.spec['edge']
        if 'dynamic' in self.spec['kind']:
            b,a=self.books[token].best_bid(),self.books[token].best_ask()
            if b and a:
                edge+=.5*(a.price-b.price)+.25*self.prob_vol
        return edge

    def _trade(self,trade,raw):
        order=self.orders.get(trade.asset_id)
        if (self.spec['kind']!='baseline' and order and order.queue_at is not None
                and trade.ts<=max(order.active_at,order.queue_at)):
            self.trade_checks+=1
            self.trade_blocks['not_strictly_after_anchor']+=1
            return
        return super()._trade(trade,raw)

    def _quote_plan(self):
        if self.spec['kind']=='baseline':
            return super()._quote_plan()
        if self.spec['kind']=='taker_opportunity':
            return {},'taker_policy'
        if not self.active or not self.fresh():
            return {},'stale_or_missing_book'
        m=self.markets[self.active]
        warmup=self.protocol['warmup_seconds'] if self.spec['kind'].startswith('filtered_') else 0
        if not m.start+warmup<=self.now<m.end-self.cfg.stop_before_close_seconds:
            return {},'time_filter'
        if self.fair is None or self.now-self.fair_ts>self.cfg.stale_seconds:
            return {},'fair_model_unavailable_or_unvalidated'
        if self.fee_schedule is None:
            return {},'fee_schedule_unverified'
        if self.pending or self.taker_job:
            return {},'pending_completion'
        tops=[(self.books[t].best_bid(),self.books[t].best_ask()) for t in m.tokens]
        if any(b is None or a is None or not 0<a.price-b.price<=self.cfg.kill_spread for b,a in tops):
            return {},'spread_guard'
        a,b=self.positions[m.up],self.positions[m.down]
        op=self.operation_cost or 0
        if max(a.size,b.size)>EPS:
            if abs(a.size-b.size)<EPS:
                return {},'paired_inventory'
            held,token,top=(a,m.down,tops[1]) if a.size>b.size else (b,m.up,tops[0])
            q=min(self.cfg.order_size,abs(a.size-b.size))
            if q<m.minimum-EPS:
                return {},'residual_below_minimum'
            price=min(top[0].price,1-self.spec['edge']-held.cost/held.size-op/q)
        else:
            choices=[(p-top[0].price-self.edge(token),token,top)
                     for p,token,top in zip((self.fair,1-self.fair),m.tokens,tops)]
            benefit,token,top=max(choices,key=lambda x:(x[0],x[1]))
            if benefit<=EPS:
                return {},'no_fair_edge'
            q,price=self.cfg.order_size,top[0].price
        if 'dynamic' in self.spec['kind']:
            inventory=abs(a.cost-b.cost)/self.cfg.max_inventory_usd
            spread=top[1].price-top[0].price
            price=min(price,top[0].price-.5*spread+min(.5*spread,inventory*.02))
        tick=self.books[token].tick_size
        price=math.floor((price+EPS)/tick)*tick
        if q<m.minimum-EPS or price<=0 or price>=top[1].price-EPS:
            return {},'invalid_research_limit'
        return {token:(price,q)},None

    def reserved(self):
        return super().reserved()+(self.taker_job.get('reserved',0) if self.taker_job else 0)

    def available(self,token,sell=False):
        book=self.books[token]
        level=book.best_bid() if sell else book.best_ask()
        if not level:
            return None,0
        return level.price,max(0,level.size-self.depth_used.get((token,sell,level.price),0))

    def frame(self,msg,ts,*,received_at=None):
        if isinstance(msg,dict):
            if msg.get('event_type')=='book':
                token=str(msg.get('asset_id'))
                self.depth_used={k:v for k,v in self.depth_used.items() if k[0]!=token}
            elif msg.get('event_type')=='price_change':
                for c in msg.get('price_changes',[]):
                    try:
                        self.depth_used.pop((str(c['asset_id']),c['side']=='BUY',float(c['price'])),None)
                    except (KeyError,ValueError,TypeError):
                        pass
        super().frame(msg,ts,received_at=received_at)

    def quote(self):
        if self.spec['kind']=='baseline':
            return super().quote()
        if self.taker_job and self.now>=self.taker_job['due']:
            self._arrive()
        if self.taker_job:
            return
        if (self.paused or self.halted or not self.active or self.active in self.blocked
                or not self.fresh() or self._movement_paused()):
            self.opportunity_since.clear()
            return super().quote()
        m=self.markets[self.active]
        if not m.start<=self.now<m.end-self.cfg.stop_before_close_seconds or self.fee_schedule is None:
            return super().quote()
        if self.spec['kind']=='taker_opportunity':
            self._schedule_pair()
            return
        a,b=self.positions[m.up],self.positions[m.down]
        if not self.orders and not self.pending and abs(a.size-b.size)>EPS:
            held,token=(a,m.down) if a.size>b.size else (b,m.up)
            q=min(self.cfg.order_size,abs(a.size-b.size))
            p,depth=self.available(token)
            if p is not None and q>=m.minimum and depth>=q:
                cost=q*p+self.fee_schedule.fee(q,p)
                if held.cost/held.size+cost/q+(self.operation_cost or 0)/q<=1-self.spec['edge']+EPS:
                    if cost<=self.cash-self.reserved()+EPS and self.committed()+cost<=self.cfg.max_inventory_usd+EPS:
                        self.taker_job={'kind':'completion','token':token,'q':q,'reserved':cost,
                                        'ceiling':cost/q,'due':self.now+self.cfg.order_latency_seconds,'cid':self.active}
                        self.log('taker_scheduled',job=dict(self.taker_job))
                        return
        return super().quote()

    def _schedule_pair(self):
        m=self.markets[self.active]
        if self.orders or self.pending or self.exposed_markets or self.now-self.s3_last<self.cfg.quote_lifetime_seconds:
            return
        for sell,kind in ((False,'taker_pair'),(True,'mint_sell')):
            levels=[self.available(t,sell) for t in m.tokens]
            if any(p is None for p,d in levels):
                self.opportunity_since.pop(kind,None)
                continue
            opportunity=upper_opportunity(kind,[p for p,d in levels],[d for p,d in levels],
                self.fee_schedule,self.spec['edge'],self.cfg.order_size,
                self.split_cost if sell else self.operation_cost)
            if not opportunity or not opportunity['qualifies'] or opportunity['quantity']<m.minimum:
                self.opportunity_since.pop(kind,None)
                continue
            self.opportunity_since.setdefault(kind,self.now)
            lag=max(0,*(self.received[t]-self.exchange_ts[t] for t in m.tokens))
            if self.now-self.opportunity_since[kind]<self.cfg.order_latency_seconds+lag:
                continue
            q=opportunity['quantity']
            cost=q+(self.split_cost or 0) if sell else q*sum(p for p,d in levels)+opportunity['taker_fees']
            if cost>self.cash+EPS or self.committed()+cost>self.cfg.max_inventory_usd+EPS:
                continue
            self.taker_job={'kind':kind,'q':q,'leg':0,'cid':self.active,'reserved':cost,
                            'limits':[p for p,d in levels],'due':self.now+self.cfg.order_latency_seconds,
                            'minted':False,'mint_assumption':'PRE_SPLIT_INVENTORY_UPPER_BOUND' if sell else None}
            self.s3_last=self.now
            self.opportunity_since.clear()
            self.log('taker_scheduled',job=dict(self.taker_job))
            return

    def _arrive(self):
        job=self.taker_job
        self.taker_job=None
        if job['cid']!=self.active or not self.fresh() or self.paused or self.halted or self.active in self.blocked:
            self.log('taker_aborted',reason='stale_gap_or_risk',job=job)
            return
        m=self.markets[self.active]
        if self.now>=m.end-self.cfg.stop_before_close_seconds:
            self.log('taker_aborted',reason='close_buffer',job=job)
            return
        sell=job['kind']=='mint_sell'
        token=job['token'] if job['kind']=='completion' else m.tokens[job['leg']]
        p,depth=self.available(token,sell)
        q=job['q']
        if p is None or depth<q:
            self.log('taker_aborted',reason='arrival_depth',job=job)
            return
        fee=self.fee_schedule.fee(q,p)
        if job['kind']=='completion':
            held=self.positions[m.down if token==m.up else m.up]
            cap=1-self.spec['edge']-held.cost/held.size-(self.operation_cost or 0)/q if held.size else -1
            if p+fee/q>min(cap,job['ceiling'])+EPS:
                self.log('taker_aborted',reason='arrival_breakeven',job=job)
                return
        else:
            limit=job['limits'][job['leg']]
            if (sell and p<limit-EPS) or (not sell and p>limit+EPS):
                self.log('taker_aborted',reason='arrival_price',job=job)
                return
        if sell and not job['minted']:
            cost=q+(self.split_cost or 0)
            if cost>self.cash+EPS:
                return
            self.cash-=cost
            for t in m.tokens:
                self.positions[t].size+=q
                self.positions[t].cost+=cost/2
            job['minted']=True
            self.log('paper_mint',q=q,cost=cost)
        if sell:
            cost=self.positions[token].remove(q)
            self.cash+=q*p-fee
            pnl=q*p-fee-cost
            self.taker_pnl+=pnl
            self.residual_pnl+=pnl
            self.sales+=1
        else:
            cost=q*p+fee
            if cost>self.cash+EPS or self.committed()+cost>self.cfg.max_inventory_usd+EPS:
                return
            self.cash-=cost
            self.positions[token].size+=q
            self.positions[token].cost+=cost
            self.fills+=1
        self.fees+=fee
        self.turnover+=q*p
        key=(token,sell,p)
        self.depth_used[key]=self.depth_used.get(key,0)+q
        self.log('taker_fill',token=token,price=p,size=q,fee=fee,sell=sell,cost_basis=cost,
                 reason='OFFLINE_VISIBLE_DEPTH_SEQUENTIAL_ARRIVAL',job_kind=job['kind'])
        if job['kind']!='completion' and job['leg']==0:
            job['leg']=1
            job['due']=self.now+self.cfg.order_latency_seconds
            job['reserved']=0 if sell else q*job['limits'][1]+self.fee_schedule.fee(q,job['limits'][1])
            self.taker_job=job
        elif job['kind']=='mint_sell':
            self.completed_taker_pairs+=1
        self._merge()
        self._risk()
