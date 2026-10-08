from dataclasses import dataclass, asdict
from datetime import datetime
import json
import math
import re


def array(value):
    return json.loads(value) if isinstance(value, str) else value


@dataclass(frozen=True)
class Market:
    slug: str
    condition_id: str
    up: str
    down: str
    start: float
    end: float
    tick: float
    minimum: float

    def __post_init__(self):
        match = re.fullmatch(r'btc-updown-5m-(\d+)', self.slug)
        if not match or int(match[1]) != self.start or self.end != self.start + 300:
            raise ValueError('Only exact BTC 5-minute windows are supported')
        if not self.condition_id or not self.up or not self.down or self.up == self.down:
            raise ValueError('Invalid market identifiers')
        if not math.isfinite(self.tick) or not 0 < self.tick < 1 or not math.isfinite(self.minimum) or self.minimum <= 0:
            raise ValueError('Invalid tick/minimum')

    @property
    def tokens(self):
        return self.up, self.down

    def data(self):
        return asdict(self)


def parse_market(raw):
    if raw.get('closed') or raw.get('acceptingOrders') is not True or raw.get('negRisk'):
        raise ValueError('Market closed, non-tradable, or unsupported neg-risk')
    slug = raw['slug']
    match = re.fullmatch(r'btc-updown-5m-(\d+)', slug)
    if not match:
        raise ValueError('Not a BTC 5-minute market')
    outcomes, tokens = array(raw['outcomes']), array(raw['clobTokenIds'])
    if len(outcomes) != 2 or len(tokens) != 2:
        raise ValueError('Two outcomes required')
    mapping = dict(zip((str(x).lower() for x in outcomes), map(str, tokens)))
    start = int(match[1])
    end = datetime.fromisoformat(raw['endDate'].replace('Z', '+00:00')).timestamp()
    return Market(slug, raw['conditionId'], mapping['up'], mapping['down'], start, end,
                  float(raw['orderPriceMinTickSize']), float(raw['orderMinSize']))


def resolution(raw, market):
    """Never infer a winner from BTC or a nearly-one price."""
    if raw.get('conditionId') != market.condition_id or raw.get('closed') is not True:
        return None
    if str(raw.get('umaResolutionStatus', '')).lower() != 'resolved':
        return None
    labels, prices = array(raw['outcomes']), array(raw['outcomePrices'])
    if len(labels) != 2 or len(prices) != 2:
        return None
    prices = list(map(float, prices))
    if sorted(prices) != [0.0, 1.0]:
        return None
    label = str(labels[prices.index(1.0)]).lower()
    return {'up': market.up, 'down': market.down}.get(label)
