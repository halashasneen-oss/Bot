import pytest
from pairbot.daily_capture import SLUG, universe, book_status


def event():
    return [{'slug': SLUG, 'markets': [{'slug': 'threshold', 'conditionId': 'condition',
        'outcomes': '["No", "Yes"]', 'clobTokenIds': '["22", "11"]',
        'closed': False, 'enableOrderBook': True}]}]


def test_outcome_mapping_preserves_order():
    assert [(r['outcome'], r['token_id']) for r in universe(event())] == [('No', '22'), ('Yes', '11')]


@pytest.mark.parametrize('field,value', [('closed', True), ('enableOrderBook', False),
    ('outcomes', '["Up", "Down"]'), ('clobTokenIds', '["11", "11"]')])
def test_bad_universe_rejected(field, value):
    rows = event()
    rows[0]['markets'][0][field] = value
    with pytest.raises(ValueError):
        universe(rows)


def test_book_sort_independent_and_token_validation():
    book = {'asset_id': '11', 'bids': [{'price': '.1', 'size': '5'}, {'price': '.4', 'size': '2'}],
            'asks': [{'price': '.9', 'size': '3'}, {'price': '.6', 'size': '4'}]}
    assert book_status(book, '11') == 'two_sided'
    assert book_status(book, '22') == 'token_mismatch'
    book['asks'][1]['price'] = '.4'
    assert book_status(book, '11') == 'crossed_or_locked'
    book['asks'] = []
    assert book_status(book, '11') == 'one_sided'
