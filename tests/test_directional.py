import copy
import json
import math
from pathlib import Path
import pytest

from pairbot.directional import (POLICY_SHA256, FREEZE_TIME, book_cost, decide, load_policy,
                                 probability, read_journal, run, summarize, volatility)
from pairbot.directional_fixture import candidate, rows


def test_policy_is_frozen():
    import hashlib
    from pairbot.directional import POLICY_PATH
    assert hashlib.sha256(POLICY_PATH.read_bytes()).hexdigest() == POLICY_SHA256
    assert load_policy()['mode'] == 'prediction-record-only'


@pytest.mark.parametrize('direction', ['YES', 'NO'])
def test_directional_choices_are_complements_without_fills(direction):
    result = decide(candidate(direction), load_policy())
    assert result['choice'] == direction
    assert result['data_clean']
    assert result['fills'] == 0 and result['realized_pnl'] is None
    assert result['bounds'][direction]['cost_usd_bound'] > 5 * .55


def test_symmetric_probability_and_twap_covariance():
    assert probability(80000, 80000, 10, 180, 300, 'terminal') == .5
    terminal = probability(80100, 80000, 10, 180, 300, 'terminal')
    twap = probability(80100, 80000, 10, 180, 300, 'twap', 60)
    # Full future TWAP has variance sigma^2 * (60 + 60/3) = sigma^2 * 80.
    expected = .5 * (1 + math.erf(100 / math.sqrt(2 * 100 * 80)))
    assert twap == pytest.approx(expected)
    assert twap > terminal
    assert twap + probability(79900, 80000, 10, 180, 300, 'twap', 60) == pytest.approx(1)


def test_partial_twap_does_not_infer_missing_area():
    with pytest.raises(ValueError, match='observed TWAP'):
        probability(80000, 80000, 10, 270, 300, 'twap', 60)
    assert probability(80000, 80000, 10, 270, 300, 'twap', 60, 80000 * 30) == .5


@pytest.mark.parametrize('path,value', [
    (('clock', 'uncertainty_seconds'), .51),
    (('clock', 'observed_at'), 0),
    (('clock', 'offset_seconds'), 4),
    (('reference', 'source_id'), 'BINANCE-REPLACEMENT'),
    (('reference', 'kind'), 'twap'),
    (('reference', 'verified'), False),
    (('reference', 'gap'), True),
    (('reference', 'exchange_at'), 1800000100),
    (('reference', 'received_at'), 1800000181),
    (('market', 'opening', 'received_at'), 1800000301),
    (('market', 'opening', 'reference_at'), 1800000001),
    (('market', 'rules', 'verified'), False),
    (('market', 'rules', 'kind'), 'unknown'),
    (('market', 'rules', 'window_seconds'), 30),
    (('market', 'minimum_shares'), 10),
    (('fees', 'verified'), False),
    (('fees', 'exponent'), 2),
    (('other_cost_per_share',), None),
    (('other_cost_verified',), False),
    (('books', 'YES', 'snapshot'), False),
    (('books', 'NO', 'gap'), True),
    (('books', 'YES', 'token_id'), 'wrong-token'),
    (('books', 'NO', 'asks'), [[.55, 1]]),
    (('books', 'YES', 'bids'), [[.6, 10]]),
    (('reference', 'price'), float('nan')),
])
def test_invalid_data_always_abstains(path, value):
    row = candidate()
    target = row
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    result = decide(row, load_policy())
    assert result['choice'] == 'ABSTAIN' and not result['data_clean']
    assert result['reasons'] and result['fills'] == 0


def test_future_history_is_rejected_even_if_outside_lookback():
    row = candidate()
    row['reference']['history'].insert(0, {'price': 1, 'exchange_at': row['received_at'] + 100,
                                          'received_at': row['received_at'] + 100})
    assert 'future' in decide(row, load_policy())['reasons'][0]


def test_gaps_and_duplicate_reference_times_rejected():
    for index in (5, 6):
        row = candidate()
        row['reference']['history'][index]['exchange_at'] = row['reference']['history'][index-1]['exchange_at']
        assert decide(row, load_policy())['choice'] == 'ABSTAIN'


def test_fee_bound_walks_depth_and_cannot_assume_zero_fee():
    row = candidate()
    book = row['books']['YES']
    book['asks'] = [[.55, 2], [.6, 3]]
    cost, _ = book_cost(book, 5, .07, .005)
    assert cost > 2 * .55 + 3 * .6 + 5 * .005
    assert cost > book_cost(candidate()['books']['YES'], 5, .07, .005)[0]


def test_edge_filter_can_abstain_with_clean_data():
    row = candidate()
    # Raise strike to today's observed price => p=0.5, both asks cost >0.5.
    row['market']['opening']['price'] = row['reference']['price']
    result = decide(row, load_policy())
    assert result['choice'] == 'ABSTAIN' and result['data_clean']
    assert result['reasons'] == ['no sufficient net expected edge']


def source_file(tmp_path, events=None):
    path = tmp_path / 'input.jsonl'
    path.write_text(''.join(json.dumps(r) + '\n' for r in (events if events is not None else rows())))
    return path


def test_full_pipeline_synthetic_never_go_and_no_overwrite(tmp_path):
    source = source_file(tmp_path)
    journal, report = tmp_path / 'predictions.jsonl', tmp_path / 'report.json'
    result = run(source, journal, report)
    assert result['decision'] == 'NO_GO' and result['synthetic']
    assert result['full']['decisions'] == 3 and result['clean']['decisions'] == 2
    assert result['excluded_decision_fraction'] == pytest.approx(1/3)
    assert result['excluded_time_fraction'] is None
    recorded = list(read_journal(journal))
    assert recorded[1]['type'] == 'prediction' and recorded[2]['type'] == 'resolution'
    assert recorded[-1]['type'] == 'complete'
    with pytest.raises(ValueError, match='Report exists'):
        run(source, journal, report)


def test_hash_chain_detects_tamper_and_missing_completion(tmp_path):
    source = source_file(tmp_path)
    journal, report = tmp_path / 'journal.jsonl', tmp_path / 'report.json'
    run(source, journal, report)
    original = journal.read_text()
    journal.write_text(original.replace('"p_yes":', '"tampered_p_yes":', 1))
    with pytest.raises(ValueError, match='altered'):
        list(read_journal(journal))
    journal.write_text('\n'.join(original.splitlines()[:-1]) + '\n')
    with pytest.raises(ValueError, match='Incomplete'):
        list(read_journal(journal))


@pytest.mark.parametrize('mutation', ['duplicate', 'early_result', 'arrival_regression', 'wrong_winner'])
def test_causal_sequence_rejected(tmp_path, mutation):
    events = list(rows())[:3]
    if mutation == 'duplicate':
        events.insert(2, copy.deepcopy(events[1]))
    elif mutation == 'early_result':
        events[2]['received_at'] = events[1]['received_at'] + 1
    elif mutation == 'arrival_regression':
        events[2]['received_at'] = events[1]['received_at'] - 1
    else:
        events[2]['winner'] = 'PRICE-GUESSED'
    source = source_file(tmp_path, events)
    with pytest.raises(ValueError):
        run(source, tmp_path / 'journal.jsonl', tmp_path / 'report.json')
    assert not (tmp_path / 'report.json').exists()


def test_old_input_format_rejected_before_output(tmp_path):
    source = source_file(tmp_path, [{'type': 'research_header'}])
    with pytest.raises(ValueError, match='old journals prohibited'):
        run(source, tmp_path / 'journal.jsonl', tmp_path / 'report.json')
    assert not (tmp_path / 'journal.jsonl').exists()


def test_old_recording_rejected_even_with_directional_header(tmp_path):
    events = list(rows())
    events[0]['synthetic'] = False
    events[0]['recording_started_at'] = FREEZE_TIME - 1
    source = source_file(tmp_path, events)
    with pytest.raises(ValueError, match='prospective'):
        run(source, tmp_path / 'journal.jsonl', tmp_path / 'report.json')


def test_no_go_without_enough_new_independent_markets():
    row = decide(candidate(), load_policy())
    report = summarize([row], {row['market_id']: 'YES'}, load_policy())
    assert report['decision'] == 'NO_GO'
    assert not report['checks']['resolved_sample']
    assert report['profitability_proven'] is False


def test_public_probe_never_admits_opening_from_metadata_or_retries_block():
    import asyncio
    from pairbot.directional_probe import probe
    from pairbot.feed import AccessBlocked
    class Fake:
        def __init__(self):
            self.calls = []
        async def get_once(self, url, params=None):
            self.calls.append(url)
            if url.endswith('/time'):
                raise AccessBlocked('fixture blocked clock')
            if url.endswith('/markets'):
                return [{'slug': 'btc-updown-5m-1800000000', 'conditionId': 'fixture',
                         'events': [{'id': 'e', 'eventMetadata': {'priceToBeat': 80000}}]}]
            return [{'slug': 'btc-updown-5m-1800000000', 'eventMetadata': {'priceToBeat': 80000}}]
    fake = Fake()
    result = asyncio.run(probe(fake, 1800000180))
    assert result['opening_observation']['opening_price'] == 80000
    assert result['reference_admitted'] is False and result['decision'] == 'NO_GO'
    assert len(fake.calls) == 3 and result['predictions'] == 0


def test_missing_windows_stay_in_full_cohort(tmp_path):
    source = source_file(tmp_path, list(rows())[:3])
    result = run(source, tmp_path / 'journal.jsonl', tmp_path / 'report.json')
    assert result['predeclared_market_count'] == 3
    assert result['full']['decisions'] == 3 and result['clean']['decisions'] == 1
    assert result['block_reasons']['missing scheduled decision'] == 2


def test_same_window_cannot_count_twice_under_different_ids(tmp_path):
    events = list(rows())[:2]
    duplicate = copy.deepcopy(events[1])
    duplicate['market']['id'] = 'another-id-same-window'
    events.append(duplicate)
    with pytest.raises(ValueError, match='Duplicate window'):
        run(source_file(tmp_path, events), tmp_path / 'journal.jsonl', tmp_path / 'report.json')


def test_frozen_clock_threshold_not_relaxed_for_integer_time():
    from pairbot.research_capture import clock_sample
    probe = clock_sample(10, 10, 10.01, sent_ns=10_000_000_000, received_ns=10_010_000_000)
    assert probe['uncertainty_seconds'] > load_policy()['maximum_clock_uncertainty_seconds']


def test_synthetic_fixture_cannot_be_relabelled_real(tmp_path):
    events = list(rows())[:3]
    events[0]['synthetic'] = False
    events[0]['recording_started_at'] = FREEZE_TIME + 1
    source = source_file(tmp_path, events)
    with pytest.raises(ValueError, match='relabelled real'):
        run(source, tmp_path / 'journal.jsonl', tmp_path / 'report.json')
