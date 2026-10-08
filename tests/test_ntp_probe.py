import asyncio
import struct
import pytest

from pairbot.ntp_probe import parse_packet, stamp, diagnostic
from pairbot.directional import load_policy


T = 1800000000.


def packet():
    value = bytearray(48)
    value[0] = (4 << 3) | 4
    value[1] = 2
    value[3] = struct.pack('!b', -20)[0]
    value[4:8] = struct.pack('!i', int(.01 * 65536))
    value[8:12] = struct.pack('!I', int(.002 * 65536))
    value[16:24] = stamp(T - 10)
    value[24:32] = stamp(T)
    value[32:40] = stamp(T + .03)
    value[40:48] = stamp(T + .04)
    return value


def test_four_timestamp_offset_and_root_error_bound():
    result = parse_packet(packet(), stamp(T), T, T + .05, .05)
    assert result['offset_seconds'] == pytest.approx(.01, abs=1e-6)
    assert result['network_delay_seconds'] == pytest.approx(.04, abs=1e-6)
    assert .0269 < result['uncertainty_seconds'] < .0271
    assert result['authentication'] == 'NONE' and result['production_admitted'] is False


@pytest.mark.parametrize('change', ['origin', 'leap', 'stratum', 'mode', 'zero_receive', 'backward_server', 'future_reference', 'length'])
def test_invalid_ntp_response_rejected(change):
    raw = packet()
    if change == 'origin': raw[24:32] = stamp(T + 1)
    elif change == 'leap': raw[0] |= 3 << 6
    elif change == 'stratum': raw[1] = 0
    elif change == 'mode': raw[0] = (4 << 3) | 3
    elif change == 'zero_receive': raw[32:40] = b'\0' * 8
    elif change == 'backward_server': raw[40:48] = stamp(T + .01)
    elif change == 'future_reference': raw[16:24] = stamp(T + 1)
    else: raw.append(0)
    with pytest.raises(ValueError): parse_packet(raw, stamp(T), T, T + .05, .05)


def test_local_clock_step_cannot_be_mistaken_for_precise_ntp():
    with pytest.raises(ValueError, match='stepped'):
        parse_packet(packet(), stamp(T), T, T + .5, .05)


def test_no_best_sample_selection(monkeypatch):
    import pairbot.ntp_probe as module
    values = iter([{'uncertainty_seconds': .1, 'offset_seconds': 0, 'observed_at': T},
                   {'uncertainty_seconds': .8, 'offset_seconds': 0, 'observed_at': T + 2}])
    monkeypatch.setattr(module, 'sample', lambda: next(values))
    monkeypatch.setattr(module.time, 'sleep', lambda _: None)
    result = diagnostic(load_policy())
    assert result['worst_uncertainty_seconds'] == .8
    assert not result['all_samples_within_frozen_bound']
    assert result['clock_result'] == 'NOT_PROVEN' and len(result['samples']) == 2


def test_inconsistent_offsets_do_not_admit_clock(monkeypatch):
    import pairbot.ntp_probe as module
    values = iter([{'uncertainty_seconds': .01, 'offset_seconds': 0, 'observed_at': T},
                   {'uncertainty_seconds': .01, 'offset_seconds': 2, 'observed_at': T + 2}])
    monkeypatch.setattr(module, 'sample', lambda: next(values))
    monkeypatch.setattr(module.time, 'sleep', lambda _: None)
    result = diagnostic(load_policy())
    assert result['all_samples_within_frozen_bound'] and not result['intervals_consistent']
    assert result['clock_result'] == 'NOT_PROVEN'


def test_catalog_and_precise_time_do_not_grant_reference_or_go(monkeypatch):
    import pairbot.directional_readiness as module
    class API:
        async def get_once(self, url, params): return {'feeds': [{'feedID': 'catalog-only'}]}
        async def close(self): pass
    async def metadata(): return {'decision': 'NO_GO', 'predictions': 0}
    monkeypatch.setattr(module, 'PublicAPI', API)
    monkeypatch.setattr(module, 'probe', metadata)
    monkeypatch.setattr(module, 'diagnostic', lambda _: {'clock_result': 'CANDIDATE_ONLY'})
    result = asyncio.run(module.readiness())
    assert result['chainlink_catalog']['ok']
    assert result['decision'] == 'NO_GO' and result['source_admission'] is False
    assert result['clock_admission'] is False and result['fills'] == 0
