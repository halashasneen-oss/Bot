"""Bounded NTP diagnostic, not a clock setter or production clock admission.

RFC5905 four-timestamp estimate with server-advertised root error. Conditional
on the server's UTC correctness; unauthenticated NTP is not a verified UTC oracle.
Two predeclared samples, both retained, no minimum-RTT cherry-picking.
"""
import math
import socket
import struct
import time

HOST = 'time.cloudflare.com'
EPOCH = 2208988800


def stamp(unix):
    t = unix + EPOCH
    return struct.pack('!II', int(t) & 0xffffffff, int((t % 1) * 2**32))


def decode_stamp(data):
    seconds, fraction = struct.unpack('!II', data)
    # This diagnostic only supports the current era, before the 2036 rollover.
    return seconds - EPOCH + fraction / 2**32


def parse_packet(packet, origin, sent, received, monotonic_elapsed):
    if len(packet) != 48:
        raise ValueError('Unexpected NTP packet length')
    leap, version, mode = packet[0] >> 6, (packet[0] >> 3) & 7, packet[0] & 7
    stratum = packet[1]
    if leap != 0 or version not in (3, 4) or mode != 4 or not 1 <= stratum <= 15:
        raise ValueError('Unsynchronized, leap-event, kiss-of-death or invalid NTP response')
    if packet[24:32] != origin:
        raise ValueError('NTP origin does not match this request')
    t2, t3 = decode_stamp(packet[32:40]), decode_stamp(packet[40:48])
    reference = decode_stamp(packet[16:24])
    wall_elapsed = received - sent
    if (monotonic_elapsed < 0 or wall_elapsed < 0 or t2 <= 0 or t3 < t2
            or reference <= 0 or reference > t3):
        raise ValueError('Invalid NTP chronology')
    if abs(wall_elapsed - monotonic_elapsed) > .001:
        raise ValueError('Local wall clock stepped during NTP probe')
    root_delay = struct.unpack('!i', packet[4:8])[0] / 65536
    root_dispersion = struct.unpack('!I', packet[8:12])[0] / 65536
    precision = 2.0 ** struct.unpack('!b', packet[3:4])[0]
    network_delay = max(wall_elapsed, monotonic_elapsed) - (t3 - t2)
    if network_delay < -precision:
        raise ValueError('Negative NTP network delay')
    delay_bound = max(0, network_delay) / 2
    # Include the entire current request wall-vs-monotonic discrepancy,
    # server precision, and signed root delay conservatively without subtraction.
    uncertainty = delay_bound + abs(root_delay) / 2 + root_dispersion + precision + abs(wall_elapsed - monotonic_elapsed)
    offset = ((t2 - sent) + (t3 - received)) / 2
    if not math.isfinite(offset + uncertainty):
        raise ValueError('Non-finite NTP result')
    return {'offset_seconds': offset, 'uncertainty_seconds': uncertainty,
            'observed_at': received, 'network_delay_seconds': network_delay,
            'root_delay_seconds': root_delay, 'root_dispersion_seconds': root_dispersion,
            'server_precision_seconds': precision, 'server_reference_at': reference,
            'stratum': stratum, 'server_receive_at': t2, 'server_transmit_at': t3,
            'request_sent_at': sent, 'monotonic_elapsed_seconds': monotonic_elapsed,
            'wall_minus_monotonic_elapsed_seconds': wall_elapsed - monotonic_elapsed,
            'authentication': 'NONE', 'production_admitted': False,
            'bound_assumption': 'Correct server UTC and advertised root error; no cryptographic proof'}


def sample():
    # Connected UDP checks the response peer against the resolved server.
    addresses = socket.getaddrinfo(HOST, 123, type=socket.SOCK_DGRAM)
    family, kind, proto, _, address = addresses[0]
    with socket.socket(family, kind, proto) as sock:
        sock.settimeout(4)
        sock.connect(address)
        sent, monotonic_sent = time.time(), time.monotonic()
        origin = stamp(sent)
        request = bytearray(48)
        request[0] = (4 << 3) | 3
        request[40:48] = origin
        sock.send(request)
        packet = sock.recv(512)
        received, monotonic_received = time.time(), time.monotonic()
    result = parse_packet(packet, origin, sent, received, monotonic_received - monotonic_sent)
    result.update({'server': HOST, 'peer': address[0], 'packet_hex': packet.hex()})
    return result


def diagnostic(policy):
    samples = []
    for index in range(2):
        if index:
            time.sleep(2)
        try:
            samples.append({'index': index, 'ok': True, **sample()})
        except (OSError, ValueError) as exc:
            samples.append({'index': index, 'ok': False, 'error': f'{type(exc).__name__}: {exc}'})
    valid = [r for r in samples if r['ok']]
    worst = max((r['uncertainty_seconds'] for r in valid), default=None)
    # Compare intervals at the second receipt, including 15ppm holdover.
    consistent = False
    if len(valid) == 2:
        left, right = valid
        age = max(0, right['observed_at'] - left['observed_at'])
        width = left['uncertainty_seconds'] + age * 15e-6 + right['uncertainty_seconds']
        consistent = abs(left['offset_seconds'] - right['offset_seconds']) <= width
    return {'source': HOST, 'samples': samples, 'worst_uncertainty_seconds': worst,
            'all_samples_within_frozen_bound': len(valid) == 2 and worst <= policy['maximum_clock_uncertainty_seconds'],
            'intervals_consistent': consistent, 'production_admitted': False,
            'sample_selection': 'both predeclared samples; no best-sample selection',
            'clock_result': 'CANDIDATE_ONLY' if len(valid) == 2 and consistent and worst <= policy['maximum_clock_uncertainty_seconds'] else 'NOT_PROVEN'}
