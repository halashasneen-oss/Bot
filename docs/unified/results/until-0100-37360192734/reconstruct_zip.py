from pathlib import Path
import hashlib
root=Path(__file__).resolve().parent
body=b''.join((root/p).read_bytes() for p in ('original.zip.part01','original.zip.part02'))
assert len(body)==14163402
assert hashlib.sha256(body).hexdigest()=='d9a1ea9c3880e5455b5960d8b483cb688ebc640a5a16bf80d545941209683caf'
(root/'original.zip').write_bytes(body)
print('Exact original ZIP reconstructed and SHA256 verified')
