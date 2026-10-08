"""Hydrate only dedicated daily archives; preserve original ZIPs and journals."""
import json
import shutil
import zipfile
from pathlib import Path
from .daily_recorder import verify_journal,day_times
from .daily_model import POLICY_SHA256,FREEZE_COMMIT


def hydrate(output):
    root=Path(output)/'daily-prospective';root.mkdir(parents=True,exist_ok=False)
    source=Path('docs/daily/captures')
    for archive in sorted(source.glob('*.zip')):
        with zipfile.ZipFile(archive)as z:
            names=[n for n in z.namelist()if n.endswith('/journal.jsonl')and not n.startswith('daily-history/')]
            if not names:raise ValueError('Archive has no current daily journal')
            for name in names:
                pieces=name.split('/')
                if len(pieces)==3 and pieces[0]=='daily-prospective':pieces=pieces[1:]
                if len(pieces)!=2 or pieces[1]!='journal.jsonl':raise ValueError('Unexpected archive path')
                day=pieces[0];_,target,_=day_times(day)
                if z.getinfo(name).file_size>10*1024*1024:raise ValueError('Oversized daily journal')
                folder=root/day;folder.mkdir(exist_ok=False)
                path=folder/'journal.jsonl';path.write_bytes(z.read(name))
                rows=verify_journal(path);h=rows[0]
                if h.get('mode')!='REAL_PUBLIC_RECORD_ONLY' or h.get('day')!=day or h.get('nominal_target')!=target or h.get('freeze_commit')!=FREEZE_COMMIT:
                    raise ValueError('Archive not a real frozen daily attempt')
                for label in sorted((Path('docs/daily/labels')/day).glob('resolution-*.json')):
                    shutil.copyfile(label,folder/label.name)
    return root
