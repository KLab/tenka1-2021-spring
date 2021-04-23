import json
import os
import subprocess
import sys
from urllib.request import urlopen

GAME_SERVER = os.getenv('GAME_SERVER', 'https://contest.2021-spring.gbc.tenka1.klab.jp')
TOKEN = os.getenv('TOKEN', 'YOUR_TOKEN')


p = subprocess.Popen(sys.argv[1:], stdin=subprocess.PIPE, stdout=subprocess.PIPE)


def call_api(x: str):
    with urlopen(f'{GAME_SERVER}{x}') as res:
        return json.loads(res.read())


def call_master_data():
    r = call_api('/api/master_data')
    assert len(r['checkpoints']) == 26
    p.stdin.write(f'{r["game_period"]} {r["max_len_task"]} {r["num_agent"]}\n'.encode())
    for c in r['checkpoints']:
        p.stdin.write(f'{c["x"]} {c["y"]}\n'.encode())
    p.stdin.write(f'{r["area_size"]}\n'.encode())
    p.stdin.flush()


def call_game():
    r = call_api(f'/api/game/{TOKEN}')
    assert r['status'] == 'ok'
    p.stdin.write(f'{r["now"]} {len(r["agent"])} {len(r["task"])}\n'.encode())
    for a in r['agent']:
        p.stdin.write(f'{len(a["move"])}\n'.encode())
        for m in a['move']:
            p.stdin.write(f'{m["x"]} {m["y"]} {m["t"]}\n'.encode())
        p.stdin.write(f'-{a["history"]}\n'.encode())
    for t in r['task']:
        p.stdin.write(f'{t["s"]} {t["t"]} {t["weight"]} {t["count"]} {t["total"]}\n'.encode())
    p.stdin.write(f'{r["next_task"]}\n'.encode())
    p.stdin.flush()


def call_move(type: str, param: str):
    r = call_api(f'/api/{type}/{TOKEN}/{param}')
    assert r['status'] == 'ok'
    assert len(r['move']) >= 2
    p.stdin.write(f'{r["now"]} {len(r["move"])}\n'.encode())
    for m in r['move']:
        p.stdin.write(f'{m["x"]} {m["y"]} {m["t"]}\n'.encode())
    p.stdin.flush()


def main():
    while True:
        line = p.stdout.readline()
        if not line:
            break
        a = line.decode().rstrip().split(' ')
        if a[0] == 'master_data':
            call_master_data()
        elif a[0] == 'game':
            call_game()
        elif a[0] in ('move', 'move_next'):
            call_move(a[0], a[1])
        else:
            assert False, f'invalid command {repr(a[0])}'


if __name__ == "__main__":
    main()
