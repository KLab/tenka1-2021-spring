import os
import redis
import sys
import map_gen

from pathlib import Path
from time import time
from typing import TextIO, NamedTuple, List, Dict, Tuple

map_path = Path(sys.argv[1])
if sys.argv[2][0] == '+':
    start_at0 = int(time() * 1000) + int(sys.argv[2])
else:
    start_at0 = int(sys.argv[2])

gamedb_host = os.environ.get('GAMEDB_HOST', 'localhost')
gamedb_port = int(os.environ.get('GAMEDB_PORT', '6379'))

print("gamedb:", gamedb_host, gamedb_port)

red = redis.Redis(host=gamedb_host, port=gamedb_port, db=0)

red.delete('start_at')
red.delete('period')
red.delete('checkpoint')
red.delete('task')
red.delete('task_time')

with (map_path).open() as f:
    m = map_gen.read_game_map(f)
for name, pos in m.check_points.items():
    red.hset('checkpoint', f'{pos[0]}-{pos[1]}', name)
for task in m.tasks:
    red.rpush('task', f'{task[2]} {task[0]} {task[1]}')
    red.hset('task_time', task[2], task[0])


red.set('start_at', start_at0)
red.set('period', m.game_time)

print("start_at:", start_at0)
print("period:", m.game_time)
print("done")