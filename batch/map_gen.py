"""
下記の形式のマップデータを生成します

ゲーム時間[s]
マップサイズ(縦横)
エージェントの1sでの移動速度
エージェント数(以下はエージェント数分)
x座標 y座標
チェックポイントの数(以下はチェックポイント数分)
名称(大文字アルファベット1文字)
x座標 y座標
タスクの数(以下はタスク数分)
タスクの公開時間
スコア
タスク文字列
"""

import sys
import argparse
import string
import time
import re
from pathlib import Path
from random import Random
from typing import TextIO, NamedTuple, List, Dict, Tuple

MAPS_DIR = "./maps"


class GameMap(NamedTuple):
    game_time: int
    map_size: int  # deprecated
    agent_speed: int  # deprecated
    agent_points: List[Tuple[int, int]]  # deprecated
    check_points: Dict[str, Tuple[int, int]]  # {A : (x, y)}
    tasks: List[Tuple[int, int, str]]  # [(t, score, task)]


def read_game_map(f: TextIO) -> GameMap:
    game_time = int(f.readline().rstrip())
    map_size = int(f.readline().rstrip())
    agent_speed = int(f.readline().rstrip())

    agent_num = int(f.readline().rstrip())
    agent_points = [
        tuple(map(int, f.readline().rstrip().split())) for _ in range(agent_num)
    ]

    check_point_num = int(f.readline().rstrip())
    check_points = dict()
    for _ in range(check_point_num):
        name = f.readline().rstrip()
        pos = tuple(map(int, f.readline().rstrip().split()))
        check_points[name] = pos

    task_num = int(f.readline().rstrip())
    tasks = [
        (int(f.readline().rstrip()), int(f.readline().rstrip()), f.readline().rstrip())
        for _ in range(task_num)
    ]

    return GameMap(
        game_time,
        map_size,
        agent_speed,
        agent_points,
        check_points,
        tasks,
    )


def write_game_map(w: TextIO, m: GameMap) -> None:
    w.write(f"{m.game_time}\n")
    w.write(f"{m.map_size}\n")
    w.write(f"{m.agent_speed}\n")

    w.write(f"{len(m.agent_points)}\n")
    for pos in m.agent_points:
        w.write(f"{pos[0]} {pos[1]}\n")

    w.write(f"{len(m.check_points)}\n")
    for name, pos in m.check_points.items():
        w.write(f"{name}\n")
        w.write(f"{pos[0]} {pos[1]}\n")

    w.write(f"{len(m.tasks)}\n")
    for task in m.tasks:
        w.write(f"{task[0]}\n")
        w.write(f"{task[1]}\n")
        w.write(f"{task[2]}\n")


def generate_check_point(random, agent_points, map_size, check_point_num) -> List[Tuple[int, int]]:
    used = [[False] * map_size for _ in range(map_size)]
    points = []

    # Do not put checkpoint at initial position of agents.
    for p in agent_points:
        used[p[0]][p[1]] = True

    def update_used(x: int, y: int):
        # Update 'uesd' marks regarding Social Distance.
        for dx in range(-3, 4):
            for dy in range(-3, 4):
                if 0 <= x + dx < map_size and 0 <= y + dy < map_size:
                    used[x + dx][y + dy] = True

    # Put 4 points that located on each 4 edges.
    # (0, y), (30, y), (x, 0), (x, 30)
    points.append((0, random.randint(0, map_size - 2)))
    points.append((map_size - 1, random.randint(0, map_size - 2)))
    points.append((random.randint(0, map_size - 2), 0))
    points.append((random.randint(0, map_size - 2), map_size - 1))

    for p in points:
        update_used(p[0], p[1])

    sum_x, sum_y = 0, 0
    for p in points:
        sum_x += p[0]
        sum_y += p[1]

    while len(points) < check_point_num:
        # Generate some next candidates to pick a good one.
        next_candidates = []

        while len(next_candidates) < 3:
            rand_x = random.randint(0, map_size - 1)
            rand_y = random.randint(0, map_size - 1)

            if used[rand_x][rand_y]:
                continue

            gx = (sum_x + rand_x) / (len(points) + 1)
            gy = (sum_y + rand_y) / (len(points) + 1)
            d = (gx - map_size // 2) ** 2 + (gy - map_size // 2) ** 2

            next_candidates.append((d, rand_x, rand_y))

        next_candidates.sort()
        _, x, y = next_candidates[0]
        points.append((x, y))
        update_used(x, y)
        sum_x += x
        sum_y += y

    random.shuffle(points)
    return points

generated_task = []

def generate_task(random, check_point_num, min_length, max_length, conflict_rate, exclude_same_task) -> str:
    uppercases = string.ascii_uppercase[:check_point_num]

    if random.random() <= conflict_rate and len(generated_task) != 0:
        # prefix is conflicted other task
        target_task = random.choice(generated_task)
        conflict_length = random.randint(1, len(target_task)-1)
        task_prefix = target_task[conflict_length:]
        assert len(task_prefix) <= max_length
        n = random.randint(max(len(task_prefix), min_length), max_length)
        task_sufix = "".join([random.choice(uppercases) for _ in range(n-len(task_prefix))])
        task = task_prefix + task_sufix
        while exclude_same_task and task in generated_task:
            # exclude same task
            task_sufix = "".join([random.choice(uppercases) for _ in range(n-len(task_prefix))])
            task = task_prefix + task_sufix
        generated_task.append(task)
        assert len(task) >= min_length and len(task) <= max_length
        return task
    else:
        # random task
        n = random.randint(min_length, max_length)
        task = "".join([random.choice(uppercases) for _ in range(n)])
        while exclude_same_task and task in generated_task:
            # exclude same task
            task = "".join([random.choice(uppercases) for _ in range(n)])
        generated_task.append(task)
        assert len(task) >= min_length and len(task) <= max_length
        return task

def generate_init_task(random, args, tutorial_task_num) -> List[Tuple[int, int, str]]:
    init_tasks = []

    last_init_task_num = args.initial_task_num - tutorial_task_num

    # import init tasks from file
    init_file_path = args.initial_task_file_path
    if init_file_path is not None:
        with Path(init_file_path).open('r') as f:
            task_lines = f.readlines()
            for i in range(min(last_init_task_num, len(task_lines))):
                t = 0
                v = random.randint(args.task_min_score, args.task_max_score)
                # use fixed score(from file)
                if args.use_fixed_score:
                    v = args.input_file_task_fixed_score
                s = task_lines[i].rstrip().upper()
                assert len(s) != 0, "init task can't empty"
                assert re.compile(r'^[A-Z]+$').match(s), 'task must be alphabet'
                init_tasks.append((t, v, s))
            last_init_task_num -= min(last_init_task_num, len(task_lines))

    for i in range(last_init_task_num):
        t = 0
        v = random.randint(args.task_min_score, args.task_max_score)
        # use fixed score(first_fixed_score)
        if args.use_fixed_score:
            v = args.first_fixed_score

        s = generate_task(
            random,
            args.check_point_num,
            args.task_min_length,
            args.task_max_length,
            0.0,
            args.exclude_same_task
        )
        init_tasks.append((t, v, s))


    return init_tasks

def get_random_task_score(random, args, t):
    if not args.use_fixed_score:
        return random.randint(args.task_min_score, args.task_max_score)

    # use fixed score
    if t < args.first_fixed_time:
        return args.first_fixed_score

    return args.second_fixed_score

def generate(args, index):
    out_path = Path(args.maps_dir) / str(index + 1)
    random = Random(time.time())
    chara_num = random.randint(args.min_chara, args.max_chara)

    assert (args.task_num - args.initial_task_num) % args.wave_task_num == 0, 'number of tasks must be same each wave'

    wave_num = (args.task_num - args.initial_task_num) // args.wave_task_num
    task_interval = args.game_time / max(1, wave_num + 1) # don't add task at last wave

    if args.loadtest:
        task_interval = 1

    # Hard coded in API program
    agent_points = [(0, 0), (0, 30), (15, 15), (30, 0), (30, 30)]

    check_points = dict()
    for i, p in enumerate(
        generate_check_point(random, agent_points, args.map_size, args.check_point_num)
    ):
        name = chr(ord("A") + i)
        check_points[name] = p

    tasks = []
    if args.tutorial_task:
        tasks.append((0, 1, "K"))

    for init_task in generate_init_task(random, args, len(tasks)):
        tasks.append(init_task)

    for i in range(args.task_num - args.initial_task_num):
        wave = int(i // args.wave_task_num + 1)
        t = int(task_interval * wave)
        v = get_random_task_score(random, args, t)
        s = generate_task(
            random,
            args.check_point_num,
            args.task_min_length,
            args.task_max_length,
            args.task_conflict_rate,
            args.exclude_same_task
        )
        tasks.append((t, v, s))

    m = GameMap(
        game_time=args.game_time,
        map_size=args.map_size,
        agent_speed=args.chara_speed,
        agent_points=agent_points,
        check_points=check_points,
        tasks=tasks,
    )

    with out_path.open("w") as f:
        write_game_map(f, m)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--num_maps", type=int, default=1, help="map num")
    parser.add_argument("--game_time", type=int, default=3600 * 1000, help="game time(ms)")
    parser.add_argument("--min_chara", type=int, default=1, help="random min character num")
    parser.add_argument("--max_chara", type=int, default=1, help="random max character num")
    parser.add_argument("--chara_speed", type=int, default=3, help="character speed")
    parser.add_argument("--maps_dir", type=str, default=MAPS_DIR, help="output maps dir")
    parser.add_argument("--map_size", type=int, default=31, help="map size")
    parser.add_argument("--check_point_num", type=int, default=26, help="check point num (1~26)")
    parser.add_argument("--task_num", type=int, default=28, help="task num")
    parser.add_argument("--initial_task_num", type=int, default=10, help="initial task num")
    parser.add_argument("--task_min_length", type=int, default=3, help="task min length")
    parser.add_argument("--task_max_length", type=int, default=10, help="task max length")
    parser.add_argument("--task_min_score", type=int, default=10, help="task min score")
    parser.add_argument("--task_max_score", type=int, default=100, help="task max score")
    parser.add_argument("--tutorial_task", type=bool, default=True, help="add tutorial task")
    parser.add_argument("--wave_task_num", type=int, default=3, help="add task num every wave")
    parser.add_argument("--initial_task_file_path", type=str, default=None, help="load initial task from file")
    parser.add_argument("--input_file_task_fixed_score", type=int, default=100, help="task from file fixed score")
    parser.add_argument("--task_conflict_rate", type=float, default=0.0, help="rate of task prefix conflict before generated task suffix (0.0~1.0)")
    parser.add_argument("--exclude_same_task", type=bool, default=True, help="do not generate same task")
    parser.add_argument("--use_fixed_score", type=bool, default=True, help="do not use random score")
    parser.add_argument("--first_fixed_score", type=int, default=100, help="fixed score before first fixed time(exclude first_fixed_time)")
    parser.add_argument("--first_fixed_time", type=int, default=3600 * 1000, help="use first_fixed_score time(ms)")
    parser.add_argument("--second_fixed_score", type=int, default=10000, help="fixed score after first fixed time(include first_fixed_time)")
    parser.add_argument("--loadtest", type=bool, default=False, help="generate map for loadtest")
    args = parser.parse_args()

    for i in range(args.num_maps):
        generate(args, i)
