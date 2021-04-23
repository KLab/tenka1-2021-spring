"""
タスクをランダムに取得するサンプルプログラムです。
実行には python3 環境が必要です。
TOKEN 変数を書き換えて実行してください。
"""

import os
import random
import time
import json
from typing import Tuple
from urllib.request import urlopen

# ゲームサーバのアドレス
GAME_SERVER = os.getenv('GAME_SERVER', 'https://contest.2021-spring.gbc.tenka1.klab.jp')
# あなたのトークン
TOKEN = os.getenv('TOKEN', 'YOUR_TOKEN')
# GameAPIを呼ぶ際のインターバル
GAME_INFO_SLEEP_TIME = 5000


def call_api(x: str) -> bytes:
    with urlopen(f'{GAME_SERVER}{x}') as res:
        return res.read()


def call_move(index: int, x: int, y: int) -> dict:
    res_str = call_api(f'/api/move/{TOKEN}/{index}-{x}-{y}')
    res_json = json.loads(res_str)
    return res_json


def call_move_next(index: int, x: int, y: int) -> dict:
    res_str = call_api(f'/api/move_next/{TOKEN}/{index}-{x}-{y}')
    res_json = json.loads(res_str)
    return res_json


def call_game() -> dict:
    res_str = call_api(f'/api/game/{TOKEN}')
    res_json = json.loads(res_str)
    return res_json


def call_master_data() -> dict:
    res_str = call_api(f'/api/master_data')
    res_json = json.loads(res_str)
    return res_json


class Bot:
    def __init__(self):
        self.master_data = call_master_data()
        self.game_info = call_game()
        self.start_game_time_ms = self.game_info['now']
        print("Start:", self.start_game_time_ms)
        self.start_time_ms = time.perf_counter_ns() // 1000000
        self.next_call_game_info_time_ms = self.get_now_game_time_ms() + GAME_INFO_SLEEP_TIME
        self.agent_move_finish_ms = [0] * self.master_data['num_agent']
        self.agent_move_point_queue = [[] for _ in range(self.master_data['num_agent'])]
        self.agent_last_point = [[] for _ in range(self.master_data['num_agent'])]
        for i in range(self.master_data['num_agent']):
            agent_move = self.game_info['agent'][i]['move']
            self.agent_last_point[i] = [agent_move[-1]['x'], agent_move[-1]['y']]
            self.set_move_point(i)

    # タスクを取得
    def choice_task(self) -> dict:
        tasks = self.game_info['task']
        return random.choice(tasks)

    def get_now_game_time_ms(self) -> int:
        now_ms = time.perf_counter_ns() // 1000000
        return self.start_game_time_ms + (now_ms - self.start_time_ms)

    def get_checkpoint(self, name: str) -> Tuple[int, int]:
        index = ord(name) - ord('A')
        checkpoint = self.master_data['checkpoints'][index]
        return checkpoint['x'], checkpoint['y']

    # 移動予定を設定
    def set_move_point(self, index: int):
        next_task = self.choice_task()
        print("Agent#{} next task:{}".format(index, next_task['s']))
        for i in range(len(next_task['s'])):
            before_x = self.agent_last_point[index][0]
            before_y = self.agent_last_point[index][1]
            move_x, move_y = self.get_checkpoint(next_task['s'][i])

            # 移動先が同じ場所の場合判定が入らないため別の箇所に移動してからにする
            if move_x == before_x and move_y == before_y:
                tmp_x = self.master_data['area_size'] // 2
                tmp_y = self.master_data['area_size'] // 2
                self.agent_move_point_queue[index].append([tmp_x, tmp_y])

            self.agent_move_point_queue[index].append([move_x, move_y])
            self.agent_last_point[index] = [move_x, move_y]

    def move_next(self, index: int) -> dict:
        move_next_point = self.agent_move_point_queue[index].pop(0)
        move_next_res = call_move_next(index+1, move_next_point[0], move_next_point[1])
        print("Agent#{} move_next to ({}, {})".format(index+1, move_next_point[0], move_next_point[1]))

        if move_next_res['status'] != 'ok':
            print(move_next_res)
            exit(1)

        assert len(move_next_res['move']) > 1

        self.agent_move_finish_ms[index] = move_next_res['move'][1]['t'] + 100

        # タスクを全てやりきったら次のタスクを取得
        if not self.agent_move_point_queue[index]:
            self.set_move_point(index)

        return move_next_res

    # game_infoの状態でのスコアを取得
    def get_now_score(self) -> float:
        score = 0.0
        tasks = self.game_info['task']
        for i in range(len(tasks)):
            if tasks[i]['total'] == 0:
                continue
            score += tasks[i]['weight'] * tasks[i]['count'] / tasks[i]['total']

        return score

    def solve(self):
        while True:
            now_game_time_ms = self.get_now_game_time_ms()

            # エージェントを移動させる
            for i in range(self.master_data['num_agent']):
                if self.agent_move_finish_ms[i] < now_game_time_ms:
                    move_next_res = self.move_next(i)
                    # 次の移動予定がない場合もう一度実行する
                    if len(move_next_res['move']) == 2:
                        self.move_next(i)

            # ゲーム情報更新
            if self.next_call_game_info_time_ms < now_game_time_ms:
                print('Update GameInfo')
                self.game_info = call_game()
                self.next_call_game_info_time_ms = self.get_now_game_time_ms() + GAME_INFO_SLEEP_TIME
                print("Score: {}".format(self.get_now_score()))

            time.sleep(0.5)


if __name__ == "__main__":
    bot = Bot()
    bot.solve()
