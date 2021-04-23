#include <stdlib.h>
#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <chrono>
#include <queue>
#include <random>
using namespace std;

const int GAME_INFO_SLEEP_TIME = 5000;

mt19937 mt;

struct MasterData {
	int game_period;
	int max_len_task;
	int num_agent;
	vector<pair<int, int>> checkpoints;
	int area_size;
};

struct AgentMove {
	double x;
	double y;
	int t;
};

struct Agent {
	vector<AgentMove> move;
	string history;
};

struct Task {
	string s;
	int t;
	int weight;
	int count;
	int total;
};

struct Game {
	int now;
	vector<Agent> agent;
	vector<Task> task;
	int next_task;
};

struct Move {
	int now;
	vector<AgentMove> move;
};

MasterData call_master_data() {
	cout << "master_data" << endl;
	MasterData res;
	cin >> res.game_period >> res.max_len_task >> res.num_agent;
	res.checkpoints = vector<pair<int, int>>(26);
	for (auto& c : res.checkpoints) {
		cin >> c.first >> c.second;
	}
	cin >> res.area_size;
	return res;
}

Game call_game() {
	cout << "game" << endl;
	Game res;
	int num_agent, num_task;
	cin >> res.now >> num_agent >> num_task;
	res.agent.resize(num_agent);
	for (auto& a : res.agent) {
		int num_move;
		cin >> num_move;
		a.move.resize(num_move);
		for (auto& m : a.move) {
			cin >> m.x >> m.y >> m.t;
		}
		string h;
		cin >> h;
		a.history = h.substr(1);
	}
	res.task.resize(num_task);
	for (auto& t : res.task) {
		cin >> t.s >> t.t >> t.weight >> t.count >> t.total;
	}
	cin >> res.next_task;
	return res;
}

Move read_move() {
	Move res;
	int num_move;
	cin >> res.now >> num_move;
	res.move.resize(num_move);
	for (auto& m : res.move) {
		cin >> m.x >> m.y >> m.t;
	}
	return res;
}

Move call_move(int index, int x, int y) {
	cout << "move " << index << "-" << x << "-" << y << endl;
	return read_move();
}

Move call_move_next(int index, int x, int y) {
	cout << "move_next " << index << "-" << x << "-" << y << endl;
	return read_move();
}

struct Bot {
	MasterData master_data;
	Game game_info;
	chrono::system_clock::time_point start_time;
	int start_game_time_ms;
	int next_call_game_info_time_ms;
	vector<int> agent_move_finish_ms;
	vector<queue<pair<int, int>>> agent_move_point_queue;
	vector<pair<int, int>> agent_last_point;
	Bot() {
		master_data = call_master_data();
		game_info = call_game();
		start_game_time_ms = game_info.now;
		cerr << "Start:" << start_game_time_ms << endl;
		start_time = chrono::system_clock::now();
		next_call_game_info_time_ms = get_now_game_time_ms() + GAME_INFO_SLEEP_TIME;
		agent_move_finish_ms.resize(master_data.num_agent);
		agent_move_point_queue.resize(master_data.num_agent);
		agent_last_point.resize(master_data.num_agent);
		for (int i = 0; i < master_data.num_agent; ++ i) {
			agent_last_point[i] = {(int)game_info.agent[i].move.back().x, (int)game_info.agent[i].move.back().y};
			set_move_point(i);
		}
	}
	const Task& choice_task() {
		int r = uniform_int_distribution<>(0, game_info.task.size()-1)(mt);
		return game_info.task[r];
	}
	pair<int, int> get_checkpoint(char c) {
		return master_data.checkpoints[c - 'A'];
	}
	// 移動予定を設定
	void set_move_point(int index) {
		const auto& next_task = choice_task();
		cerr << "Agent#" << index+1 << " next task:" << next_task.s << endl;

		for (char c : next_task.s) {
			auto before_point = agent_last_point[index];
			auto move_point = get_checkpoint(c);

			// 移動先が同じ場所の場合判定が入らないため別の箇所に移動してからにする
			if (before_point == move_point) {
				int tmp_x = master_data.area_size / 2;
				int tmp_y = master_data.area_size / 2;
				agent_move_point_queue[index].push({tmp_x, tmp_y});
			}

			agent_move_point_queue[index].push(move_point);
			agent_last_point[index] = move_point;
		}
	}
	int get_now_game_time_ms() {
		auto t = chrono::duration_cast<std::chrono::milliseconds>(chrono::system_clock::now() - start_time).count();
		return start_game_time_ms + t;
	}
	Move move_next(int index) {
		auto [x, y] = agent_move_point_queue[index].front();
		agent_move_point_queue[index].pop();
		auto move_next_res = call_move_next(index+1, x, y);
		cerr << "Agent#" << index+1 << " move_next to (" << x << ", " << y << ")" << endl;

		agent_move_finish_ms[index] = move_next_res.move[1].t + 100;

		// タスクを全てやりきったら次のタスクを取得
		if (agent_move_point_queue[index].empty()) {
			set_move_point(index);
		}

		return move_next_res;
	}
	double get_now_score() {
		double score = 0.0;
		for (const auto& task : game_info.task) {
			if (task.total == 0) continue;
			score += (double)(task.weight * task.count) / task.total;
		}

		return score;
	}
	void solve() {
		for (;;) {
			int now_game_time_ms = get_now_game_time_ms();

			// エージェントを移動させる
			for (int i = 0; i < master_data.num_agent; ++ i) {
				if (agent_move_finish_ms[i] < now_game_time_ms) {
					auto move_next_res = move_next(i);
					// 次の移動予定がない場合もう一度実行する
					if (move_next_res.move.size() == 2) {
						move_next(i);
					}
				}
			}

			// ゲーム情報更新
			if (next_call_game_info_time_ms < now_game_time_ms) {
				cerr << "Update GameInfo" << endl;
				game_info = call_game();
				next_call_game_info_time_ms = get_now_game_time_ms() + GAME_INFO_SLEEP_TIME;
				cerr << "Score: " << get_now_score() << endl;
			}

			this_thread::sleep_for(chrono::milliseconds(500));
		}
	}
};

int main() {
	random_device seed_gen;
	mt = mt19937(seed_gen());

	Bot bot;
	bot.solve();
}
