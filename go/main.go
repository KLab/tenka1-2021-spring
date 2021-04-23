/*
タスクをランダムに取得するサンプルプログラムです。
実行には go 環境が必要です。
TOKEN 変数を書き換えて実行してください。
*/

package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"time"
)

// GameServer ゲームサーバのアドレス
var GameServer string

// TOKEN あなたのトークン
var TOKEN string

// GameInfoSleepTime GameAPIを呼ぶ際のインターバル
var GameInfoSleepTime int

func callAPI(x string) []byte {
	resp, err := http.Get(GameServer + x)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		log.Fatal(resp.Status)
	}
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
	}
	return data
}

// Point 座標の構造体
type Point struct {
	X int `json:"x"`
	Y int `json:"y"`
}

// MasterData マスタデータ取得APIの構造体
type MasterData struct {
	GamePeriod  int     `json:"game_period"`
	MaxLenTask  int     `json:"max_len_task"`
	NumAgent    int     `json:"num_agent"`
	Checkpoints []Point `json:"checkpoints"`
	AreaSize    int     `json:"area_size"`
}

// AgentMove エージェントの移動場所の構造体
type AgentMove struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
	T int     `json:"t"`
}

// Agent エージェントの構造体
type Agent struct {
	Move    []AgentMove `json:"move"`
	History string      `json:"history"`
}

// Task タスクの構造体
type Task struct {
	S      string `json:"s"`
	T      int    `json:"t"`
	Weight int    `json:"weight"`
	Count  int    `json:"count"`
	Total  int    `json:"total"`
}

// Game ゲーム情報取得APIの構造体
type Game struct {
	Status string  `json:"status"`
	Now    int     `json:"now"`
	Agent  []Agent `json:"agent"`
	Task   []Task  `json:"task"`
}

// Move 移動APIの構造体
type Move struct {
	Status string      `json:"status"`
	Now    int         `json:"now"`
	Move   []AgentMove `json:"move"`
}

func callMasterData() MasterData {
	var masterData MasterData
	res := callAPI("/api/master_data")
	if err := json.Unmarshal(res, &masterData); err != nil {
		log.Fatal(err)
	}
	return masterData
}

func callGame() Game {
	var game Game
	res := callAPI(fmt.Sprintf("/api/game/%s", TOKEN))
	if err := json.Unmarshal(res, &game); err != nil {
		log.Fatal(err)
	}
	return game
}

func callMoveNext(index int, x int, y int) Move {
	var move Move
	res := callAPI(fmt.Sprintf("/api/move_next/%s/%d-%d-%d", TOKEN, index, x, y))
	if err := json.Unmarshal(res, &move); err != nil {
		log.Fatal(err)
	}
	return move
}

func callMove(index int, x int, y int) Move {
	var move Move
	res := callAPI(fmt.Sprintf("/api/move/%s/%d-%d-%d", TOKEN, index, x, y))
	if err := json.Unmarshal(res, &move); err != nil {
		log.Fatal(err)
	}
	return move
}

// Bot ボットの構造体
type Bot struct {
	MasterData             MasterData
	GameInfo               Game
	StartGameTimeMs        int
	StartTimeMs            int64
	NextCallGameInfoTimeMs int
	AgentMoveFinisMs       []int
	AgentMovePointQueue    [][]Point
	AgentLastPoint         []Point
}

// NewBot 初期化したBotを取得します
func NewBot() *Bot {
	bot := new(Bot)
	bot.MasterData = callMasterData()
	bot.GameInfo = callGame()
	bot.StartGameTimeMs = bot.GameInfo.Now
	fmt.Println(fmt.Sprintf("Start: %d", bot.StartGameTimeMs))
	bot.StartTimeMs = time.Now().UTC().UnixNano() / int64(time.Millisecond)
	bot.NextCallGameInfoTimeMs = bot.getNowGameTimeMs() + GameInfoSleepTime
	for i := 0; i < bot.MasterData.NumAgent; i++ {
		bot.AgentMoveFinisMs = append(bot.AgentMoveFinisMs, 0)
		moveLen := len(bot.GameInfo.Agent[i].Move)
		// 移動予定のqueueを初期化
		bot.AgentMovePointQueue = append(bot.AgentMovePointQueue, nil)

		// 最後の移動後の座標保存
		lastMove := bot.GameInfo.Agent[i].Move[moveLen-1]
		lastPoint := Point{}
		lastPoint.X = int(lastMove.X)
		lastPoint.Y = int(lastMove.Y)
		bot.AgentLastPoint = append(bot.AgentLastPoint, lastPoint)

		bot.setMovePoint(i)
	}

	return bot
}

func (bot Bot) choiceTask() Task {
	index := rand.Intn(len(bot.GameInfo.Task))
	return bot.GameInfo.Task[index]
}

func (bot Bot) getNowGameTimeMs() int {
	nowMs := time.Now().UTC().UnixNano() / int64(time.Millisecond)
	return bot.StartGameTimeMs + (int)(nowMs-bot.StartTimeMs)
}

func (bot Bot) getCheckpoint(name rune) Point {
	index := int(name - 'A')
	return bot.MasterData.Checkpoints[index]
}

// 移動予定を設定
func (bot Bot) setMovePoint(index int) {
	nextTask := bot.choiceTask()
	fmt.Println(fmt.Sprintf("Agent#%d next task: %s", index, nextTask.S))
	for _, checkpointName := range nextTask.S {
		beforePoint := bot.AgentLastPoint[index]
		movePoint := bot.getCheckpoint(checkpointName)

		// 移動先が同じ場所の場合判定が入らないため別の箇所に移動してからにする
		if movePoint.X == beforePoint.X && movePoint.Y == beforePoint.Y {
			tmpPoint := Point{}
			tmpPoint.X = bot.MasterData.AreaSize / 2
			tmpPoint.Y = bot.MasterData.AreaSize / 2
			bot.AgentMovePointQueue[index] = append(bot.AgentMovePointQueue[index], tmpPoint)
		}

		bot.AgentMovePointQueue[index] = append(bot.AgentMovePointQueue[index], movePoint)
		bot.AgentLastPoint[index] = movePoint
	}
}

func (bot Bot) moveNext(index int) Move {
	moveNextPoint := bot.AgentMovePointQueue[index][0]
	bot.AgentMovePointQueue[index] = bot.AgentMovePointQueue[index][1:]
	moveNextRes := callMoveNext(index+1, moveNextPoint.X, moveNextPoint.Y)
	fmt.Println(fmt.Sprintf("Agent#%d moveNext to (%d, %d)", index+1, moveNextPoint.X, moveNextPoint.Y))

	if moveNextRes.Status != "ok" {
		log.Fatal(moveNextRes.Status)
	}

	if len(moveNextRes.Move) <= 1 {
		log.Fatal(len(moveNextRes.Move))
	}

	bot.AgentMoveFinisMs[index] = moveNextRes.Move[1].T + 100

	// タスクを全てやりきったら次のタスクを取得
	if len(bot.AgentMovePointQueue[index]) == 0 {
		bot.setMovePoint(index)
	}

	return moveNextRes
}

func (bot Bot) getNowScore() float64 {
	score := 0.0
	for _, task := range bot.GameInfo.Task {
		if task.Total == 0 {
			continue
		}
		score += float64(task.Weight*task.Count) / float64(task.Total)
	}

	return score
}

func (bot Bot) solve() {
	for {
		nowGameTimeMs := bot.getNowGameTimeMs()

		// エージェントを移動させる
		for i := 0; i < bot.MasterData.NumAgent; i++ {
			if bot.AgentMoveFinisMs[i] < nowGameTimeMs {
				moveNextRes := bot.moveNext(i)
				// 次の移動予定がない場合もう一度実行する
				if len(moveNextRes.Move) == 2 {
					bot.moveNext(i)
				}
			}
		}

		if bot.NextCallGameInfoTimeMs < nowGameTimeMs {
			fmt.Println("Update GameInfo")
			bot.GameInfo = callGame()
			bot.NextCallGameInfoTimeMs = bot.getNowGameTimeMs() + GameInfoSleepTime
			fmt.Println(fmt.Sprintf("Score: %f", bot.getNowScore()))
		}

		time.Sleep(time.Millisecond * 500)
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())
	GameServer = os.Getenv("GAME_SERVER")
	TOKEN = os.Getenv("TOKEN")
	if GameServer == "" {
		GameServer = "https://contest.2021-spring.gbc.tenka1.klab.jp"
	}
	if TOKEN == "" {
		TOKEN = "YOUR_TOKEN"
	}
	GameInfoSleepTime = 5000

	bot := NewBot()
	bot.solve()
}
