/*
 * タスクをランダムに取得するサンプルプログラムです。
 * 実行には C# 環境が必要です。
 * TOKEN 変数を書き換えて実行してください。
 */
using System;
using System.Net.Http;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Collections.Generic;

// 座標のクラス
public class Point
{
    [JsonPropertyName("x")]
    public int X { get; set; }
    [JsonPropertyName("y")]
    public int Y { get; set; }
}

// マスターデータ取得APIのクラス
public class MasterData
{
    [JsonPropertyName("game_period")]
    public int GemePeriod { get; set; }
    [JsonPropertyName("max_len_task")]
    public int MaxLenTask { get; set; }
    [JsonPropertyName("num_agent")]
    public int NumAgent { get; set; }
    [JsonPropertyName("checkpoints")]
    public Point[] Checkpoints { get; set; }
    [JsonPropertyName("area_size")]
    public int AreaSize { get; set; }
}

// エージェントの移動場所のクラス
public class AgentMove
{
    [JsonPropertyName("x")]
    public double X { get; set; }
    [JsonPropertyName("y")]
    public double Y { get; set; }
    [JsonPropertyName("t")]
    public int T { get; set; }
}

// エージェントのクラス
public class Agent
{
    [JsonPropertyName("move")]
    public AgentMove[] AgentMove { get; set; }
    [JsonPropertyName("hisitory")]
    public string History { get; set; }
}

// タスクのクラス
public class GameTask
{
    [JsonPropertyName("s")]
    public string S { get; set; }
    [JsonPropertyName("t")]
    public int T { get; set; }
    [JsonPropertyName("weight")]
    public int Weight { get; set; }
    [JsonPropertyName("count")]
    public int Count { get; set; }
    [JsonPropertyName("total")]
    public int Total { get; set; }
}

// ゲーム情報取得APIのクラス
public class Game
{
    [JsonPropertyName("status")]
    public string Status { get; set; }
    [JsonPropertyName("now")]
    public int Now { get; set; }
    [JsonPropertyName("agent")]
    public Agent[] Agent { get; set; }
    [JsonPropertyName("task")]
    public GameTask[] Task { get; set; }
    [JsonPropertyName("next_task")]
    public int NextTask { get; set; }
}

// 移動APIのクラス
public class Move
{
    [JsonPropertyName("status")]
    public string Status { get; set; }
    [JsonPropertyName("now")]
    public int Now { get; set; }
    [JsonPropertyName("move")]
    public AgentMove[] AgentMove { get; set; }
}

public class Bot
{
    private string GAME_SERVER;
    private string TOKEN;
    private int GameInfoSleepTime;
    static readonly HttpClient client = new HttpClient();

    public MasterData masterData;
    public Game gameInfo;
    public int startGameTimeMs;
    public Stopwatch startStopwatch;
    public int nextCallGameInfoTimeMs;
    public int[] agentMoveFinishMs;
    public Queue<Point>[] agentMovePointQueue;
    public Point[] agentLastPoint;

    async Task<byte[]> CallAPI(string x)
    {
        var res = await client.GetAsync($"{GAME_SERVER}{x}");
        return await res.Content.ReadAsByteArrayAsync();
    }

    async Task<MasterData> CallMasterData()
    {
        var json = await CallAPI("/api/master_data");
        var obj = JsonSerializer.Deserialize<MasterData>(json);
        return obj;
    }

    async Task<Game> CallGame()
    {
        var json = await CallAPI($"/api/game/{TOKEN}");
        var obj = JsonSerializer.Deserialize<Game>(json);
        return obj;
    }

    async Task<Move> CallMoveNext(int index, int x, int y)
    {
        var json = await CallAPI($"/api/move_next/{TOKEN}/{index}-{x}-{y}");
        var obj = JsonSerializer.Deserialize<Move>(json);
        return obj;
    }

    async Task<Move> CallMove(int index, int x, int y)
    {
        var json = await CallAPI($"/api/move/{TOKEN}/{index}-{x}-{y}");
        var obj = JsonSerializer.Deserialize<Move>(json);
        return obj;
    }

    public GameTask ChoiceTask()
    {
        return gameInfo.Task[new Random().Next(0, gameInfo.Task.Length)];
    }

    public int GetNowGameTimeMs()
    {
        return startGameTimeMs + (int)startStopwatch.ElapsedMilliseconds;
    }

    public Point GetCheckpoint(char name)
    {
        var index = name - 'A';
        return masterData.Checkpoints[index];
    }

    async Task Init()
    {
        startStopwatch = new Stopwatch();
        masterData = await CallMasterData();
        gameInfo = await CallGame();
        startGameTimeMs = gameInfo.Now;
        Console.WriteLine($"Start: {startGameTimeMs}");
        startStopwatch.Start();
        nextCallGameInfoTimeMs = GetNowGameTimeMs();
        agentMoveFinishMs = new int[masterData.NumAgent];
        agentMovePointQueue = new Queue<Point>[5];
        agentLastPoint = new Point[masterData.NumAgent];
        for (int i = 0; i < masterData.NumAgent; i++)
        {
            var moveLen = gameInfo.Agent[i].AgentMove.Length;
            agentMovePointQueue[i] = new Queue<Point>();
            // 最後の移動後の座標保存
            var lastMove = gameInfo.Agent[i].AgentMove[moveLen - 1];
            var lastPoint = new Point();
            lastPoint.X = (int)lastMove.X;
            lastPoint.Y = (int)lastMove.Y;
            agentLastPoint[i] = lastPoint;

            SetMovePoint(i);
        }
    }

    // 移動予定を設定
    public void SetMovePoint(int index)
    {
        var nextTask = ChoiceTask();
        Console.WriteLine($"Agent#{index+1} next task: {nextTask.S}");
        for (int i = 0; i < nextTask.S.Length; i++)
        {
            var checkpointName = nextTask.S[i];
            var beforePoint = agentLastPoint[index];
            var movePoint = GetCheckpoint(checkpointName);

            // 移動先が同じ場所の場合判定が入らないため別の箇所に移動してからにする
            if (movePoint.X == beforePoint.X && movePoint.Y == beforePoint.Y)
            {
                var tmpPoint = new Point();
                tmpPoint.X = masterData.AreaSize / 2;
                tmpPoint.Y = masterData.AreaSize / 2;
                agentMovePointQueue[index].Enqueue(tmpPoint);
            }
            agentMovePointQueue[index].Enqueue(movePoint);
            agentLastPoint[index] = movePoint;
        }
    }

    public async Task<Move> MoveNext(int index)
    {
        var moveNextPoint = agentMovePointQueue[index].Dequeue();
        var moveNextRes = await CallMoveNext(index+1, moveNextPoint.X, moveNextPoint.Y);
        Console.WriteLine($"Agent#{index+1} moveNext to ({moveNextPoint.X}, {moveNextPoint.Y})");

        if (moveNextRes.Status != "ok")
        {
            Environment.FailFast("Error Status");
        }

        if (moveNextRes.AgentMove.Length <= 1)
        {
            Environment.FailFast("Error Length");
        }

        agentMoveFinishMs[index] = moveNextRes.AgentMove[1].T + 100;

        // タスクを全てやりきったら次のタスクを取得
        if (agentMovePointQueue[index].Count == 0)
        {
            SetMovePoint(index);
        }

        return moveNextRes;
    }

    public double GetNowScore()
    {
        double score = 0.0;
        for (int i = 0; i < gameInfo.Task.Length; i++)
        {
            var nowTask = gameInfo.Task[i];
            if (nowTask.Total == 0)
            {
                continue;
            }
            score += (double)(nowTask.Weight * nowTask.Count) / (double)nowTask.Total;
        }

        return score;
    }

    public async Task Solve()
    {
        while(true)
        {
            var nowGameTime = GetNowGameTimeMs();

            // エージェントを移動させる
            for (int i = 0; i < masterData.NumAgent; i++)
            {
                if (agentMoveFinishMs[i] < nowGameTime)
                {
                    var moveNextRes = await MoveNext(i);
                    // 次の移動予定がない場合はもう一度実行する
                    if (moveNextRes.AgentMove.Length == 2)
                    {
                        await MoveNext(i);
                    }
                }
            }

            if (nextCallGameInfoTimeMs < nowGameTime)
            {
                Console.WriteLine("Update GameInfo");
                gameInfo = await CallGame();
                nextCallGameInfoTimeMs = GetNowGameTimeMs() + GameInfoSleepTime;
                Console.WriteLine($"Score: {GetNowScore()}");
            }

            await Task.Delay(500);
        }
    }

    public static void Main(string[] args)
    {
        var bot = new Bot();
        bot.GAME_SERVER = Environment.GetEnvironmentVariable("GAME_SERVER") ?? "https://contest.2021-spring.gbc.tenka1.klab.jp";
        bot.TOKEN = Environment.GetEnvironmentVariable("TOKEN") ?? "YOUR_TOKEN";
        bot.GameInfoSleepTime = 5000;
        bot.Init().Wait();
        bot.Solve().Wait();
    }
}
