using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace api
{
    internal static class Program
    {
        private const string HealthEndpoint = "/api/health";
        private const string GameEndpoint = "/api/game/";
        private const string MoveEndpoint = "/api/move/";
        private const string MoveNextEndpoint = "/api/move_next/";
        private const string TaskEndpoint = "/api/task/";
        private const string MasterDataEndpoint = "/api/master_data";
        private const string EventEndpoint = "/event/";

        private static readonly byte[] SseHeader = Encoding.UTF8.GetBytes("data: ");
        private static readonly byte[] SseFooter = Encoding.UTF8.GetBytes("\n\n");
        private static readonly byte[] SsePing = Encoding.UTF8.GetBytes("event: ping\ndata: \n\n");

        private static readonly Regex reNum = new(@"^([0-9]|[1-9][0-9]{1,8})$", RegexOptions.Compiled);
        private static readonly Regex reToken = new(@"^[0-9a-z]+$", RegexOptions.Compiled);

        private static HttpListener listener;
        private static ConnectionMultiplexer redis;
        private static LoadedLuaScript runGameScript;
        private static LoadedLuaScript runMoveScript;
        private static LoadedLuaScript runTimeLimitScript;

        private static long GameStartTime;
        private static long GamePeriod;
        private static int maxLenTask;
        private static ImmutableList<object> CheckpointsList;
        private static ImmutableDictionary<(int, int), char> CheckpointsDict;
        private static ImmutableList<(string, int, int)> TaskList;
        private const int NumAgent = 5;
        private const int AreaSize = 30;
        private const int NumRanking = 10;
        private static byte[] MasterDataJson;

        private const int SSETimeLimit = 1000;
        private const int GameTimeLimit = 1000;
        private const int MoveTimeLimit = 100;
        private const int TaskTimeLimit = 500;
        private const int TaskWaitTime = 4000;
        private const int RankingPeriod = 60000;

        public static void Main(string[] args)
        {
            var calcScoreMode = true;
            var apiMode = true;
            int? startOffset = null;
            if (args.Length >= 1)
            {
                switch (args[0])
                {
                    case "calcScore":
                        apiMode = false;
                        break;
                    case "api":
                        calcScoreMode = false;
                        break;
                    case "-":
                        break;
                    default:
                        throw new Exception("invalid args[0] (mode)");
                }
            }

            if (args.Length >= 2)
            {
                if (args[1].StartsWith("+"))
                {
                    startOffset = int.Parse(args[1]);
                }
                else if (args[1] != "-")
                {
                    throw new Exception("invalid args[1] (startOffset)");
                }
            }

            var GameDBHost = Environment.GetEnvironmentVariable("GAMEDB_HOST");
            if (String.IsNullOrEmpty(GameDBHost))
            {
                GameDBHost = "localhost";
            }

            var GameDBPort = Environment.GetEnvironmentVariable("GAMEDB_PORT");
            if (String.IsNullOrEmpty(GameDBPort))
            {
                GameDBPort = "6379";
            }

            var redisConfig = ConfigurationOptions.Parse($"{GameDBHost}:{GameDBPort}");
            redis = ConnectionMultiplexer.Connect(redisConfig);
            LoadMasterData(startOffset);

            if (apiMode)
            {
                LoadRedisLuaScript(redisConfig);
                InitializeGameData();
                listener = new HttpListener();
                listener.Prefixes.Add("http://*:8080/");
                listener.Start();
                listener.BeginGetContext(OnRequest, listener);
            }

            Console.WriteLine("started.");

            if (calcScoreMode)
            {
                new Thread(CalcScore).Start();
            }

            Thread.Sleep(Timeout.Infinite);

            // listener.Stop();
            // listener.Close();
        }

        private static void LoadRedisLuaScript(ConfigurationOptions redisConfig)
        {
            var gamePrepared = LuaScript.Prepare(@"
local user_id = @userId
local now = tonumber(@now)
local num_agent = tonumber(@NumAgent)

local function get_agent_val(idx, n)
  local v = redis.call('hget', 'agent_'..user_id..'_'..idx, n)
  if v then
    return tonumber(v)
  else
    return false
  end
end

local function string_table(a)
  local r = {}
  for _, v in ipairs(a) do
    table.insert(r, tostring(v))
  end
  return r
end

local res = {}
local histories = {}
local task_cnt = redis.call('hgetall', 'task_cnt_'..user_id)
for i = 1, num_agent do
  local history = redis.call('lrange', 'history_'..user_id..'_'..tostring(i), 0, -1)
  local x0 = get_agent_val(i, 'x0')
  local y0 = get_agent_val(i, 'y0')
  local t0 = get_agent_val(i, 't0')
  local x1 = get_agent_val(i, 'x1')
  local y1 = get_agent_val(i, 'y1')
  local t1 = get_agent_val(i, 't1')
  local x2 = get_agent_val(i, 'x2')
  local y2 = get_agent_val(i, 'y2')
  local t2 = get_agent_val(i, 't2')
  if t1 and now < t1 then
    if t2 then
      table.insert(res, string_table({x0, y0, t0, x1, y1, t1, x2, y2, t2}))
    else
      table.insert(res, string_table({x0, y0, t0, x1, y1, t1}))
    end
  elseif t2 and now < t2 then
    table.insert(res, string_table({x1, y1, t1, x2, y2, t2}))
    local name1 = redis.call('hget', 'checkpoint', tostring(x1)..'-'..tostring(y1))
    if name1 then
      table.insert(history, name1)
      table.insert(history, t1)
    end
  else
    if t2 then
      table.insert(res, string_table({x2, y2, t2}))
      local name1 = redis.call('hget', 'checkpoint', tostring(x1)..'-'..tostring(y1))
      if name1 then
        table.insert(history, name1)
        table.insert(history, t1)
      end
      local name2 = redis.call('hget', 'checkpoint', tostring(x2)..'-'..tostring(y2))
      if name2 then
        table.insert(history, name2)
        table.insert(history, t2)
      end
    elseif t1 then
      table.insert(res, string_table({x1, y1, t1}))
      local name1 = redis.call('hget', 'checkpoint', tostring(x1)..'-'..tostring(y1))
      if name1 then
        table.insert(history, name1)
        table.insert(history, t1)
      end
    else
      local start_pos = ({{0, 0}, {0, 30}, {15, 15}, {30, 0}, {30, 30}})[i]
      table.insert(res, string_table({start_pos[1], start_pos[2], now}))
    end
  end
  table.insert(histories, history)
end
local sum_counter = redis.call('hgetall', 'sum_counter')
return {res, histories, task_cnt, sum_counter}
");
            var movePrepared = LuaScript.Prepare(@"
local user_id = @userId
local idx = @idx
local x = tonumber(@x)
local y = tonumber(@y)
local now = tonumber(@now)
local next = @next ~= '0'
local max_len_task = tonumber(@maxLenTask)

local function get_agent_val(n)
  local v = redis.call('hget', 'agent_'..user_id..'_'..idx, n)
  if v then
    return tonumber(v)
  else
    return false
  end
end

local function calc_cost(xx, yy)
  return math.max(1, math.ceil(((x-xx)*(x-xx) + (y-yy)*(y-yy))^0.5 * 100))
end

local function move_immediately(xx, yy)
  local t = now + calc_cost(xx, yy)
  redis.call('hset', 'agent_'..user_id..'_'..idx, 'x0', xx, 'y0', yy, 't0', now, 'x1', x, 'y1', y, 't1', t)
  redis.call('hdel', 'agent_'..user_id..'_'..idx, 'x2', 'y2', 't2')
  return {xx, yy, now, x, y, t}
end

local function push_history(xx, yy, tt)
  local name = redis.call('hget', 'checkpoint', tostring(xx)..'-'..tostring(yy))
  if name then
    local redis_key = 'history_'..user_id..'_'..idx
    local len_history = redis.call('rpush', redis_key, name, tt)
    redis.call('rpush', 'history2_'..user_id..'_'..idx, name, tt)
    if len_history > 2 * max_len_task then
      local history = redis.call('lrange', redis_key, 0, -1)
      local history_str = ''
      local ti = tonumber(history[2])
      for i = 1, len_history, 2 do
        history_str = history_str..history[i]
        local tt = redis.call('hget', 'task_time', history_str)
        if tt and ti >= tonumber(tt) then
          redis.call('hincrby', 'task_cnt_'..user_id, history_str, 1)
        end
      end
      redis.call('lpop', redis_key)
      redis.call('lpop', redis_key)
    end
  end
end

local x0 = get_agent_val('x0')
local y0 = get_agent_val('y0')
local t0 = get_agent_val('t0')
local x1 = get_agent_val('x1')
local y1 = get_agent_val('y1')
local t1 = get_agent_val('t1')
local x2 = get_agent_val('x2')
local y2 = get_agent_val('y2')
local t2 = get_agent_val('t2')
local res = {}
if t1 and now < t1 then
  if next then
    if x == x1 and y == y1 then
      redis.call('hdel', 'agent_'..user_id..'_'..idx, 'x2', 'y2', 't2')
      res = {x0, y0, t0, x1, y1, t1}
    else
      local t = t1 + calc_cost(x1, y1)
      redis.call('hset', 'agent_'..user_id..'_'..idx, 'x2', x, 'y2', y, 't2', t)
      res = {x0, y0, t0, x1, y1, t1, x, y, t}
    end
  else
    local xx = (x0*(t1-now)+x1*(now-t0))/(t1-t0)
    local yy = (y0*(t1-now)+y1*(now-t0))/(t1-t0)
    res = move_immediately(xx, yy)
  end
elseif t2 and now < t2 then
  push_history(x1, y1, t1)
  if next then
    if x == x2 and y == y2 then
      redis.call('hset', 'agent_'..user_id..'_'..idx, 'x0', x1, 'y0', y1, 't0', t1, 'x1', x2, 'y1', y2, 't1', t2)
      redis.call('hdel', 'agent_'..user_id..'_'..idx, 'x2', 'y2', 't2')
      res = {x1, y1, t1, x2, y2, t2}
    else
      local t = t2 + calc_cost(x2, y2)
      redis.call('hset', 'agent_'..user_id..'_'..idx, 'x0', x1, 'y0', y1, 't0', t1, 'x1', x2, 'y1', y2, 't1', t2, 'x2', x, 'y2', y, 't2', t)
      res = {x1, y1, t1, x2, y2, t2, x, y, t}
    end
  else
    local xx = (x1*(t2-now)+x2*(now-t1))/(t2-t1)
    local yy = (y1*(t2-now)+y2*(now-t1))/(t2-t1)
    res = move_immediately(xx, yy)
  end
else
  if t2 then
    if x == x2 and y == y2 then
      res = {x, y, now}
    else
      push_history(x1, y1, t1)
      push_history(x2, y2, t2)
      res = move_immediately(x2, y2)
    end
  elseif t1 then
    if x == x1 and y == y1 then
      res = {x, y, now}
    else
      push_history(x1, y1, t1)
      res = move_immediately(x1, y1)
    end
  else
    local start_pos = ({{0, 0}, {0, 30}, {15, 15}, {30, 0}, {30, 30}})[tonumber(idx)]
    if x == start_pos[1] and y == start_pos[2] then
      res = {x, y, now}
    else
      res = move_immediately(start_pos[1], start_pos[2])
    end
  end
end
local r = {}
for _, v in ipairs(res) do
  table.insert(r, tostring(v))
end
redis.call('publish', user_id, 'M'..idx..' '..tostring(now)..' '..table.concat(r, ' '))
return r
");
            var timeLimitPrepared = LuaScript.Prepare(@"
local field = @field
local now = tonumber(@now)
local time_limit = tonumber(@timeLimit)

local t = redis.call('hget', 'unlock_time', field)
if t and now < tonumber(t) then
  return tostring(t)
end
redis.call('hset', 'unlock_time', field, now + time_limit)
return 'ok'
");
            Debug.Assert(redisConfig.EndPoints.Count == 1);
            var server = redis.GetServer(redisConfig.EndPoints[0]);
            runGameScript = gamePrepared.Load(server);
            runMoveScript = movePrepared.Load(server);
            runTimeLimitScript = timeLimitPrepared.Load(server);
        }

        private static void LoadMasterData(int? startOffset)
        {
            var db = redis.GetDatabase();
            if (startOffset.HasValue)
            {
                db.StringSet("start_at", GetTime() + startOffset.Value);
            }

            var startAt = db.StringGet("start_at");
            if (startAt.IsNull)
            {
                throw new Exception("start_at is not set");
            }

            var period = db.StringGet("period");
            if (period.IsNull)
            {
                if (startOffset.HasValue)
                {
                    db.StringSet("period", 1000000);
                    period = db.StringGet("period");
                }
                else
                {
                    throw new Exception("period is not set");
                }
            }

            GameStartTime = long.Parse(startAt.ToString());
            GamePeriod = long.Parse(period.ToString());
            var checkpoint = new Dictionary<char, object>();
            var checkpointDict = new Dictionary<(int, int), char>();
            foreach (var p in db.HashGetAll("checkpoint"))
            {
                var k = p.Name.ToString().Split("-");
                if (k.Length != 2)
                {
                    throw new Exception($"invalid checkpoint {p.Name}");
                }

                var name = p.Value.ToString();
                if (name.Length != 1 || checkpoint.ContainsKey(name[0]))
                {
                    throw new Exception($"invalid checkpoint {p.Value}");
                }

                var x = int.Parse(k[0]);
                var y = int.Parse(k[1]);
                checkpoint.Add(name[0], new {x, y});
                checkpointDict[(x, y)] = name[0];
            }

            Debug.Assert(checkpoint.Count == 26);
            var checkpointList = new List<object>();
            for (var c = 'A'; c <= 'Z'; c++)
            {
                if (!checkpoint.ContainsKey(c))
                {
                    throw new Exception($"checkpoint {c} not exist");
                }

                checkpointList.Add(checkpoint[c]);
            }

            CheckpointsList = checkpointList.ToImmutableList();
            CheckpointsDict = checkpointDict.ToImmutableDictionary();

            var task = new List<(string, int, int)>();
            var tt = 0;
            maxLenTask = 0;
            foreach (var v in db.ListRange("task"))
            {
                var a = v.ToString().Split(" ");
                Debug.Assert(a.Length == 3);
                Debug.Assert(a[0].All(c => 'A' <= c && c <= 'Z'));
                var t = int.Parse(a[1]);
                var w = int.Parse(a[2]);
                Debug.Assert(t >= tt);
                task.Add((a[0], t, w));
                tt = t;
                var r = db.HashGet("task_time", a[0]);
                if ((int) r != t)
                {
                    throw new Exception($"task_time invalid : {v} {r}");
                }
                maxLenTask = Math.Max(maxLenTask, a[0].Length);
            }

            if (db.HashLength("task_time") != task.Count)
            {
                throw new Exception("HLEN task_time invalid");
            }

            TaskList = task.ToImmutableList();

            MasterDataJson = JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, object>
            {
                ["game_period"] = GamePeriod,
                ["max_len_task"] = maxLenTask,
                ["num_agent"] = NumAgent,
                ["checkpoints"] = CheckpointsList,
                ["area_size"] = AreaSize,
            });
        }

        private static void InitializeGameData()
        {
            var db = redis.GetDatabase();
            if (db.StringGet("initialize_game_data").IsNull)
            {
                return;
            }

            foreach (var x in db.ListRange("ranking_times"))
            {
                db.KeyDelete($"ranking_{x}");
                db.KeyDelete($"sum_counter_{x}");
            }
            db.KeyDelete("ranking_times");
            db.KeyDelete("unlock_time");
            db.KeyDelete("sum_counter");
            foreach (var x in db.HashValues("user_token"))
            {
                var userId = x.ToString();
                db.KeyDelete($"task_cnt_{userId}");
                for (var idx = 1; idx <= NumAgent; idx++)
                {
                    db.KeyDelete($"history_{userId}_{idx}");
                    db.KeyDelete($"history2_{userId}_{idx}");
                    db.KeyDelete($"agent_{userId}_{idx}");
                }
            }
        }

        private class ScoreCalc
        {
            private readonly string _userId;
            private readonly List<string> _historyStr;
            private readonly List<List<int>> _historyTime;
            private readonly int[] _historyIndex;
            private readonly List<int> _taskCounter;

            public ScoreCalc(string userId)
            {
                _userId = userId;
                _historyStr = new List<string>();
                _historyTime = new List<List<int>>();
                _historyIndex = new int[NumAgent];
                _taskCounter = new List<int>();
                for (var idx = 1; idx <= NumAgent; idx++)
                {
                    _historyStr.Add("");
                    _historyTime.Add(new List<int>());
                }

                foreach (var _ in TaskList)
                {
                    _taskCounter.Add(0);
                }
            }

            public ImmutableList<int> Calc(IDatabase db, long now)
            {
                var counter = new List<int>();
                for (var i = 0; i < TaskList.Count; i++)
                {
                    var (_, t, _) = TaskList[i];
                    if (t > now) break;
                    counter.Add(_taskCounter[i]);
                }

                var agentTasks = new Task<RedisValue[]>[NumAgent];
                var historyTasks = new Task<RedisValue[]>[NumAgent];
                for (var idx = 1; idx <= NumAgent; idx++)
                {
                    agentTasks[idx - 1] = db.HashGetAsync($"agent_{_userId}_{idx}", new RedisValue[]{"x1", "y1", "t1", "x2", "y2", "t2"});
                    historyTasks[idx - 1] = db.ListRangeAsync($"history2_{_userId}_{idx}", _historyIndex[idx - 1]);
                }

                for (var idx = 1; idx <= NumAgent; idx++)
                {
                    var agent = db.Wait(agentTasks[idx - 1]);
                    var history = db.Wait(historyTasks[idx - 1]);
                    for (var i = 0; i + 1 < history.Length; i += 2)
                    {
                        _historyStr[idx-1] += history[i].ToString();
                        _historyTime[idx-1].Add(int.Parse(history[i+1].ToString()));
                        _historyIndex[idx-1] += 2;
                    }

                    var times = new List<int>();
                    for (var i = 0; i < _historyTime[idx-1].Count; i++)
                    {
                        var t = _historyTime[idx - 1][i];
                        if (t >= now) break;
                        times.Add(t);
                    }

                    var historyStr = _historyStr[idx-1].Substring(0, times.Count);
                    var offset = Math.Max(0, historyStr.Length - maxLenTask);
                    for (var i = 0; i < 6; i += 3)
                    {
                        if (agent[i + 2].IsNull) continue;
                        var x = int.Parse(agent[i]);
                        var y = int.Parse(agent[i+1]);
                        var t = int.Parse(agent[i+2]);
                        if (t >= now || times.Contains(t) || !CheckpointsDict.ContainsKey((x, y))) continue;
                        times.Add(t);
                        historyStr += CheckpointsDict[(x, y)];
                    }

                    if (historyStr.Length <= 0) continue;

                    for (var i = 0; i < counter.Count; i++)
                    {
                        var (s, t, _) = TaskList[i];
                        var p = 0;
                        while (p < times.Count && times[p] < t) p++;
                        for (;;)
                        {
                            p = historyStr.IndexOf(s, p, StringComparison.Ordinal);
                            if (p == -1) break;
                            if (p < offset) _taskCounter[i]++;
                            counter[i]++;
                            p++;
                        }
                    }

                    if (offset > 0)
                    {
                        _historyStr[idx-1] = _historyStr[idx-1].Substring(offset);
                        _historyTime[idx-1].RemoveRange(0, offset);
                    }
                }

                return counter.ToImmutableList();
            }
        }

        private static void CalcScore()
        {
            long lastRankingTime = -(1<<30);
            var db = redis.GetDatabase();
            var calcDict = new Dictionary<string, ScoreCalc>();
            for (;;)
            {
                var now = GetTime() - GameStartTime - 1000;
                if (now >= 0)
                {
                    Console.WriteLine($"{now}");

                    var counters = new Dictionary<string, ImmutableList<int>>();
                    foreach (var userIdValue in db.HashValues("user_token"))
                    {
                        var userId = (string)userIdValue;
                        if (counters.ContainsKey(userId)) continue;
                        if (!calcDict.ContainsKey(userId))
                        {
                            calcDict.Add(userId, new ScoreCalc(userId));
                        }

                        counters.Add(userId, calcDict[userId].Calc(db, now));
                    }

                    var sumCounter = new List<int>();
                    foreach (var a in counters.Values)
                    {
                        if (sumCounter.Count == 0)
                        {
                            sumCounter = new List<int>(a);
                        }
                        else
                        {
                            Debug.Assert(a.Count == sumCounter.Count);
                            for (var i = 0; i < a.Count; i++)
                            {
                                sumCounter[i] += a[i];
                            }
                        }
                    }

                    var scores = new Dictionary<string, List<double>>();
                    for (var i = 0; i < TaskList.Count; i++)
                    {
                        var (_, t, w) = TaskList[i];
                        if (t > now) break;
                        foreach (var (userId, counter) in counters)
                        {
                            if (counter[i] > 0)
                            {
                                if (!scores.ContainsKey(userId)) scores.Add(userId, new List<double>());
                                scores[userId].Add((double)w * counter[i] / sumCounter[i]);
                            }
                        }
                    }

                    var ranking = new List<(double, string)>();
                    foreach (var (userId, a) in scores)
                    {
                        while (a.Count > 1)
                        {
                            a.Sort();
                            a[0] += a[1];
                            a.RemoveAt(1);
                        }

                        ranking.Add((a[0], userId));
                    }

                    var rankingDataForRedis = ranking.Select((x, _) => new SortedSetEntry(x.Item2, x.Item1)).ToArray();

                    ranking.Sort((l, r) =>
                    {
                        var (ls, ln) = l;
                        var (rs, rn) = r;
                        var x = ls.CompareTo(rs);
                        if (x != 0) return -x;
                        return string.Compare(ln, rn, StringComparison.Ordinal);
                    });

                    foreach (var (userId, _) in counters)
                    {
                        if (!scores.ContainsKey(userId))
                        {
                            ranking.Add((0, userId));
                        }
                    }

                    var ranks = new List<int> { 1 };
                    for (var i = 1; i < ranking.Count; i++)
                    {
                        // ReSharper disable once CompareOfFloatsByEqualityOperator
                        ranks.Add(ranking[i-1].Item1 == ranking[i].Item1 ? ranks[i-1]: i+1);
                    }

                    var rankingData = new List<object>();
                    for (var i = 0; i < ranking.Count && i < NumRanking; i++)
                    {
                        var (point, userId) = ranking[i];
                        var rank = ranks[i];
                        rankingData.Add(new {point, userId, rank});
                    }

                    var rankingJson = "R" + JsonSerializer.Serialize(new {type = "ranking", ranking = rankingData, taskTotal = sumCounter});
                    var publishTasks = new List<Task<long>>();
                    for (var i = 0; i < ranking.Count; i++)
                    {
                        if (i >= NumRanking)
                        {
                            var (point, userId) = ranking[i];
                            var rank = ranks[i];
                            rankingData[NumRanking - 1] = new {point, userId, rank};
                            rankingJson = "R" + JsonSerializer.Serialize(new {type = "ranking", ranking = rankingData, taskTotal = sumCounter});
                        }

                        publishTasks.Add(db.PublishAsync(ranking[i].Item2, rankingJson));
                    }

                    foreach (var task in publishTasks)
                    {
                        db.Wait(task);
                    }

                    if (now >= lastRankingTime + RankingPeriod)
                    {
                        var task1 = db.SortedSetAddAsync($"ranking_{now}", rankingDataForRedis);
                        var task2 = db.HashSetAsync($"sum_counter_{now}", sumCounter.Select((v, i) => new HashEntry(TaskList[i].Item1, v)).ToArray());
                        var task3 = db.ListRightPushAsync("ranking_times", now);
                        db.Wait(task1);
                        db.Wait(task2);
                        db.Wait(task3);
                        lastRankingTime = now;
                        Console.WriteLine($"Ranking update {now}");
                        if (now > GamePeriod) break;
                    }

                    db.HashSet("sum_counter", sumCounter.Select((v, i) => new HashEntry(i, v)).ToArray());
                }

                var elapsed = GetTime() - GameStartTime - 1000 - now;
                Console.WriteLine($"elapsed {elapsed}");
                if (elapsed < 2000)
                {
                    Thread.Sleep((int)(2000 - elapsed));
                }
            }
        }

        private static void OnRequest(IAsyncResult result)
        {
            if (!listener.IsListening) return;

            var context = listener.EndGetContext(result);
            listener.BeginGetContext(OnRequest, listener);

            new Thread(() => OnRequest(context)).Start();
        }

        private static void OnRequest(HttpListenerContext context)
        {
            var rawUrl = context.Request.RawUrl;
            Debug.Assert(rawUrl != null, nameof(rawUrl) + " != null");
            Console.WriteLine("rawUrl = " + rawUrl);
            context.Response.AppendHeader("Access-Control-Allow-Origin", "*");
            if (rawUrl.StartsWith(EventEndpoint, StringComparison.Ordinal))
            {
                try
                {
                    RunSSE(context.Response, rawUrl);
                }
                catch (HttpListenerException)
                {
                    context.Response.Abort();
                }
                catch (Exception e2)
                {
                    Console.Error.WriteLine(e2);
                    context.Response.Abort();
                }
            }
            else if (rawUrl.StartsWith(GameEndpoint, StringComparison.Ordinal))
            {
                OnRequestMain(context, rawUrl, RunGame);
            }
            else if (rawUrl.StartsWith(MoveEndpoint, StringComparison.Ordinal))
            {
                OnRequestMain(context, rawUrl, RunMove);
            }
            else if (rawUrl.StartsWith(MoveNextEndpoint, StringComparison.Ordinal))
            {
                OnRequestMain(context, rawUrl, RunMoveNext);
            }
            else if (string.Compare(rawUrl, MasterDataEndpoint, StringComparison.Ordinal) == 0)
            {
                OnRequestMain(context, rawUrl, RunMasterData);
            }
            else if (rawUrl.StartsWith(TaskEndpoint, StringComparison.Ordinal))
            {
                OnRequestMain(context, rawUrl, RunTask);
            }
            else if (rawUrl.StartsWith(HealthEndpoint, StringComparison.Ordinal))
            {
                var response = context.Response;
                response.StatusCode = (int)HttpStatusCode.OK;
                response.Close();
            }
            else
            {
                var response = context.Response;
                response.StatusCode = (int)HttpStatusCode.NotFound;
                response.Close();
            }
        }

        private static void OnRequestMain(HttpListenerContext context, string rawUrl, Func<string, byte[]> func)
        {
            byte[] responseString;
            try
            {
                responseString = func(rawUrl);
            }
            catch (Exception e)
            {
                Console.Error.WriteLine(e);
                context.Response.StatusCode = (int)HttpStatusCode.InternalServerError;
                context.Response.ContentType = "text/plain";
                try
                {
                    context.Response.OutputStream.Write(Encoding.UTF8.GetBytes(e.ToString()));
                    context.Response.Close();
                }
                catch (HttpListenerException)
                {
                    context.Response.Abort();
                }
                catch (Exception e2)
                {
                    Console.Error.WriteLine(e2);
                    context.Response.Abort();
                }

                return;
            }

            if (responseString == null)
            {
                var response = context.Response;
                response.StatusCode = (int)HttpStatusCode.NotFound;
                response.Close();
                return;
            }

            try
            {
                var response = context.Response;
                response.StatusCode = (int)HttpStatusCode.OK;
                response.OutputStream.Write(responseString);
                response.Close();
            }
            catch (HttpListenerException)
            {
                context.Response.Abort();
            }
            catch (Exception e2)
            {
                Console.Error.WriteLine(e2);
                context.Response.Abort();
            }
        }

        private static long? WaitUnlock(string type, string userId, int timeLimit)
        {
            var now = GetTime() - GameStartTime;
            if (now < 0) return now;

            var unlockTime = -1;
            for (;;)
            {
                var ut = GetSetTimeLimit(type, userId, now, timeLimit);
                if (ut < 0)
                {
                    break;
                }

                if (unlockTime < 0)
                {
                    unlockTime = ut;
                }
                else if (unlockTime != ut)
                {
                    return null;
                }

                Thread.Sleep((int)Math.Max(1, unlockTime - now));
                now = GetTime() - GameStartTime;
                while (now < unlockTime)
                {
                    Thread.Sleep(1);
                    now = GetTime() - GameStartTime;
                }
            }

            return now;
       }

        private static byte[] RunGame(string rawUrl)
        {
            // /api/game/([0-9a-z]+)
            Debug.Assert(rawUrl.StartsWith(GameEndpoint, StringComparison.Ordinal));

            var token = rawUrl.Substring(GameEndpoint.Length);

            var userId = GetUserId(token);
            if (userId == null) return null;

            var nowNullable = WaitUnlock("game", userId, GameTimeLimit);
            if (!nowNullable.HasValue)
            {
                return JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, object>
                {
                    ["status"] = "error_time_limit",
                });
            }

            var now = nowNullable.Value;
            if (now < 0) return null;
            if (now >= GamePeriod)
            {
                return JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, object>
                {
                    ["status"] = "game_finished",
                });
            }

            var db = redis.GetDatabase();
            var res = (RedisResult[])runGameScript.Evaluate(db, new { userId, now, NumAgent });
            Debug.Assert(res.Length == 4);
            var res0 = (RedisResult[])res[0];
            var res1 = (RedisResult[])res[1];
            var res2 = (RedisResult[])res[2];
            var res3 = (RedisResult[])res[3];
            Debug.Assert(res0.Length == NumAgent);
            Debug.Assert(res1.Length == NumAgent);
            Debug.Assert(res2.Length % 2 == 0);
            Debug.Assert(res3.Length % 2 == 0);

            var taskCnt = new Dictionary<string, int>();
            for (var i = 0; i < res2.Length; i += 2)
            {
                taskCnt.Add((string)res2[i], (int)res2[i+1]);
            }

            var taskCounter = new List<int>();
            foreach (var (s, t, _) in TaskList)
            {
                if (t > now) break;
                taskCounter.Add(taskCnt.ContainsKey(s) ? taskCnt[s] : 0);
            }

            var agentData = new List<object>();
            for (var idx = 0; idx < NumAgent; idx++)
            {
                var a = (RedisResult[])res1[idx];
                Debug.Assert(a.Length % 2 == 0);
                var ss = "";
                var tt = new List<int>();
                for (var i = 0; i < a.Length; i += 2)
                {
                    ss += (string)a[i];
                    tt.Add(int.Parse((string)a[i+1]));
                }

                var b = (RedisResult[])res0[idx];
                Debug.Assert(b.Length % 3 == 0);
                var r = new List<object>();
                for (var i = 0; i < b.Length; i += 3)
                {
                    var x = double.Parse((string) b[i], NumberStyles.Float);
                    var y = double.Parse((string) b[i + 1], NumberStyles.Float);
                    var t = int.Parse((string) b[i + 2]);
                    r.Add(new { x, y, t });
                    if (i > 0 && t <= now && !tt.Contains(t))
                    {
                        var xx = (int)x;
                        var yy = (int)y;
                        if (CheckpointsDict.ContainsKey((xx, yy)))
                        {
                            tt.Add(t);
                            ss += CheckpointsDict[(xx, yy)];
                        }
                    }
                }

                for (var i = 0; i < TaskList.Count; i++)
                {
                    var (s, t, _) = TaskList[i];
                    if (t > now) break;
                    var p = 0;
                    while (p < tt.Count && tt[p] < t) p++;
                    for (;;)
                    {
                        p = ss.IndexOf(s, p, StringComparison.Ordinal);
                        if (p == -1) break;
                        taskCounter[i]++;
                        p++;
                    }
                }

                if (ss.Length > maxLenTask)
                {
                    var offset = ss.Length - maxLenTask;
                    ss = ss.Substring(offset);
                    tt.RemoveRange(0, offset);
                }

                agentData.Add(new
                {
                    move = r,
                    history = ss,
                    history_times = tt,
                });
            }

            var sumCounter = new Dictionary<int, int>();
            for (var i = 0; i < res3.Length; i += 2)
            {
                sumCounter.Add((int) res3[i], (int) res3[i + 1]);
            }

            var taskData = new List<object>();
            var nextTaskTime = -1;
            for (var i = 0; i < TaskList.Count; i++)
            {
                var (s, t, weight) = TaskList[i];
                if (t > now)
                {
                    nextTaskTime = t;
                    break;
                }
                var total = sumCounter.ContainsKey(i) ? sumCounter[i] : 0;
                taskData.Add(new { s, t, weight, count = taskCounter[i], total });
            }

            return JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, object>
            {
                ["status"] = "ok",
                ["now"] = now,
                ["agent"] = agentData,
                ["task"] = taskData,
                ["next_task"] = nextTaskTime,
            });
        }

        private static byte[] RunMove(string rawUrl)
        {
            // /api/move/([0-9a-z]+)/([0-9]+)-([0-9]+)-([0-9]+)
            Debug.Assert(rawUrl.StartsWith(MoveEndpoint, StringComparison.Ordinal));
            return RunMoveMain(rawUrl.Substring(MoveEndpoint.Length), false);
        }

        private static byte[] RunMoveNext(string rawUrl)
        {
            // /api/move_next/([0-9a-z]+)/([0-9]+)-([0-9]+)-([0-9]+)
            Debug.Assert(rawUrl.StartsWith(MoveNextEndpoint, StringComparison.Ordinal));
            return RunMoveMain(rawUrl.Substring(MoveNextEndpoint.Length), true);
        }

        private static byte[] RunMasterData(string rawUrl)
        {
            // /api/master_data
            var now = GetTime() - GameStartTime;
            if (now < 0) return null;

            return MasterDataJson;
        }

        private static byte[] RunTask(string rawUrl)
        {
            // /api/task/([0-9a-z]+)/([0-9]+)
            Debug.Assert(rawUrl.StartsWith(TaskEndpoint, StringComparison.Ordinal));

            var param = rawUrl.Substring(TaskEndpoint.Length).Split('/');
            if (param.Length != 2) return null;
            if (!(reToken.IsMatch(param[0]) && reNum.IsMatch(param[1]))) return null;

            var token = param[0];
            var taskTime = int.Parse(param[1]);
            if (!(0 <= taskTime && taskTime < GamePeriod)) return null;

            var userId = GetUserId(token);
            if (userId == null) return null;

            var nowNullable = WaitUnlock("task", userId, TaskTimeLimit);
            if (!nowNullable.HasValue)
            {
                return JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, object>
                {
                    ["status"] = "error_time_limit",
                });
            }

            var now = nowNullable.Value;
            if (now < 0) return null;
            if (now >= GamePeriod)
            {
                return JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, object>
                {
                    ["status"] = "game_finished",
                });
            }

            if (taskTime > now + TaskWaitTime)
            {
                return JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, object>
                {
                    ["status"] = "error_time_limit",
                });
            }

            var taskData = new List<object>();
            var nextTaskTime = -1;
            foreach (var (s, t, weight) in TaskList)
            {
                if (t > taskTime)
                {
                    nextTaskTime = t;
                    break;
                }

                if (t == taskTime)
                {
                    taskData.Add(new { s, t, weight });
                }
            }

            if (taskData.Count == 0)
            {
                return JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, object>
                {
                    ["status"] = "error_not_found",
                });
            }

            if (taskTime > now)
            {
                Thread.Sleep((int)(taskTime - now));
                now = GetTime() - GameStartTime;
                while (now < taskTime)
                {
                    Thread.Sleep(1);
                    now = GetTime() - GameStartTime;
                }
            }

            return JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, object>
            {
                ["status"] = "ok",
                ["task"] = taskData,
                ["next_task"] = nextTaskTime,
            });
        }

        private static void RunSSE(HttpListenerResponse response, string rawUrl)
        {
            // /event/([0-9a-z]+)
            Debug.Assert(rawUrl.StartsWith(EventEndpoint, StringComparison.Ordinal));
            var token = rawUrl.Substring(EventEndpoint.Length);

            var connectTime = GetTime() - GameStartTime;
            if (connectTime < 0)
            {
                response.StatusCode = (int)HttpStatusCode.Forbidden;
                response.Close();
                return;
            }

            var userId = GetUserId(token);
            if (userId == null)
            {
                response.StatusCode = (int)HttpStatusCode.NotFound;
                response.Close();
                return;
            }

            response.AppendHeader("Content-Type", "text/event-stream");
            response.AppendHeader("X-Accel-Buffering", "no"); // to avoid buffering in nginx 
            if (connectTime >= GamePeriod)
            {
                response.OutputStream.Write(SseHeader);
                response.OutputStream.Write(JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, object>
                {
                    ["type"] = "game_finished",
                }));
                response.OutputStream.Write(SseFooter);
                response.OutputStream.Flush();
                return;
            }

            if (GetSetTimeLimit("SSE", userId, connectTime, SSETimeLimit) >= 0)
            {
                response.OutputStream.Write(SseHeader);
                response.OutputStream.Write(JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, object>
                {
                    ["type"] = "error_time_limit",
                }));
                response.OutputStream.Write(SseFooter);
                response.OutputStream.Flush();
                return;
            }

            var db = redis.GetDatabase();
            db.Publish(userId, $"C{connectTime}");

            Console.WriteLine("start SSE");

            var ch = Channel.CreateUnbounded<byte[]>();
            var sub = redis.GetSubscriber().Subscribe(userId);
            try
            {
                sub.OnMessage(message => ch.Writer.WriteAsync(message.Message).AsTask().Wait());

                Thread.Sleep(500);
                var subStartTime = GetTime() - GameStartTime;
                if (subStartTime >= GamePeriod)
                {
                    response.OutputStream.Write(SseHeader);
                    response.OutputStream.Write(JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, object>
                    {
                        ["type"] = "game_finished",
                    }));
                    response.OutputStream.Write(SseFooter);
                    response.OutputStream.Flush();
                    return;
                }

                response.OutputStream.Write(SseHeader);
                response.OutputStream.Write(GetGameSSE(db, userId, subStartTime));
                response.OutputStream.Write(SseFooter);
                response.OutputStream.Flush();

                var taskIdx = 0;
                while (taskIdx < TaskList.Count && TaskList[taskIdx].Item2 <= subStartTime)
                {
                    taskIdx++;
                }

                for (;;)
                {
                    var cancel = new CancellationTokenSource();
                    cancel.CancelAfter(5000);
                    try
                    {
                        var task = ch.Reader.ReadAsync(cancel.Token).AsTask();
                        task.Wait(cancel.Token);
                        var msg = task.Result;
                        switch (msg[0])
                        {
                            case (byte)'M':
                            {
                                var s = GetMoveSSE(msg, subStartTime);
                                if (s != null)
                                {
                                    response.OutputStream.Write(SseHeader);
                                    response.OutputStream.Write(s);
                                    response.OutputStream.Write(SseFooter);
                                    response.OutputStream.Flush();
                                }
                                break;
                            }
                            case (byte)'R':
                            {
                                response.OutputStream.Write(SseHeader);
                                response.OutputStream.Write(msg.AsSpan()[1..]);
                                response.OutputStream.Write(SseFooter);
                                response.OutputStream.Flush();
                                break;
                            }
                            case (byte)'C':
                            {
                                if (long.Parse(Encoding.UTF8.GetString(msg.AsSpan()[1..])) != connectTime)
                                {
                                    response.OutputStream.Write(SseHeader);
                                    response.OutputStream.Write(JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, object>
                                    {
                                        ["type"] = "disconnected",
                                    }));
                                    response.OutputStream.Write(SseFooter);
                                    response.OutputStream.Flush();
                                    return;
                                }
                                break;
                            }
                            default:
                                throw new Exception(Encoding.UTF8.GetString(msg));
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        response.OutputStream.Write(SsePing);
                        response.OutputStream.Flush();
                    }

                    var now = GetTime() - GameStartTime;
                    if (now >= GamePeriod)
                    {
                        response.OutputStream.Write(SseHeader);
                        response.OutputStream.Write(JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, object>
                        {
                            ["type"] = "game_finished",
                        }));
                        response.OutputStream.Write(SseFooter);
                        response.OutputStream.Flush();
                        return;
                    }

                    var taskIdx0 = taskIdx;
                    while (taskIdx < TaskList.Count && TaskList[taskIdx].Item2 <= now)
                    {
                        taskIdx++;
                    }

                    if (taskIdx > taskIdx0)
                    {
                        response.OutputStream.Write(SseHeader);
                        response.OutputStream.Write(GetTaskSSE(taskIdx0, taskIdx));
                        response.OutputStream.Write(SseFooter);
                        response.OutputStream.Flush();
                    }
                }
            }
            catch (Exception)
            {
                sub.Unsubscribe();
                throw;
            }
        }

        private static int GetSetTimeLimit(string type, string userId, long now, int timeLimit)
        {
            var db = redis.GetDatabase();
            var field = $"{type}_{userId}";
            var r = (string)runTimeLimitScript.Evaluate(db, new { field, now, timeLimit });
            return r == "ok" ? -1 : int.Parse(r);
        }

        private static byte[] GetGameSSE(IDatabase db, string userId, long now)
        {
            var res = (RedisResult[])runGameScript.Evaluate(db, new { userId, now, NumAgent });
            Debug.Assert(res.Length == 4);
            var res0 = (RedisResult[])res[0];
            var res1 = (RedisResult[])res[1];
            var res2 = (RedisResult[])res[2];
            var res3 = (RedisResult[])res[3];
            Debug.Assert(res0.Length == NumAgent);
            Debug.Assert(res1.Length == NumAgent);
            Debug.Assert(res2.Length % 2 == 0);
            Debug.Assert(res3.Length % 2 == 0);

            var agentData = new List<List<object>>();
            foreach (var a in res0)
            {
                var b = (RedisResult[])a;
                Debug.Assert(b.Length % 3 == 0);
                var r = new List<object>();
                for (var i = 0; i < b.Length; i += 3)
                {
                    r.Add(new
                    {
                        x = double.Parse((string)b[i], NumberStyles.Float),
                        y = double.Parse((string)b[i+1], NumberStyles.Float),
                        t = int.Parse((string)b[i+2]),
                    });
                }
                agentData.Add(r);
            }

            var historyData = new List<object>();
            foreach (var a in res1)
            {
                var b = (RedisResult[])a;
                Debug.Assert(b.Length % 2 == 0);
                var ss = "";
                var tt = new List<int>();
                for (var i = 0; i < b.Length; i += 2)
                {
                    ss += (string)b[i];
                    tt.Add(int.Parse((string)b[i+1]));
                }
                historyData.Add(new {s = ss, t = tt});
            }

            var taskCnt = new Dictionary<string, int>();
            for (var i = 0; i < res2.Length; i += 2)
            {
                taskCnt.Add((string)res2[i], (int)res2[i+1]);
            }

            var sumCounter = new Dictionary<int, int>();
            for (var i = 0; i < res3.Length; i += 2)
            {
                sumCounter.Add((int) res3[i], (int) res3[i + 1]);
            }

            var taskData = new List<object>();
            for (var i = 0; i < TaskList.Count; i++)
            {
                var (s, t, w) = TaskList[i];
                if (t > now) break;
                var c = 0;
                if (taskCnt.TryGetValue(s, out var x))
                {
                    c = x;
                }

                var total = sumCounter.ContainsKey(i) ? sumCounter[i] : 0;
                taskData.Add(new {s, t, w, c, total});
            }

            return JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, object>
            {
                ["type"] = "game",
                ["now"] = now,
                ["game_period"] = GamePeriod,
                ["max_len_task"] = maxLenTask,
                ["agent"] = agentData,
                ["checkpoints"] = CheckpointsList,
                ["tasks"] = taskData,
                ["history"] = historyData,
                ["userId"] = userId,
            });
        }

        private static long ReadLongFromSpan(Span<byte> msg, ref int p)
        {
            long r = 0;
            while (p < msg.Length)
            {
                var c = msg[p];
                if (c == (byte) ' ')
                {
                    ++p;
                    break;
                }

                if (!(0x30 <= c && c <= 0x39)) throw new Exception("ReadLongFromSpan error");

                r = 10 * r + (c - 0x30);
                ++p;
            }

            return r;
        }

        private static double ReadDoubleFromSpan(Span<byte> msg, ref int p)
        {
            var p0 = p;
            while (p < msg.Length)
            {
                if (msg[p] == (byte) ' ')
                {
                    var r = double.Parse(Encoding.UTF8.GetString(msg[p0..p]), NumberStyles.Float);
                    ++p;
                    return r;
                }

                ++p;
            }

            return double.Parse(Encoding.UTF8.GetString(msg[p0..]), NumberStyles.Float);
        }

        private static byte[] GetMoveSSE(byte[] msg, long subStartTime)
        {
            var s = msg.AsSpan();
            var p = 1;
            var idx = ReadLongFromSpan(s, ref p);
            var moveTime = ReadLongFromSpan(s, ref p);
            if (moveTime < subStartTime) return null;

            var r = new List<object>();
            while (p < s.Length)
            {
                var x = ReadDoubleFromSpan(s, ref p);
                var y = ReadDoubleFromSpan(s, ref p);
                var t = ReadLongFromSpan(s, ref p);
                r.Add(new { x, y, t });
            }

            return JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, object>
            {
                ["type"] = "move",
                ["idx"] = idx,
                ["now"] = moveTime,
                ["move"] = r,
            });
        }

        private static byte[] GetTaskSSE(int taskIdx0, int taskIdx)
        {
            var taskData = new List<object>();
            for (var i = taskIdx0; i < taskIdx; i++)
            {
                var (s, t, w) = TaskList[i];
                taskData.Add(new {s, t, w});
            }

            return JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, object>
            {
                ["type"] = "task",
                ["tasks"] = taskData,
            });
        }

        private static byte[] RunMoveMain(string paramStr, bool next)
        {
            var param = paramStr.Split('/');
            if (param.Length != 2) return null;
            var param1 = param[1].Split("-");
            if (param1.Length != 3) return null;
            if (!(reToken.IsMatch(param[0]) && reNum.IsMatch(param1[0]) && reNum.IsMatch(param1[1]) && reNum.IsMatch(param1[2]))) return null;

            var token = param[0];
            var idx = int.Parse(param1[0]);
            var x = int.Parse(param1[1]);
            var y = int.Parse(param1[2]);
            if (!(1 <= idx && idx <= NumAgent && 0 <= x && x <= AreaSize && 0 <= y && y <= AreaSize)) return null;

            var userId = GetUserId(token);
            if (userId == null) return null;

            var nowNullable = WaitUnlock($"move_{idx}", userId, MoveTimeLimit);
            if (!nowNullable.HasValue)
            {
                return JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, object>
                {
                    ["status"] = "error_time_limit",
                });
            }

            var now = nowNullable.Value;
            if (now < 0) return null;
            if (now >= GamePeriod)
            {
                return JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, object>
                {
                    ["status"] = "game_finished",
                });
            }

            var db = redis.GetDatabase();
            var res = (RedisResult[])runMoveScript.Evaluate(db, new { userId, idx, x, y, now, next, maxLenTask });
            Debug.Assert(res.Length % 3 == 0);
            var r = new List<object>();
            for (var i = 0; i < res.Length; i += 3)
            {
                r.Add(new
                {
                    x = double.Parse((string)res[i], NumberStyles.Float),
                    y = double.Parse((string)res[i+1], NumberStyles.Float),
                    t = int.Parse((string)res[i+2]),
                });
            }
            return JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, object>
            {
                ["status"] = "ok",
                ["now"] = now,
                ["move"] = r,
            });
        }

        private static string GetUserId(string token)
        {
            var db = redis.GetDatabase();
            var res = db.HashGet("user_token", token);
            return res.IsNull ? null : res.ToString();
        }

        private static long GetTime()
        {
            return (DateTime.UtcNow.Ticks - DateTime.UnixEpoch.Ticks) / 10000;
        }
    }
}
