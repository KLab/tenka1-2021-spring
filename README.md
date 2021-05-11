# 天下一 Game Battle Contest 2021 Spring

- [公式サイト](https://tenka1.klab.jp/2021-spring/)
- [問題概要](PROBLEM.md)
- [ポータルサイトの使い方](portal.md)
- [ビジュアライザの使い方](visualizer.md)
- [API仕様](apispec.md)

## サンプルコード

- [C++(通信はPython)](cpp_and_python)
  - GCC 9.3.0 Python 3.8.5 で動作確認
- [Go](go)
  - Go 1.13.8 で動作確認
- [Python](py)
  - Python 3.8.5 で動作確認
- [C#](cs)
  - .NET 5.0.101 で動作確認

動作確認環境はいずれも Ubuntu 20.04 LTS

## ゲームサーバのプログラム

- [APIサーバと点数計算バッチ](api)
- [バッチ](batch)
- [使用したマップ](batch/maps/2021-contest)

## 結果

- [最終結果](ranking.tsv)
- [タスク達成回数](data.tsv)

## ローカル実行

ゲームサーバーを手元で動かせる環境を用意しました。

docker, docker-compose をインストールした環境で、以下のコマンドを実行してください。

起動
```
$ docker-compose up
```

ユーザー登録
```
# ユーザID: user0001 トークン: token0001 のユーザーを作成
$ docker-compose exec gamedb redis-cli HSET user_token token0001 user0001
```

以下のURLでAPIとビジュアライザにアクセス可能です。
- http://localhost:8080/api/move/token0001/1-1-1
- http://localhost:8080/visualizer/index.html?user_id=user0001&token=token0001

## ビジュアライザで使用したライブラリ等

- [Json.NET (MIT) © James Newton-King](https://github.com/JamesNK/Newtonsoft.Json/blob/master/LICENSE.md)
- [TextShader (MIT) © gam0022](https://qiita.com/gam0022/items/f3b7a3e9821a67a5b0f3)
- [Rajdhani (OFL) © Indian Type Foundry](https://fonts.google.com/specimen/Rajdhani)
- [Share Tech Mono (OFL) © Carrois Apostrophe](https://fonts.google.com/specimen/Share+Tech+Mono)

## ルール

- コンテスト期間
  - 2021年4月24日(土) 14:00～18:00 (日本時間)
- 参加資格
  - 学生、社会人問わず、どなたでも参加可能です。
他人と協力せず、個人で取り組んでください。
- ランキング
  - 制限時間（4時間）内で獲得されたスコアを競います。
スコアが同じ場合は、同率順位とします。
- 使用可能言語
  - 言語の制限はありません。ただしHTTPSによる通信ができる必要があります。
- SNS等の利用について
  - 本コンテスト開催中にSNS等にコンテスト問題について言及して頂いて構いませんが、ソースコードを公開するなどの直接的なネタバレ行為はお控えください。
ハッシュタグ: #klabtenka1
