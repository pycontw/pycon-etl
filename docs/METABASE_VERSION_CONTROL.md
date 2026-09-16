# Metabase 版控討論（草案，尚未定案）

> 狀態：討論中，尚未決議。2026-07-28 記錄。
> 起點問題：Airflow 的內容都在 git 裡，Metabase 沒有——我們該幫 Metabase 做版控嗎？

## 結論

**要，但只做單向 snapshot；而且在那之前有兩件更重要的事。**

1. **Metabase application DB 定期備份到 GCS** —
   真正的痛點是 disk 掛掉 dashboard 全沒，而這是備份問題不是版控問題。優先度最高。
2. **把重 SQL 邏輯下推到 `dwd.*`** —
   邏輯長在 git 外面才是根本問題。Metabase question 退化成薄查詢後，版控與否就不痛了。
3. **API dump → JSON commit，單向、每日一次** —
   給 `git log` 可追、可 grep「哪些 question 還在打 `ods.*`」。
   放 `pycontw-infra-scripts` 比放 pycon-etl 合理（Metabase 部署本來就在那）。
4. **不做 git-driven deployment**（git → Metabase）—
   志工在 UI 上改，round-trip 脆弱，只會得到假保險。

## 理由

### 1. 災難復原：這不是版控問題

Metabase 的 questions / dashboards / collections 全存在它自己的 application DB，跑在
`/mnt/disks/data-team-additional-disk/pycontw-infra-scripts/data_team/metabase_server`
（見 [DEPLOYMENT.md](DEPLOYMENT.md)）。那顆 disk 掛掉，所有 dashboard 直接消失，
而 `dags/app/team_registration_bot/udf.py:74` 還硬編碼了 `question/142`，
重建時連「142 原本是什麼」都查不到。

解法是定期 dump application DB 到 GCS（配 lifecycle rule）。
DB dump 進 git 只會得到一堆無法 diff 的 binary blob，沒有意義。

### 2. SQL 邏輯下推：最大槓桿

如果 question 裡塞著複雜 SQL，那就是商業邏輯長在 git 外面。
與其把 Metabase 納管，不如把邏輯推到 ETL：在本 repo 定義 `dwd.*` 的 view / table，
讓 question 退化成 `SELECT * FROM dwd.xxx WHERE year = {{year}}`。

這正是 [contrib/README.md](../contrib/README.md) 那個 KKTIX transform 已經在走的方向——
把 `json_extract` 的痛推回後端，Metabase 端維持原本使用體驗。
這條路走完，Metabase 裡剩下的東西就淺到「沒版控也不太痛」，第 3 點就從必需變成加分。

### 3. 單向 snapshot：值得做，但別做雙向

用 Metabase API（`/api/card`、`/api/dashboard`、`/api/collection`）定期把定義 dump 成 JSON commit。
好處很實際：

- `git log` 看得出誰在什麼時候改了哪張圖的 SQL
- 可以 grep「哪些 question 還在打 `ods_kktix_attendeeId_datetime`」——做 schema migration 時會需要
- review 時有東西可對照

**git 是紀錄，Metabase 是 source of truth。**

### 4. 為什麼不做雙向

編輯者是志工，在 UI 上點一點就改了，不會為了改個 filter 開 PR；
且 import 的 round-trip 一向脆弱（entity ID 對應、collection 階層、DB connection ID）。
做成雙向只會變成「git 跟現況永遠不一致」的假保險。

## 待查證

- 官方 serialization（`export` / `import` 產 YAML）**應該**是 Enterprise / Pro 限定，
  OSS build 沒有那組指令——動工前對著實際跑的版本確認一次。
  若確認沒有，就走 API + 一支 dump script，OSS 一定拿得到。

## 如果要動工，下一步

- 先用 API 打一次 PoC，確認拿得到哪些欄位，再決定 snapshot 格式
- 或先盤點目前 Metabase 上有哪些 question 直接打 `ods.*`——這份清單對「邏輯下推」的排序最有用
