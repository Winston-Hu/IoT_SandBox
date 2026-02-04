monitor.py
    This monitor is watching for:  
    1. slave DO become zero, topic: "/SKE_SOLAR/SLAVE_BECOME_0"  
    2. slave ping 10.10.9.2(pi) false, topic: "/SKE_SOLAR/M300_ping"  
    3. slave device restart  
    4. long time no heartbeat  

---

## monitor.py（Notes）

### 总览

`monitor.py` 是一个基于 MQTT 的监控守护程序，用来订阅多个 Topic，解析收到的消息（优先按 JSON 解析），并把关键事件写入日志文件 `monitor_log`（带滚动）以及控制台输出。

当前代码的告警动作：

* **(1) / (2)**：只写 `logger.warning`（邮件发送已注释掉）
* **(3)**：仅在 **Device Restart Confirmed** 时发送邮件（其余情况只写日志）
* **(4)**：**SERIOUS TIMEOUT** 时：写日志 + **publish** 到 `SERIOUS_TIMEOUT_PUB_TOPIC`（邮件发送已注释掉）

---

## monitor.py 监控的内容

该 monitor 主要在看以下事件：

1. **slave DO 变成 0**

   * Topic：`/SKE_SOLAR/SLAVE_BECOME_0`
   * 含义：slave 上报 DO 从 1→0 的状态变化
   * 逻辑：

     * 从 payload 取：`seq / mac / DO / masterState / isNetworkStable / pingQueue / ts`
     * 把 payload 里的 `ts`（假定为 UTC+8，无 tz 信息）转换为 Sydney 本地时间显示（UTC+11 或以系统本地时区为准）
     * 根据 `isNetworkStable` 和 `pingQueue` 判断 Problem：

       * `Network Unstable`：`isNetworkStable == False` 且 `pingQueue` 里包含 `0`
       * `Master Was 0`：`isNetworkStable == True` 且 `pingQueue` 没有 `0` 且 `masterState == 0`
       * 其它：`Unknown Reason`
   * 动作：记录 warning 日志（目前邮件已注释）

2. **slave ping 10.10.9.2(pi) 失败**

   * Topic：`/SKE_SOLAR/M300_ping`
   * 含义：M300 上报对内网主机 `10.10.9.2` 的 ping 不通或恢复
   * 逻辑：

     * 从 payload 取：`status / mac / first_offline_ts / recovered_ts`
     * 把两个时间从 UTC+8 转成本地显示
   * 动作：记录 warning 日志（目前邮件已注释）

3. **slave 设备重启检测（基于 heartbeat 间隔）**

   * Topic：`/SKE_SOLAR/slave_1hour/Morriset/`
   * payload 示例：`{"MAC":"D4:AD:20:B7:EA:E2"}`
   * 状态管理（按 MAC 分开，互不混淆）：

     * `heartbeat_last_seen[mac] = now_ts`：每个 mac 最近一次 heartbeat 的接收时间（epoch 秒）
     * `heartbeat_history[mac]`：每个 mac 保存最近最多 3 个 heartbeat 时间点 `[t1, t2, t3]`
   * 判定逻辑：

     * 当 `hist` 有 3 个点时：

       * `t1, t2, t3 = hist`
       * `d1 = t2 - t1`，`d2 = t3 - t2`
       * **d1/d2 的单位是秒**（因为 t1/t2/t3 是 `time.time()` 的 epoch 秒）
     * classify 规则：

       * `< HEARTBEAT_INTERVAL - HEARTBEAT_TOLERANCE` → `RESTART_CONFIRMED`
       * `> HEARTBEAT_INTERVAL + HEARTBEAT_TOLERANCE` → `RESTART_OR_PACKET_LOSS`
       * 否则 → `OK`
     * “重启确认”条件（你当前写法）：

       * (c1==RESTART_CONFIRMED 且 c2==RESTART_CONFIRMED) 或
       * (c1==OK 且 c2==RESTART_CONFIRMED) 或
       * (c1==RESTART_CONFIRMED 且 c2==OK)
   * 动作：

     * Restart Confirmed：写 warning + **发邮件**，然后 `heartbeat_history[mac] = [t3]`（保留最后一个点）
     * Restart OR Packet Loss：写 warning（邮件已注释），然后 `heartbeat_history[mac] = [t3]`
     * OK：写 info
   * hist 不足 3 个点时：

     * 只写收集日志（COLLECT），显示已经收到了 1 或 2 个时间点

4. **长时间没有 heartbeat（SERIOUS TIMEOUT）**

   * 线程函数：`heartbeat_watchdog(logger, mqtt_client)`
   * 每隔 `CHECK_INTERVAL = HEARTBEAT_INTERVAL * 1.4` 秒检查一次
   * 对每个 `heartbeat_last_seen` 里的 mac：

     * `gap = now - last_ts`
     * 若 `gap > HEARTBEAT_INTERVAL * 1.5`：

       * 写 warning 日志
       * publish 一条 JSON 到 `SERIOUS_TIMEOUT_PUB_TOPIC = "/monitor_send_sms/SERIOUS_TIMEOUT"`
       * 检查 publish 返回值 `info.rc`，非成功则写 error
       * （邮件发送部分目前注释）

---

## 关键函数说明（function details）

### `parse_payload(payload_bytes)`

* 将 MQTT payload 解码成字符串
* 如果看起来像 JSON（以 `{` 或 `[` 开头），尝试 `json.loads`
* 返回 `(raw_text, payload_obj_or_text)`

### `utc8_to_local_str(ts_str)`

* 输入类似：`"2026-02-04T07:50:56"`（无时区信息，代码假定它是 UTC+8）
* 强制附加 tzinfo = UTC+8
* 然后 `.astimezone()` 转换为系统本地时区（你的环境是 Sydney，通常是 UTC+11 / 会随 DST 变化）
* 返回格式：`YYYY-MM-DD HH:MM:SS`

### `handle_slave_restart_heartbeat(payload, logger)`

* 按 MAC 维护 heartbeat 轨迹
* 计算 d1/d2（秒）
* 根据阈值判断是否重启、丢包或正常
* 重启确认会发邮件（当前代码）

### `heartbeat_watchdog(logger, mqtt_client)`

* 定期检查每个 mac 的最后一次 heartbeat 时间
* 超时则 publish JSON：

  * `type`: `"SERIOUS_TIMEOUT"`
  * `mac`
  * `gap`（秒）
  * `last_time`（字符串）
  * `now`（字符串）
* publish 失败会记录 rc

---

## 日志样例（log 里可能出现什么）

### 启动 / 连接 / 订阅

```
2026-02-05 09:10:12 | INFO | Connecting to 13.238.189.183:1883 ...
2026-02-05 09:10:12 | INFO | CONNECTED to 13.238.189.183:1883
2026-02-05 09:10:12 | INFO | SUBSCRIBE qos=0 topic=/SKE_SOLAR/SLAVE_BECOME_0
2026-02-05 09:10:12 | INFO | SUBSCRIBE qos=0 topic=/SKE_SOLAR/M300_ping
2026-02-05 09:10:12 | INFO | SUBSCRIBE qos=0 topic=/SKE_SOLAR/slave_1hour/Morriset/
```

### DO 变 0（Topic 1）

```
2026-02-05 09:12:01 | WARNING | [/SKE_SOLAR/SLAVE_BECOME_0] < Problem: Network Unstable >, mac=D4:AD:20:B7:EA:E2, timeHappened=2026-02-05 10:50:56, seq=13, DO=0, masterState=1, isNetworkStable=False, pingQueue=[1,1,1,1,1,1,1,1,1,0]
```

### ping 失败（Topic 2）

```
2026-02-05 09:15:33 | WARNING | [/SKE_SOLAR/M300_ping] < Problem: Slave Ping 10.10.9.2 False >, mac=D4:AD:20:B7:EA:E2, first_offline_ts=2026-02-05 10:36:44, recovered_ts=2026-02-05 10:36:50, pingStatus=host is not reachable
```

### heartbeat 收集中（Topic 3）

```
2026-02-05 09:20:00 | INFO | [SLAVE_HEARTBEAT_COLLECT] mac=D4:AD:20:B7:EA:E2 count=1, history=2026-02-05 09:20:00
2026-02-05 09:30:00 | INFO | [SLAVE_HEARTBEAT_COLLECT] mac=D4:AD:20:B7:EA:E2 count=2, history=2026-02-05 09:20:00, 2026-02-05 09:30:00
```

### 设备重启确认（Topic 3）

```
2026-02-05 09:40:00 | WARNING | [/SKE_SOLAR/slave_1hour/Morriset/] < Problem: Device Restart Confirmed > mac=D4:AD:20:B7:EA:E2 d1=120.3s d2=598.9s timestamps=2026-02-05 09:20:00, 2026-02-05 09:22:00, 2026-02-05 09:32:59
```

### SERIOUS TIMEOUT（Watchdog）

```
2026-02-05 10:55:12 | WARNING | [/SKE_SOLAR/slave_1hour/Morriset/] < Problem: SERIOUS TIMEOUT, Restart OR Packet Loss > mac=D4:AD:20:B7:EA:E2, gap=931.2s timestamps=2026-02-05 10:39:41, 2026-02-05 10:55:12
```

### publish 失败（Watchdog）

```
2026-02-05 10:55:12 | ERROR | [WATCHDOG] publish rc=4
```

---
