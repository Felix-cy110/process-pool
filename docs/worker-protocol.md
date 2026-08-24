# Worker NDJSON 协议

进程池与 worker 通过 stdin/stdout 交换 UTF-8 NDJSON。每个 JSON 对象占一行，行尾为 `\n`。同一个 worker 上的请求严格串行，因此响应顺序与请求顺序一致；`id` 仍必须原样返回，用于发现实现错误。

## 请求

```json
{"id":1,"payload":{"op":"sum","values":[7,11,24]}}
```

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `id` | 无符号 64 位整数 | 池生成的任务 ID |
| `payload` | 任意 JSON | 调用方提交的业务参数 |

## 成功响应

```json
{"id":1,"ok":true,"result":{"sum":42}}
```

`result` 可以是任意 JSON。省略或传 `null` 都会被视为 JSON `null`。

## 业务失败响应

```json
{
  "id":1,
  "ok":false,
  "error":{
    "code":"INVALID_INPUT",
    "message":"values must be a number array",
    "details":{"field":"values"}
  }
}
```

业务失败不会销毁 worker。`code` 和 `message` 必填，`details` 可选。

下面这些情况属于协议或传输失败，进程池会销毁 worker：

- 返回的 `id` 与请求不一致；
- JSON 无法解析或包含协议未定义字段；
- `ok: true` 同时带有 `error`；
- `ok: false` 没有 `error` 或同时带有 `result`；
- worker 退出、关闭 stdout 或超过调用超时。

## Python worker 最小示例

```python
import json
import os
import sys

for line in sys.stdin:
    request = json.loads(line)
    response = {
        "id": request["id"],
        "ok": True,
        "result": {
            "pid": os.getpid(),
            "echo": request["payload"],
        },
    }
    print(json.dumps(response, separators=(",", ":")), flush=True)
```

日志必须写入 stderr：

```python
print("worker initialized", file=sys.stderr, flush=True)
```
