# eventbus

eventbus 是一个分区消息队列/事件总线的控制面：主题与分区、消息追加与分段、
消费组与成员心跳、分区分配与再平衡、偏移提交、保留清理、主题压缩、未确认
重投与死信、租户配额限流。

## 构建

```bash
./build_benzhi_docker.sh eventbus linux/amd64
./build_benzhi_docker.sh eventbus linux/arm64
```

## 运行

```bash
go run ./cmd/eventbus
```

## 容器内验证

```bash
go build ./...
go test ./...
go vet ./...
go run ./cmd/eventbus
```

## 功能

- 主题与分区管理（active/sealed/retired）
- 分区消息追加与分段存储
- 消费组、成员心跳与分区再平衡
- 偏移提交与持久化
- 保留清理（按时间/大小）与主题压缩
- 未确认消息重投与死信
- 租户配额限流与指标
