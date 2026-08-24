// Command eventbus runs a deterministic demo of the partitioned event bus:
// it creates a topic, appends messages, consumes with a group, rebalances,
// compacts and prints a report.
package main

import (
	"fmt"
	"time"

	"eventbus/internal/ack"
	"eventbus/internal/broker"
	"eventbus/internal/compaction"
	"eventbus/internal/consumer"
	"eventbus/internal/group"
	"eventbus/internal/metric"
	"eventbus/internal/model"
	"eventbus/internal/offset"
	"eventbus/internal/partition"
	"eventbus/internal/quota"
	"eventbus/internal/redelivery"
	"eventbus/internal/retention"
	"eventbus/internal/worker"
)

func main() {
	ps := partition.New()
	br := broker.New(ps)
	om := offset.New()
	ol := offset.NewLog()
	gc := group.New()
	metrics := metric.New()
	ackTr := ack.New()
	redel := redelivery.New(ps, 2*time.Second)
	vis := partition.NewVisibility()
	cl := retention.New(ps, om)
	comp := compaction.New(ps)
	run := worker.New(br, cl, comp, gc)

	t, err := br.CreateTopic("orders", 3, true)
	if err != nil {
		fmt.Println("create:", err)
		return
	}
	_ = t
	_ = br.ConfigureTopic("orders", 3600000, 1024*1024)

	// append messages to partition 1
	p1 := ps.Get(1)
	msg1 := model.NewMessage("m1", "user-a", "create", 1)
	off1 := ps.Append(p1, "seg-1", msg1)
	msg2 := model.NewMessage("m2", "user-b", "pay", 1)
	ps.Append(p1, "seg-1", msg2)
	metrics.RecordAppend()
	metrics.RecordAppend()
	_, _ = br.Append("orders", 1, model.NewMessage("m3", "user-c", "ship", 1))
	metrics.RecordAppend()
	if _, err := br.Append("orders", 99, model.NewMessage("mX", "k", "v", 1)); err != nil {
		metrics.RecordError()
	}
	vis.Publish(1, "seg-1")
	_ = vis.Visible(1, "seg-1")
	_ = vis.Visible(1, "seg-ghost")

	// consumer group
	gc.Join("g1", "c1")
	gc.Join("g1", "c2")
	gc.Heartbeat("g1", "c1")
	assign := gc.Rebalance("g1", t.Partitions)
	metrics.RecordRebalance()
	_ = assign

	cons := consumer.New(gc, om, ps, "g1", "c1")
	cons.AcquireLease(1, 10*time.Second)
	_ = om.Recover()
	_ = ackTr.Acked(1, "m1")
	_ = redel.Due(1, time.Now().Add(-time.Hour))
	sess := consumer.NewSession("c1")
	sess.Touch()
	_ = sess.Idle()
	got := cons.Pull(1, "seg-1")
	bounded := cons.PullBounded(1, "seg-1", 1)
	_ = bounded
	metrics.RecordRead()
	cons.Commit(1, off1+2)
	om.Durable(1)
	for _, m := range got {
		ackTr.Ack(1, m.ID)
		redel.Track(1, m)
	}
	processed := cons.Run(1, "seg-1", func(msg *model.Message) error { return nil }, 2)
	_ = processed

	// compaction view
	views := comp.Compact(1, "seg-1")
	_ = views

	// retention
	removed := cl.Clean(p1, "seg-1", time.Now().Add(-time.Hour))
	_ = removed

	// quota + metrics
	quotaMgr := quota.NewManager(5, 5)
	quotaMgr.Allow("tenant-a")
	quotaMgr.Allow("tenant-a")
	quotaMgr.Reset("tenant-a")
	hist := metric.NewHistogram()
	hist.Record(14)
	hist.Record(9)
	fmt.Println(hist.Average())

	// heartbeat timeout + schedule + redelivery policy
	_ = gc.HeartbeatTimeout("g1", 30*time.Second)
	_ = worker.DefaultSchedule()
	_ = redelivery.DefaultPolicy()
	_ = om.Checkpoints()
	_ = retention.DefaultPolicy().AgeExpired(7200000)
	_ = gc.Snapshot("g1")
	_ = consumer.NewSession("c1").Idle()
	_ = consumer.DefaultLimits().Bounded()
	_ = model.NewEventRecord("append", "orders", 1, 1, "user-a")
	_ = broker.DefaultAckPolicy()
	rep := broker.NewReplicator()
	rep.Ack(1, broker.DefaultAckPolicy())
	_ = rep.Ready(1)
	_ = gc.CurrentAssignment("g1")
	_ = om.RecoverFromLog(ol.Rows())
	_ = offset.DefaultPolicy().WaitDurable
	_ = model.DefaultConfig().Compaction
	_ = ackTr.Pending(1, got)
	_ = ackTr.Dead(1)
	reg := quota.NewRegistry()
	reg.Register(quota.Tenant{ID: "tenant-a", Rate: 5, Burst: 5})
	_, _ = reg.Get("tenant-a")
	_ = model.Stats{Partition: 1, State: p1.State().String(), NextOffset: p1.NextOffset, Segments: len(ps.Segments(1))}
	_ = partition.Capture(p1, len(ps.Segments(1)))
	_ = gc.Elect("g1", 1, 1)
	_ = consumer.NewSession("c1")
	_ = worker.DefaultMaintenancePolicy()
	syncH := offset.NewSync()
	syncH.MarkPending(1, om.Committed(1)+1)
	syncH.ConfirmVisible(1, om.Committed(1))
	_ = syncH.PendingOffset(1)
	_ = syncH.VisibleOffset(1)
	lc := partition.NewLifecycle()
	_ = lc.Writable(p1)
	_ = lc.Seal(p1)
	_ = lc.Retire(p1)
	pset := broker.NewPartitionSet("orders")
	pset.Add(1)
	pset.Add(2)
	_ = br.DeleteTopic("temp-topic")
	_ = pset.Contains(1)
	_ = pset.List()
	_ = pset.Snapshot(ps.Get)
	router := broker.NewRouter(pset)
	pid, ok := router.Route("user-a")
	_ = pid
	_ = ok
	bm := broker.NewMetrics()
	bm.IncAppend()
	bm.IncDelete()
	bm.IncError()
	_ = bm.Appends()
	batch := partition.NewBatch()
	batch.Add(model.NewMessage("b1", "k", "v", 1))
	batch.Close()
	_ = batch.Size()
	_ = batch.Offsets()
	idx := partition.NewIndex()
	idx.Add("user-a", 1)
	idx.Add("user-b", 2)
	_ = idx.Lookup("user-a")
	_ = idx.Terms()
	cp := cons.Snapshot(1)
	cons.Restore(cp)
	_ = cons.Progress(1)
	_ = gc.State("g1")
	gc.Reset("g1")
	run.RunOnce(time.Now().Add(-time.Hour))
	leaseStop := make(chan struct{})
	go func() { time.Sleep(30 * time.Millisecond); close(leaseStop) }()
	go cons.LeaseLoop(1, 50*time.Millisecond, leaseStop)
	time.Sleep(40 * time.Millisecond)

	// segment writer/reader
	wr := partition.NewWriter(ps, "seg-w")
	wr.Write(p1, model.NewMessage("w1", "k", "v", 1))
	flushed := wr.Flush(p1)
	_ = flushed
	rd := partition.NewReader(ps, "seg-w", 0)
	_, _ = rd.Next(1)
	_ = rd.Cursor()

	ol.Append(model.Checkpoint{Partition: 1, Offset: om.Committed(1)})
	_ = ol.Rows()

	// compaction rewrite + metric report
	_ = comp.Rewrite(1, "seg-1")
	fmt.Println(metrics.Report())

	// worker loop (short-lived)
	stop := make(chan struct{})
	go func() {
		time.Sleep(60 * time.Millisecond)
		close(stop)
	}()
	go run.Loop(worker.Schedule{Interval: 20 * time.Millisecond, Retention: true, Compaction: false}, stop)
	time.Sleep(80 * time.Millisecond)

	// worker
	run.RunOnce(time.Now().Add(-time.Hour))

	fmt.Printf("offset=%d messages=%d views=%d avg_ms=%.0f metrics=%v\n",
		om.Committed(1), len(got), len(views), hist.Average(), metrics.Snapshot())
}
