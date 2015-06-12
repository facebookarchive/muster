package muster

import (
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/facebookgo/clock"
)

type testClient struct {
	MaxBatchSize         uint
	BatchTimeout         time.Duration
	MaxConcurrentBatches uint
	PendingWorkCapacity  uint
	Fire                 func(items []string, notifier Notifier)
	muster               Client
}

func (c *testClient) Start() error {
	c.muster.MaxBatchSize = c.MaxBatchSize
	c.muster.BatchTimeout = c.BatchTimeout
	c.muster.MaxConcurrentBatches = c.MaxConcurrentBatches
	c.muster.PendingWorkCapacity = c.PendingWorkCapacity
	c.muster.BatchMaker = func() Batch { return &testBatch{Client: c} }
	return c.muster.Start()
}

func (c *testClient) Stop() error {
	return c.muster.Stop()
}

func (c *testClient) Add(item string) {
	c.muster.Work <- item
}

type testBatch struct {
	Client *testClient
	Items  []string
}

func (b *testBatch) Add(item interface{}) {
	b.Items = append(b.Items, item.(string))
}

func (b *testBatch) Fire(notifier Notifier) {
	b.Client.Fire(b.Items, notifier)
}

type fatal interface {
	Fatal(args ...interface{})
}

func errCall(t fatal, f func() error) {
	if err := f(); err != nil {
		t.Fatal(err)
	}
}

func expectFire(
	t *testing.T,
	finished chan struct{},
	expected [][]string,
) func(actual []string, notifier Notifier) {

	return func(actual []string, notifier Notifier) {
		defer notifier.Done()
		defer close(finished)
		for _, batch := range expected {
			if !reflect.DeepEqual(actual, batch) {
				t.Fatalf("expected %v\nactual %v", batch, actual)
			}
		}
	}
}

func addExpected(c *testClient, expected [][]string) {
	for _, b := range expected {
		for _, v := range b {
			c.Add(v)
		}
	}
}

func TestMaxBatch(t *testing.T) {
	t.Parallel()
	expected := [][]string{{"milk", "yogurt", "butter"}}
	finished := make(chan struct{})
	c := &testClient{
		MaxBatchSize:        uint(len(expected[0][0])),
		BatchTimeout:        20 * time.Millisecond,
		Fire:                expectFire(t, finished, expected),
		PendingWorkCapacity: 100,
	}
	errCall(t, c.Start)
	addExpected(c, expected)
	<-finished
}

func TestBatchTimeout(t *testing.T) {
	t.Parallel()
	expected := [][]string{{"milk", "yogurt"}}
	finished := make(chan struct{})
	c := &testClient{
		MaxBatchSize:        3,
		BatchTimeout:        20 * time.Millisecond,
		Fire:                expectFire(t, finished, expected),
		PendingWorkCapacity: 100,
	}
	errCall(t, c.Start)
	addExpected(c, expected)
	time.Sleep(30 * time.Millisecond)
	<-finished
}

func TestStop(t *testing.T) {
	t.Parallel()
	expected := [][]string{{"milk", "yogurt"}}
	finished := make(chan struct{})
	c := &testClient{
		MaxBatchSize:        3,
		BatchTimeout:        time.Hour,
		Fire:                expectFire(t, finished, expected),
		PendingWorkCapacity: 100,
	}
	errCall(t, c.Start)
	addExpected(c, expected)
	errCall(t, c.Stop)
	<-finished
}

func TestZeroMaxBatchSize(t *testing.T) {
	t.Parallel()
	expected := [][]string{{"milk", "yogurt"}}
	finished := make(chan struct{})
	c := &testClient{
		BatchTimeout:        20 * time.Millisecond,
		Fire:                expectFire(t, finished, expected),
		PendingWorkCapacity: 100,
	}
	errCall(t, c.Start)
	addExpected(c, expected)
	time.Sleep(30 * time.Millisecond)
	<-finished
}

func TestZeroBatchTimeout(t *testing.T) {
	t.Parallel()
	expected := [][]string{{"milk", "yogurt"}}
	finished := make(chan struct{})
	c := &testClient{
		MaxBatchSize:        3,
		Fire:                expectFire(t, finished, expected),
		PendingWorkCapacity: 100,
	}
	errCall(t, c.Start)
	addExpected(c, expected)
	time.Sleep(30 * time.Millisecond)
	select {
	case <-finished:
		t.Fatal("should not be finished yet")
	default:
	}
	errCall(t, c.Stop)
	<-finished
}

func TestZeroBoth(t *testing.T) {
	t.Parallel()
	c := &testClient{}
	if c.Start() == nil {
		t.Fatal("was expecting error")
	}
}

func TestEmptyStop(t *testing.T) {
	t.Parallel()
	c := &testClient{
		MaxBatchSize: 3,
		Fire: func(actual []string, notifier Notifier) {
			defer notifier.Done()
			t.Fatal("should not get called")
		},
		PendingWorkCapacity: 100,
	}
	errCall(t, c.Start)
	errCall(t, c.Stop)
}

func TestContiniousSendWithTimeoutOnlyBlocking(t *testing.T) {
	t.Parallel()
	var fireTotal, addedTotal uint64
	c := &testClient{
		BatchTimeout: 5 * time.Millisecond,
		Fire: func(actual []string, notifier Notifier) {
			defer notifier.Done()
			atomic.AddUint64(&fireTotal, uint64(len(actual)))
		},
	}
	klock := clock.NewMock()
	c.muster.klock = klock
	errCall(t, c.Start)

	finished := make(chan struct{})
	go func() {
		defer close(finished)
		for {
			select {
			case <-finished:
				return
			case c.muster.Work <- "42":
				atomic.AddUint64(&addedTotal, 1)
			default:
			}
		}
	}()

	for i := 0; i < 3; i++ {
		klock.Add(c.BatchTimeout)
	}
	finished <- struct{}{}
	<-finished

	errCall(t, c.Stop)
	if fireTotal != addedTotal {
		t.Fatalf("fireTotal=%d VS addedTotal=%d", fireTotal, addedTotal)
	}
}

func TestContiniousSendWithTimeoutOnly(t *testing.T) {
	t.Parallel()
	var fireTotal, addedTotal uint64
	c := &testClient{
		BatchTimeout: 5 * time.Millisecond,
		Fire: func(actual []string, notifier Notifier) {
			defer notifier.Done()
			atomic.AddUint64(&fireTotal, uint64(len(actual)))
		},
		PendingWorkCapacity: 100,
	}
	klock := clock.NewMock()
	c.muster.klock = klock
	errCall(t, c.Start)

	finished := make(chan struct{})
	go func() {
		defer close(finished)
		for {
			select {
			case <-finished:
				return
			case c.muster.Work <- "42":
				atomic.AddUint64(&addedTotal, 1)
			default:
			}
		}
	}()

	for i := 0; i < 3; i++ {
		klock.Add(c.BatchTimeout)
	}
	finished <- struct{}{}
	<-finished

	errCall(t, c.Stop)
	if fireTotal != addedTotal {
		t.Fatalf("fireTotal=%d VS addedTotal=%d", fireTotal, addedTotal)
	}
}

func TestMaxConcurrentBatches(t *testing.T) {
	t.Parallel()
	expected := [][]string{{"milk", "yogurt"}}
	gotMilk := make(chan struct{})
	gotYogurt := make(chan struct{})
	c := &testClient{
		MaxBatchSize:         1,
		MaxConcurrentBatches: 1,
		Fire: func(items []string, notifier Notifier) {
			defer notifier.Done()
			if len(items) == 1 && items[0] == "milk" {
				gotMilk <- struct{}{}
				return
			}
			if len(items) == 1 && items[0] == "yogurt" {
				gotYogurt <- struct{}{}
				return
			}
			t.Fatalf("unexpected items: %v", items)
		},
	}
	errCall(t, c.Start)
	addExpected(c, expected)
	select {
	case c.muster.Work <- "never sent":
		t.Fatal("should not get here")
	case <-time.After(1 * time.Millisecond):
	}
	<-gotMilk
	<-gotYogurt
	errCall(t, c.Stop)
}

func BenchmarkFlow(b *testing.B) {
	c := &testClient{
		MaxBatchSize:        3,
		BatchTimeout:        time.Hour,
		PendingWorkCapacity: 100,
		Fire: func(actual []string, notifier Notifier) {
			notifier.Done()
		},
	}
	errCall(b, c.Start)
	for i := 0; i < b.N; i++ {
		c.Add("42")
	}
	errCall(b, c.Stop)
}
