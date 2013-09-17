package muster_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/daaku/go.muster"
)

type testClient struct {
	MaxBatchSize    int
	BatchTimeout    time.Duration
	PendingCapacity int
	Fire            func(items []string, notifier muster.Notifier)
	muster          muster.Client
}

func (c *testClient) Start() error {
	c.muster.MaxBatchSize = c.MaxBatchSize
	c.muster.BatchTimeout = c.BatchTimeout
	c.muster.PendingCapacity = c.PendingCapacity
	c.muster.BatchMaker = muster.BatchMakerFunc(
		func() muster.Batch { return &testBatch{testClient: c} })
	return c.muster.Start()
}

func (c *testClient) Stop() error {
	return c.muster.Stop()
}

func (c *testClient) Add(item string) {
	c.muster.Work <- item
}

type testBatch struct {
	testClient *testClient
	Items      []string
}

func (b *testBatch) Add(item interface{}) {
	b.Items = append(b.Items, item.(string))
}

func (b *testBatch) Fire(notifier muster.Notifier) {
	b.testClient.Fire(b.Items, notifier)
}

func TestMaxBatch(t *testing.T) {
	t.Parallel()
	expected := []string{"milk", "yogurt", "butter"}
	finished := make(chan struct{})
	sm := &testClient{
		MaxBatchSize:    3,
		BatchTimeout:    20 * time.Millisecond,
		PendingCapacity: 100,
		Fire: func(actual []string, notifier muster.Notifier) {
			defer close(finished)
			if !reflect.DeepEqual(actual, expected) {
				t.Fatal("did not find expected batch")
			}
		},
	}

	if err := sm.Start(); err != nil {
		t.Fatal(err)
	}

	for _, v := range expected {
		sm.Add(v)
	}
	<-finished
}

func TestBatchTimeout(t *testing.T) {
	t.Parallel()
	expected := []string{"milk", "yogurt"}
	finished := make(chan struct{})
	sm := &testClient{
		MaxBatchSize:    3,
		BatchTimeout:    20 * time.Millisecond,
		PendingCapacity: 100,
		Fire: func(actual []string, notifier muster.Notifier) {
			defer close(finished)
			if !reflect.DeepEqual(actual, expected) {
				t.Fatal("did not find expected batch")
			}
		},
	}

	if err := sm.Start(); err != nil {
		t.Fatal(err)
	}

	for _, v := range expected {
		sm.Add(v)
	}
	time.Sleep(30 * time.Millisecond)
	<-finished
}
