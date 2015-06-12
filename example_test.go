package muster_test

import (
	"fmt"
	"os"
	"time"

	"github.com/facebookgo/muster"
)

// The ShoppingClient manages the shopping list and dispatches shoppers.
type ShoppingClient struct {
	MaxBatchSize        uint          // How much a shopper can carry at a time.
	BatchTimeout        time.Duration // How long we wait once we need to get something.
	PendingWorkCapacity uint          // How long our shopping list can be.
	muster              muster.Client
}

// The ShoppingClient has to be started in order to initialize the underlying
// work channel as well as the background goroutine that handles the work.
func (s *ShoppingClient) Start() error {
	s.muster.MaxBatchSize = s.MaxBatchSize
	s.muster.BatchTimeout = s.BatchTimeout
	s.muster.PendingWorkCapacity = s.PendingWorkCapacity
	s.muster.BatchMaker = func() muster.Batch { return &batch{Client: s} }
	return s.muster.Start()
}

// Similarly the ShoppingClient has to be stopped in order to ensure we flush
// pending items and wait for in progress batches.
func (s *ShoppingClient) Stop() error {
	return s.muster.Stop()
}

// The ShoppingClient provides a typed Add method which enqueues the work.
func (s *ShoppingClient) Add(item string) {
	s.muster.Work <- item
}

// The batch is the collection of items that will be dispatched together.
type batch struct {
	Client *ShoppingClient
	Items  []string
}

// The batch provides an untyped Add to satisfy the muster.Batch interface. As
// is the case here, the Batch implementation is internal to the user of muster
// and not exposed to the users of ShoppingClient.
func (b *batch) Add(item interface{}) {
	b.Items = append(b.Items, item.(string))
}

// Once a Batch is ready, it will be Fired. It must call notifier.Done once the
// batch has been processed.
func (b *batch) Fire(notifier muster.Notifier) {
	defer notifier.Done()
	fmt.Println("Delivery", b.Items)
	os.Stdout.Sync()
}

func Example() {
	sm := &ShoppingClient{
		MaxBatchSize:        3,
		BatchTimeout:        20 * time.Millisecond,
		PendingWorkCapacity: 100,
	}

	// We need to start the muster.
	if err := sm.Start(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// Since our capacity is 3, these 3 will end up in a batch as soon as the
	// third item has been added.
	sm.Add("milk")
	sm.Add("yogurt")
	sm.Add("butter")

	// Since our timeout is 20ms, these 2 will end up in a batch once we Sleep.
	sm.Add("bread")
	sm.Add("bagels")
	time.Sleep(500 * time.Millisecond)

	// Finally this 1 will also get batched as soon as we Stop which flushes.
	sm.Add("cheese")

	// Stopping the muster ensures we wait for all batches to finish.
	if err := sm.Stop(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
