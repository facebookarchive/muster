package muster_test

import (
	"fmt"
	"os"
	"time"

	"github.com/daaku/go.muster"
)

// The ShoppingManager manages the shopping list and dispatches shoppers.
type ShoppingManager struct {
	ShopperCapacity int           // How much a shopper can carry at a time.
	TripTimeout     time.Duration // How long we wait once we need to get something.
	PendingCapacity int           // How long our shopping list can be.
	Delivery        chan []string // Finished batches will be delivered here.
	muster          muster.Client
}

func (s *ShoppingManager) Start() error {
	s.muster.MaxBatchSize = s.ShopperCapacity
	s.muster.BatchTimeout = s.TripTimeout
	s.muster.PendingCapacity = s.PendingCapacity
	s.muster.BatchMaker = &batchMaker{ShoppingManager: s}
	return s.muster.Start()
}

func (s *ShoppingManager) Stop() error {
	return s.muster.Stop()
}

func (s *ShoppingManager) Add(item string) {
	s.muster.Work <- item
}

type batch struct {
	ShoppingManager *ShoppingManager
	Items           []string
}

func (b *batch) Add(item interface{}) {
	b.Items = append(b.Items, item.(string))
}

func (b *batch) Fire(notifier muster.Notifier) {
	defer notifier.Done()
	b.ShoppingManager.Delivery <- b.Items
}

type batchMaker struct {
	ShoppingManager *ShoppingManager
}

func (b *batchMaker) MakeBatch() muster.Batch {
	return &batch{ShoppingManager: b.ShoppingManager}
}

func Example() {
	delivery := make(chan []string)
	go func() {
		for batch := range delivery {
			fmt.Println("Delivery", batch)
		}
	}()

	sm := &ShoppingManager{
		ShopperCapacity: 3,
		TripTimeout:     20 * time.Millisecond,
		PendingCapacity: 100,
		Delivery:        delivery,
	}

	// We need to start the muster.
	if err := sm.Start(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// Since our capacity is 3, these 3 will end up in a batch.
	sm.Add("milk")
	sm.Add("yogurt")
	sm.Add("butter")

	// Since our timeout is 20ms, these 2 will end up in a batch once we Sleep.
	sm.Add("bread")
	sm.Add("bagels")
	time.Sleep(30 * time.Millisecond)

	// Finally this 1 will also get batched as soon as we Stop.
	sm.Add("cheese")

	// Stopping the muster ensures we wait for all batches to finish.
	if err := sm.Stop(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// Output:
	// Delivery [milk yogurt butter]
	// Delivery [bread bagels]
	// Delivery [cheese]
}
