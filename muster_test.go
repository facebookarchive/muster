package muster_test

import (
	"reflect"
	"testing"
	"time"
)

func TestMaxBatch(t *testing.T) {
	t.Parallel()
	expected := []string{"milk", "yogurt", "butter"}
	delivery := make(chan []string)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		actual := <-delivery
		if !reflect.DeepEqual(actual, expected) {
			t.Fatal("did not find expected batch")
		}
	}()

	sm := &ShoppingManager{
		ShopperCapacity: 3,
		TripTimeout:     20 * time.Millisecond,
		PendingCapacity: 100,
		Delivery:        delivery,
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
	delivery := make(chan []string)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		actual := <-delivery
		if !reflect.DeepEqual(actual, expected) {
			t.Fatal("did not find expected batch")
		}
	}()

	sm := &ShoppingManager{
		ShopperCapacity: 3,
		TripTimeout:     20 * time.Millisecond,
		PendingCapacity: 100,
		Delivery:        delivery,
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
