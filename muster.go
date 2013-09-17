// Package muster provides a framework to write batching enabled libraries. The
// batching is triggered based on a maximum number items in a batch, and/or
// based on a timeout for how long a batch waits before it is dispatched.
package muster

import (
	"errors"
	"sync"
	"time"
)

var (
	errZeroMaxBatchSize = errors.New("muster: invalid zero MaxBatchSize")
	errZeroBatchTimeout = errors.New("muster: invalid zero BatchTimeout")
)

// The notifier is used to indicate to the Client when a batch has finished
// processing.
type Notifier interface {
	// Calling Done will indicate the batch has finished processing.
	Done()
}

// Represents a single batch where items will be added and Fire will be called
// exactly once. The Batch does not need to be safe for concurrent access;
// synchronization will be handled by the Client.
type Batch interface {
	// This should add the given single item to the Batch.
	Add(item interface{})

	// Fire off the Batch. It should call Notifier.Done() when it has finished
	// processing the Batch.
	Fire(notifier Notifier)
}

// Makes empty Batches.
type BatchMaker interface {
	// Makes a new empty batch.
	MakeBatch() Batch
}

// The BatchMakerFunc type is an adapter to allow the use of ordinary functions
// as BatchMakers.
type BatchMakerFunc func() Batch

func (f BatchMakerFunc) MakeBatch() Batch {
	return f()
}

// The Client manages the background process that makes, populates & fires
// Batches.
type Client struct {
	PendingCapacity int              // Capacity of work channel.
	MaxBatchSize    int              // Maximum number of items in a batch.
	BatchTimeout    time.Duration    // Duration after which to send a pending batch.
	BatchMaker      BatchMaker       // Makes empty Batches.
	Work            chan interface{} // Send work items here to add to batch.
	workGroup       sync.WaitGroup
}

// Start the background worker goroutines and get ready for accepting requests.
func (c *Client) Start() error {
	if c.MaxBatchSize == 0 {
		return errZeroMaxBatchSize
	}
	if int64(c.BatchTimeout) == 0 {
		return errZeroMaxBatchSize
	}

	c.Work = make(chan interface{}, c.PendingCapacity)
	c.workGroup.Add(1) // this is the worker itself
	go c.worker()
	return nil
}

// Gracefully stop and return once all processing has finished.
func (c *Client) Stop() error {
	close(c.Work)
	c.workGroup.Wait()
	return nil
}

// Background process.
func (c *Client) worker() {
	defer c.workGroup.Done()
	var batch = c.BatchMaker.MakeBatch()
	var count int
	var batchTimeout <-chan time.Time
	send := func() {
		c.workGroup.Add(1)
		go batch.Fire(&c.workGroup)
		batch = c.BatchMaker.MakeBatch()
		count = 0
		batchTimeout = nil
	}
	add := func(item interface{}) {
		batch.Add(item)
		count++
		if count >= c.MaxBatchSize {
			send()
		} else if batchTimeout == nil {
			batchTimeout = time.After(c.BatchTimeout)
		}
	}
	for {
		// We use two selects in order to first prefer draining the work queue.
		select {
		case item, open := <-c.Work:
			if !open {
				send()
				return
			}
			add(item)
		default:
			select {
			case item, open := <-c.Work:
				if !open {
					send()
					return
				}
				add(item)
			case <-batchTimeout:
				send()
			}
		}
	}
}
