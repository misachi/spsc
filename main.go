package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const cpuCacheLinePad = 64

type SPSCQueue struct {
	head    atomic.Int64
	_       [cpuCacheLinePad]byte // padding to prevent false sharing
	tail    atomic.Int64
	_       [cpuCacheLinePad]byte // padding to prevent false sharing
	size    int64
	dataArr []interface{}
}

func NewSPSCQueue(sz int64) *SPSCQueue {
	return &SPSCQueue{
		size:    sz,
		dataArr: make([]interface{}, sz),
	}
}

func (spsc *SPSCQueue) Push(elem interface{}) bool {
	tail := spsc.tail.Load()
	new_tail := tail + 1
	if new_tail == spsc.MaxSize() {
		new_tail = 0
	}

	// Head has not moved -- We'll be rewriting valid values; We don't want that
	if new_tail == spsc.head.Load() {
		return false
	}

	spsc.dataArr[tail] = elem
	spsc.tail.Store(new_tail)
	return true
}

func (spsc *SPSCQueue) Pop() (interface{}, error) {
	head := spsc.head.Load()
	new_head := head + 1

	if head == spsc.tail.Load() {
		return nil, fmt.Errorf("Pop: Queue is empty")
	}

	if new_head == spsc.MaxSize() {
		new_head = 0
	}

	ret := spsc.dataArr[head]
	spsc.dataArr[head] = nil
	spsc.head.Store(new_head)
	return ret, nil
}

func (spsc *SPSCQueue) MaxSize() int64 {
	return spsc.size
}

func (spsc *SPSCQueue) Size() int64 {
	return spsc.tail.Load() - spsc.head.Load()
}

func (spsc *SPSCQueue) IsFull() bool {
	return spsc.Size() >= spsc.MaxSize()
}

func (spsc *SPSCQueue) IsEmpty() bool {
	return spsc.head.Load() == spsc.tail.Load()
}

func main() {
	elemSize := 100000
	q_size := 4000
	queue := NewSPSCQueue(int64(q_size))
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < elemSize; i++ {
			if !queue.Push(i) {
				fmt.Println("Unable to add to queue")
				i -= 1 // Ensure to always push. Remove if it's okay to loose items
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		total := 0
		for {
			// Wait for producer to add to queue
			// Tweak sleep duration to get the best waiting time
			if queue.IsEmpty() {
				time.Sleep(100 * time.Microsecond)
			}

			elem, err := queue.Pop()
			if err != nil {
				fmt.Printf("%v\n", err)
				break
			}
			if elem != nil {
				total += elem.(int)
			}

		}
		fmt.Printf("End: %v\n", total)
	}()

	wg.Wait()
}
