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
