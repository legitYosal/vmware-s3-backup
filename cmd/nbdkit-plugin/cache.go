package main

import (
	"fmt"
	"sync"
)

const MaxNumberOfInMemoryParts = 10

type LruCache struct {
	parts         map[int32][]byte
	locks         map[int32]bool
	numberOfParts int32
	queue         []int32
	mutex         sync.RWMutex
}

func NewLruCache() *LruCache {
	return &LruCache{
		parts: make(map[int32][]byte),
		locks: make(map[int32]bool),
		queue: make([]int32, 0),
		mutex: sync.RWMutex{},
	}
}

func (c *LruCache) GetPart(partNumber int32) ([]byte, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	part, ok := c.parts[partNumber]
	if !ok {
		return nil, fmt.Errorf("part %d not found", partNumber)
	}
	return part, nil
}

func (c *LruCache) HasPart(partNumber int32) bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	_, ok := c.parts[partNumber]
	return ok
}

func (c *LruCache) ReadFrom(partNumber int32, offset uint64, length uint64) ([]byte, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	part, ok := c.parts[partNumber]
	if !ok {
		if IsSparse(partNumber) {
			return make([]byte, length), nil
		}
		return nil, fmt.Errorf("part %d not found", partNumber)
	}
	// we move the part to the end of the queue
	index := -1
	for i, p := range c.queue {
		if p == partNumber {
			index = i
			break
		}
	}
	c.queue = append(c.queue[:index], c.queue[index+1:]...)
	c.queue = append(c.queue, partNumber)
	return part[offset : offset+length], nil // we have checked boundary before calling this function
}

func (c *LruCache) LockPart(partNumber int32) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.locks[partNumber] = true
	return nil
}

func (c *LruCache) UnlockPart(partNumber int32) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	_, ok := c.parts[partNumber]
	if ok {
		c.locks[partNumber] = false
	}
	return nil
}

func (c *LruCache) AddPart(partNumber int32, data []byte) error {
	// we can later add a hit counter and then sort by hits and remove the least hits
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.numberOfParts >= MaxNumberOfInMemoryParts {
		deletedAny := false
		for index, partNumber := range c.queue {
			if c.locks[partNumber] {
				continue
			}
			deletedAny = true
			delete(c.parts, partNumber)
			delete(c.locks, partNumber)
			c.queue = append(c.queue[:index], c.queue[index+1:]...)
			c.numberOfParts--
			break
		}
		if !deletedAny {
			return fmt.Errorf("failed to delete any part from cache, all parts are locked")
		}
	}
	c.parts[partNumber] = data
	c.locks[partNumber] = true
	c.queue = append(c.queue, partNumber)
	c.numberOfParts++
	return nil
}
