package main

import (
	"fmt"
	"sync"

	"github.com/legitYosal/vmware-s3-backup/pkg/vms3"
)

const MaxNumberOfInMemoryParts = 10

type LruCache struct {
	buffers            map[int32][]byte
	bufferBusy         map[int32]bool
	partsBufferMapping map[int32]int32
	partsBusy          map[int32]bool
	numberOfParts      int32
	queue              []int32
	mutex              sync.RWMutex
}

func NewLruCache() *LruCache {
	buffers := make(map[int32][]byte, MaxNumberOfInMemoryParts)
	for i := int32(0); i < MaxNumberOfInMemoryParts; i++ {
		buffers[i] = make([]byte, vms3.MaxChunkSize)
	}
	return &LruCache{
		buffers:            buffers,
		bufferBusy:         make(map[int32]bool),
		partsBufferMapping: make(map[int32]int32),
		partsBusy:          make(map[int32]bool),
		queue:              make([]int32, 0),
		mutex:              sync.RWMutex{},
	}
}

func (c *LruCache) GetNumberOfParts() int32 {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.numberOfParts
}

func (c *LruCache) GetPart(partNumber int32) ([]byte, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	bufferIndex, ok := c.partsBufferMapping[partNumber]
	if !ok {
		return nil, fmt.Errorf("part %d not found", partNumber)
	}
	return c.buffers[bufferIndex], nil
}

func (c *LruCache) HasPart(partNumber int32) bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	_, ok := c.partsBufferMapping[partNumber]
	return ok
}

func (c *LruCache) ReadFrom(partNumber int32, offset uint64, length uint64) ([]byte, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	bufferIndex, ok := c.partsBufferMapping[partNumber]
	if !ok {
		if IsSparse(partNumber) {
			return zeroBuffer[:length], nil
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
	return c.buffers[bufferIndex][offset : offset+length], nil // we have checked boundary before calling this function
}

func (c *LruCache) MakePartBusy(partNumber int32) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.partsBusy[partNumber] = true
	return nil
}

func (c *LruCache) MakePartFree(partNumber int32) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	_, ok := c.partsBusy[partNumber]
	if ok {
		c.partsBusy[partNumber] = false
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
			if c.partsBusy[partNumber] {
				continue
			}
			deletedAny = true
			buffIndex := c.partsBufferMapping[partNumber]
			c.bufferBusy[buffIndex] = false
			copy(c.buffers[buffIndex], zeroBuffer)
			delete(c.partsBufferMapping, partNumber)
			delete(c.partsBusy, partNumber)
			c.queue = append(c.queue[:index], c.queue[index+1:]...)
			c.numberOfParts--
			break
		}
		if !deletedAny {
			return fmt.Errorf("failed to delete any part from cache, all parts are locked")
		}
	}
	for i := int32(0); i < MaxNumberOfInMemoryParts; i++ {
		if !c.bufferBusy[i] {
			copy(c.buffers[i], data)
			c.bufferBusy[i] = true
			c.partsBufferMapping[partNumber] = i
			c.partsBusy[partNumber] = true
			c.queue = append(c.queue, partNumber)
			c.numberOfParts++
			return nil
		}
	}
	return fmt.Errorf("failed to add part to cache, all buffers are busy")
}
