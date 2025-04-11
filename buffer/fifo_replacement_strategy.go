package buffer

import (
	"sync"
)

// compile time check to verify if FifoStrategy implements ReplacementStrategy interface
var _ ReplacementStrategy = (*FifoStrategy)(nil)

// FifoStrategy collects all the unpinned buffers during initialization in the FIFO queue
type FifoStrategy struct {
	ReplacementStrategy
	buffers []*Buffer
	mu      sync.Mutex
	// queue containing only the unpinned buffers
	queue []*Buffer
}

// NewFifoStrategy creates a new NaiveStrategy.
func NewFifoStrategy() *FifoStrategy {
	return &FifoStrategy{}
}

// Initialize initializes the strategy with the buffer pool.
func (fs *FifoStrategy) initialize(buffers []*Buffer) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.buffers = buffers

	// store all the initial unpinned buffers to the queue
	for _, buff := range fs.buffers {
		if !buff.isPinned() {
			fs.queue = append(fs.queue, buff)
		}
	}
}

// PinBuffer notifies the strategy that a buffer has been pinned.
func (fs *FifoStrategy) pinBuffer(buff *Buffer) {
	// already been pinned, do nothing
	fs.mu.Lock()
	defer fs.mu.Unlock()
	// invalid buffer: buffer has not been pinned
	if !buff.isPinned() {
		return
	}

	// find and remove the block as it has been marked as pinned
	for ind, queuedBuff := range fs.queue {
		if queuedBuff == buff {
			fs.queue = append(fs.queue[:ind], fs.queue[ind+1:]...)
			break
		}
	}
}

// UnpinBuffer notifies the strategy that a buffer has been unpinned.
func (fs *FifoStrategy) unpinBuffer(buff *Buffer) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	// invalid buffer: buffer is still pinned, thus cannot be marked for replacement
	if buff.isPinned() {
		return
	}

	// check if buffer is already present in queue or not
	for _, queuedBuff := range fs.queue {
		if queuedBuff == buff {
			// duplicate buffer found in the queue
			return
		}
	}

	// buff becomes available for replacement
	fs.queue = append(fs.queue, buff)
}

// ChooseUnpinnedBuffer selects an unpinned buffer to replace.
func (fs *FifoStrategy) chooseUnpinnedBuffer() *Buffer {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if len(fs.queue) > 0 {
		buff := fs.queue[0]
		fs.queue = fs.queue[1:]
		return buff
	}
	return nil
}
