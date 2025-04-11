package buffer

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"math/rand"

	"slices"

	"github.com/JyotinderSingh/dropdb/file"
	"github.com/JyotinderSingh/dropdb/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	numBuffers int = 5
)

// testBufferConfig holds configuration needed for buffer replacement strategy tests
type testBufferConfig struct {
	fileManager *file.Manager
	logManager  *log.Manager
	cleanup     func()
}

// setupStrategyTest creates a new test environment with temporary database directory
// and returns a testBufferConfig with initialized file/log managers and cleanup function
func setupStrategyTest(t *testing.T) *testBufferConfig {
	t.Helper()
	dbDir := filepath.Join(os.TempDir(), "testdb")
	require.NoError(t, os.MkdirAll(dbDir, 0755))

	fileManager, err := file.NewManager(dbDir, 400)
	require.NoError(t, err)

	logManager, err := log.NewManager(fileManager, "testlog")
	require.NoError(t, err)

	cleanup := func() {
		_ = os.RemoveAll(dbDir)
	}

	return &testBufferConfig{
		fileManager: fileManager,
		logManager:  logManager,
		cleanup:     cleanup,
	}
}

// createInitialTestBuffers creates a slice of test buffers where even-indexed buffers are pinned
// and odd-indexed buffers are unpinned
func createInitialTestBuffers(config *testBufferConfig, numBuffers int) []*Buffer {
	buffers := make([]*Buffer, numBuffers)
	for i := 0; i < len(buffers); i++ {
		buffers[i] = NewBuffer(config.fileManager, config.logManager)
		//mark even indexed buffers as pinned.
		if i%2 == 0 {
			buffers[i].pin()
		}
	}
	return buffers
}

// newTestStrategy creates and initializes a new FIFO replacement strategy with test buffers
func newTestStrategy(config *testBufferConfig) ReplacementStrategy {
	buffers := createInitialTestBuffers(config, numBuffers)
	strategy := NewFifoStrategy()
	strategy.initialize(buffers)
	return strategy
}

// selectRandPinnedBuffer returns a random pinned buffer (even-indexed) from the buffer pool
func selectRandPinnedBuffer(buffers []*Buffer, numBuffers int) *Buffer {
	// gets random integer from [0, numBuffers) range.
	var bufferInd int = rand.Intn(numBuffers)
	// loop till you don't find even index
	for bufferInd%2 != 0 {
		bufferInd = rand.Intn(numBuffers)
	}

	// returns pinned buffer present at even index
	return buffers[bufferInd]
}

// selectRandUnpinnedBuffer returns a random unpinned buffer (odd-indexed) from the buffer pool
func selectRandUnpinnedBuffer(buffers []*Buffer, numBuffers int) *Buffer {
	// gets random integer from [0, numBuffers) range.
	var bufferInd int = rand.Intn(numBuffers)
	// loop till you don't find odd index
	for bufferInd%2 == 0 {
		bufferInd = rand.Intn(numBuffers)
	}

	// returns unpinned buffer present at odd index
	return buffers[bufferInd]
}

// TestInitialize contains test cases for the initialize method of the FIFO replacement strategy
func TestInitialize(t *testing.T) {
	// Test that all buffers are properly initialized in the strategy
	t.Run("initializes all the buffers to strategy", func(t *testing.T) {
		config := setupStrategyTest(t)
		defer config.cleanup()

		IStrategy := newTestStrategy(config)
		strategy := IStrategy.(*FifoStrategy)
		// Verify that the total number of buffers matches expected
		if len(strategy.buffers) != numBuffers {
			t.Errorf("Expected %d buffers in total, got %d", numBuffers, len(strategy.buffers))
		}

	})

	// Test that the queue size matches the number of unpinned buffers
	t.Run("makes sure that queue size equals no. of unpinned buffers", func(t *testing.T) {
		config := setupStrategyTest(t)
		defer config.cleanup()

		IStrategy := newTestStrategy(config)
		strategy := IStrategy.(*FifoStrategy)
		var numUnpinnedBuffers int = 0
		// Count number of unpinned buffers
		for i := 0; i < numBuffers; i++ {
			if !strategy.buffers[i].isPinned() {
				numUnpinnedBuffers++
			}
		}

		// Verify queue size matches unpinned buffer count
		if len(strategy.queue) != numUnpinnedBuffers {
			t.Errorf("Expected %d unpinned buffers in queue, got %d", numUnpinnedBuffers, len(strategy.queue))
		}
	})

	// Test that only unpinned buffers are present in the queue
	t.Run("makes sure that queue contains only unpinned buffers", func(t *testing.T) {
		config := setupStrategyTest(t)
		defer config.cleanup()

		IStrategy := newTestStrategy(config)
		strategy := IStrategy.(*FifoStrategy)
		// Check each buffer in queue is unpinned
		for _, queueBuff := range strategy.queue {
			if queueBuff.isPinned() {
				t.Error("Expected unpinned buffer in queue, got pinned")
			}
		}
	})

}

// TestPinBuffer contains test cases for the pinBuffer method of the FIFO replacement strategy
func TestPinBuffer(t *testing.T) {
	// Test that pinned buffers are removed from the queue
	t.Run("makes sure that pinned buffer has been removed from the queue", func(t *testing.T) {
		config := setupStrategyTest(t)
		defer config.cleanup()

		buffers := createInitialTestBuffers(config, numBuffers)
		strategy := NewFifoStrategy()
		strategy.initialize(buffers)
		buf := selectRandUnpinnedBuffer(strategy.buffers, numBuffers)
		buf.pin()
		// Notify strategy of buffer pin
		strategy.pinBuffer(buf)

		// Verify pinned buffer is not in queue
		for i := 0; i < len(strategy.queue); i++ {
			if strategy.queue[i] == buf {
				t.Errorf("Expected buffer at index %d to be removed from the queue after getting pinned", i)
			}
		}
	})

	// Test that queue size decreases after pinning a buffer
	t.Run("makes sure that queue size decreases by 1 after the buffer has been unpinned", func(t *testing.T) {
		config := setupStrategyTest(t)
		defer config.cleanup()

		buffers := createInitialTestBuffers(config, numBuffers)
		strategy := NewFifoStrategy()
		strategy.initialize(buffers)
		buf := selectRandUnpinnedBuffer(strategy.buffers, numBuffers)
		queueSizeBeforeOp := len(strategy.queue)
		buf.pin()
		// Notify strategy of buffer pin
		strategy.pinBuffer(buf)

		assert.Equal(t, queueSizeBeforeOp-1, len(strategy.queue), fmt.Sprintf("Expected %d to be %d", queueSizeBeforeOp-1, len(strategy.queue)))
	})

	// Test that pinning an already pinned buffer doesn't affect queue size
	t.Run("makes sure that queue size does not decrease after it has been unpinned once.", func(t *testing.T) {
		config := setupStrategyTest(t)
		defer config.cleanup()

		buffers := createInitialTestBuffers(config, numBuffers)
		strategy := NewFifoStrategy()
		strategy.initialize(buffers)
		buf := selectRandUnpinnedBuffer(strategy.buffers, numBuffers)
		queueSizeBeforeOp := len(strategy.queue)
		buf.pin()
		// First pin notification
		strategy.pinBuffer(buf)

		assert.Equal(t, queueSizeBeforeOp-1, len(strategy.queue), fmt.Sprintf("Expected %d to be equal to %d", queueSizeBeforeOp-1, len(strategy.queue)))
		queueSizeBeforeOp = len(strategy.queue)
		// Second pin notification should not affect queue size
		strategy.pinBuffer(buf)
		assert.Equal(t, queueSizeBeforeOp, len(strategy.queue), fmt.Sprintf("Expected %d to be equal to %d", queueSizeBeforeOp, len(strategy.queue)))
	})
}

// TestUnpinBuffer contains test cases for the unpinBuffer method of the FIFO replacement strategy
func TestUnpinBuffer(t *testing.T) {

	// Test that queue size increases after unpinning a buffer
	t.Run("makes sure that queue size has been incremented by 1 after buffer has been unpinned", func(t *testing.T) {
		config := setupStrategyTest(t)
		defer config.cleanup()

		buffers := createInitialTestBuffers(config, numBuffers)
		strategy := NewFifoStrategy()
		strategy.initialize(buffers)
		buf := selectRandPinnedBuffer(strategy.buffers, numBuffers)
		// Record initial queue size
		queueSizeBeforeOp := len(strategy.queue)
		buf.unpin()
		// Notify strategy of buffer unpin
		strategy.unpinBuffer(buf)
		queueSizeAfterOp := len(strategy.queue)

		assert.Equal(t, queueSizeBeforeOp+1, queueSizeAfterOp, "Buffer could not be unpinned")
	})

	// Test that unpinned buffer is added to the queue
	t.Run("makes sure that buffer is present in the queue, after being unpinned", func(t *testing.T) {
		config := setupStrategyTest(t)
		defer config.cleanup()

		buffers := createInitialTestBuffers(config, numBuffers)
		strategy := NewFifoStrategy()
		strategy.initialize(buffers)
		buf := selectRandPinnedBuffer(strategy.buffers, numBuffers)
		buf.unpin()
		// Notify strategy of buffer unpin
		strategy.unpinBuffer(buf)
		isPresent := slices.Contains(strategy.queue, buf)
		if !isPresent {
			t.Errorf("Expected buffer to be appended to the queue")
		}
	})

	// Test that unpinning an already unpinned buffer doesn't affect queue size
	t.Run("makes sure that buffer is present in the queue, after being unpinned", func(t *testing.T) {
		config := setupStrategyTest(t)
		defer config.cleanup()

		buffers := createInitialTestBuffers(config, numBuffers)
		strategy := NewFifoStrategy()
		strategy.initialize(buffers)
		buf := selectRandPinnedBuffer(strategy.buffers, numBuffers)
		buf.unpin()
		// First unpin notification
		strategy.unpinBuffer(buf)
		queueSizeAfterOp := len(strategy.queue)
		buf = selectRandPinnedBuffer(strategy.buffers, numBuffers)
		// Second unpin notification should not affect queue size
		strategy.unpinBuffer(buf)

		assert.Equal(t, queueSizeAfterOp, len(strategy.queue), "Duplicate buffer added to the queue")
	})
}

// TestChooseUnpinnedBuffer contains test cases for the chooseUnpinnedBuffer method
func TestChooseUnpinnedBuffer(t *testing.T) {
	// Test that the first buffer in queue is selected for replacement
	t.Run("queue's first element must be selected for replacement", func(t *testing.T) {
		config := setupStrategyTest(t)
		defer config.cleanup()

		Istrategy := newTestStrategy(config)
		strategy := Istrategy.(*FifoStrategy)
		buffToBeRemoved := strategy.queue[0]
		buf := strategy.chooseUnpinnedBuffer()

		if buf != buffToBeRemoved {
			t.Errorf("Expected queue's previous top element must be same as buffer chosen by strategy for replacement")
		}

	})

	// Test that queue size decreases after selecting a buffer for replacement
	t.Run("queue's size must be decreased by 1 after buffer is selected for replacement", func(t *testing.T) {
		config := setupStrategyTest(t)
		defer config.cleanup()

		Istrategy := newTestStrategy(config)
		strategy := Istrategy.(*FifoStrategy)
		queueSizeBeforeOp := len(strategy.queue)
		_ = strategy.chooseUnpinnedBuffer()
		assert.Equal(t, queueSizeBeforeOp-1, len(strategy.queue), fmt.Sprintf("Expected %d to be %d", queueSizeBeforeOp-1, len(strategy.queue)))
	})
}

// TestChooseUnpinnedBufferEmptyQueue tests the behavior of chooseUnpinnedBuffer when all buffers are pinned
func TestChooseUnpinnedBufferEmptyQueue(t *testing.T) {
	config := setupStrategyTest(t)
	defer config.cleanup()

	buffers := make([]*Buffer, numBuffers)
	for i := 0; i < len(buffers); i++ {
		buffers[i] = NewBuffer(config.fileManager, config.logManager)
		buffers[i].pin()
	}

	strategy := NewFifoStrategy()
	strategy.initialize(buffers)

	// Verify nil is returned when no unpinned buffers are available
	buf := strategy.chooseUnpinnedBuffer()
	assert.Nil(t, buf, "Expected nil when no unpinned buffers available")
}

// TestConcurrentFifoQueueAccess tests concurrent access to the FIFO queue through pin/unpin operations
func TestConcurrentFifoQueueAccess(t *testing.T) {
	config := setupStrategyTest(t)
	defer config.cleanup()

	IStrategy := newTestStrategy(config)
	strategy := IStrategy.(*FifoStrategy)

	var wg sync.WaitGroup
	operationCount := 100

	wg.Add(operationCount * 2)

	for i := 0; i < operationCount; i++ {
		go func() {
			defer wg.Done()
			buf := selectRandPinnedBuffer(strategy.buffers, numBuffers)
			buf.unpin()
			strategy.unpinBuffer(buf)
		}()

		go func() {
			defer wg.Done()
			buf := selectRandUnpinnedBuffer(strategy.buffers, numBuffers)
			buf.pin()
			strategy.pinBuffer(buf)
		}()
	}

	// Wait for all operations to complete
	wg.Wait()

	// Verify test completed without deadlocks or panics
	assert.True(t, true, "Concurrent operations completed without panic")
}
