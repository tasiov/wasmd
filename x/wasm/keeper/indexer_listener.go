package keeper

import (
	"fmt"

	sdkstoretypes "github.com/cosmos/cosmos-sdk/store/types"
	sdktypes "github.com/cosmos/cosmos-sdk/types"
	"github.com/tendermint/tendermint/libs/log"
)

type PendingIndexerEvent struct {
	contractAddress string
	codeID          uint64
	blockHeight     int64
	blockTimeUnixMs int64
	key             []byte
	value           []byte
	delete          bool
}

// IndexerWriteListener is used to configure listening to a KVStore by writing
// out to a PostgreSQL DB.
type IndexerWriteListener struct {
	parentIndexerListener *IndexerWriteListener
	logger                log.Logger

	queue     []PendingIndexerEvent
	committed bool

	// Contract info.
	contractAddress string
	codeID          uint64
	blockHeight     int64
	blockTimeUnixMs int64
}

func NewIndexerWriteListener(parentIndexerListener *IndexerWriteListener, ctx *sdktypes.Context, contractAddress sdktypes.AccAddress, codeID uint64) *IndexerWriteListener {
	return &IndexerWriteListener{
		parentIndexerListener: parentIndexerListener,
		logger:                ctx.Logger(),

		queue:     make([]PendingIndexerEvent, 0),
		committed: false,

		// Contract info.
		contractAddress: contractAddress.String(),
		codeID:          codeID,
		blockHeight:     ctx.BlockHeight(),
		blockTimeUnixMs: ctx.BlockTime().UnixMicro(),
	}
}

// Add write events to queue.
func (wl *IndexerWriteListener) OnWrite(storeKey sdkstoretypes.StoreKey, key []byte, value []byte, delete bool) error {
	wl.queue = append(wl.queue, PendingIndexerEvent{
		blockHeight:     wl.blockHeight,
		blockTimeUnixMs: wl.blockTimeUnixMs,
		contractAddress: wl.contractAddress,
		codeID:          wl.codeID,
		key:             key,
		value:           value,
		delete:          delete,
	})

	return nil
}

// Commit entire queue. This should be called if the transaction succeeds.
func (wl *IndexerWriteListener) commit() {
	// Add all events to parent listener queue if exists.
	if wl.parentIndexerListener != nil {
		wl.parentIndexerListener.queue = append(wl.parentIndexerListener.queue, wl.queue...)
	}
	wl.committed = true
}

// If we failed to commit or are not the parent listener, don't export the
// queue. If we are not the root listener, we added our events to our parent's
// queue when we committed. Our child listeners added their events to our queue
// if they committed. The root (parent with no parent) listener will export the
// whole queue (below) if it commits.
func (wl *IndexerWriteListener) finish() {
	// Move the current indexer listener pointer up one to our parent. If we are
	// the root listener, this will be nil.
	CurrentIndexerListener = wl.parentIndexerListener

	if !wl.committed || wl.parentIndexerListener != nil {
		return
	}

	// Export entire queue.
	for _, event := range wl.queue {
		suffix := fmt.Sprintf("Info:\n  blockHeight = %d\n  blockTimeUnixMs = %d\n  contractAddress = %s\n  codeID = %d\n", event.blockHeight, event.blockTimeUnixMs, wl.contractAddress, wl.codeID)

		if event.delete {
			wl.logger.Error(fmt.Sprintf("\n\ndelete\nkey = %v (%s)\n%s", event.key, event.key, suffix))
		} else {
			wl.logger.Error(fmt.Sprintf("\n\nset\nkey = %v (%s)\nvalue = %s\n%s", event.key, event.key, event.value, suffix))
		}
	}
}
