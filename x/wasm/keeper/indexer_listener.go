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
	succeeded bool

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
		succeeded: false,

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

// Commit entire queue.
func (wl *IndexerWriteListener) commit() {
	// Add all events to parent listener queue if exists, and mark succeeded.
	if wl.parentIndexerListener != nil {
		wl.parentIndexerListener.queue = append(wl.parentIndexerListener.queue, wl.queue...)
	}
	wl.succeeded = true
}

// Finish.
func (wl *IndexerWriteListener) finish() {
	// If we failed or are not the parent listener, move the current indexer
	// listener back one. Our child listeners added to our queue when they
	// committed. The parent listener will export the whole queue once it
	// finishes.
	if !wl.succeeded || wl.parentIndexerListener != nil {
		CurrentIndexerListener = wl.parentIndexerListener
		return
	}

	for _, event := range wl.queue {
		suffix := fmt.Sprintf("Info:\n  blockHeight = %d\n  blockTimeUnixMs = %d\n  contractAddress = %s\n  codeID = %d\n", event.blockHeight, event.blockTimeUnixMs, wl.contractAddress, wl.codeID)

		if event.delete {
			wl.logger.Error(fmt.Sprintf("\n\ndelete\nkey = %v (%s)\n%s", event.key, event.key, suffix))
		} else {
			wl.logger.Error(fmt.Sprintf("\n\nset\nkey = %v (%s)\nvalue = %s\n%s", event.key, event.key, event.value, suffix))
		}
	}

	// Unset the keeper's current listener since we're done with this chain of
	// messages.
	CurrentIndexerListener = nil
}
