package keeper

import (
	"fmt"

	sdkstoretypes "github.com/cosmos/cosmos-sdk/store/types"
	sdktypes "github.com/cosmos/cosmos-sdk/types"
)

type PendingWrite struct {
	key    []byte
	value  []byte
	delete bool
}

// IndexerWriteListener is used to configure listening to a KVStore by writing
// out to a PostgreSQL DB.
type IndexerWriteListener struct {
	queue []PendingWrite

	ctx    *sdktypes.Context
	source string

	// Contract info.
	contractAddress string
	codeID          uint64
	creatorAddress  string
	adminAddress    string
	label           string

	callerAddress string
	msg           string
}

func NewIndexerWriteListener(ctx sdktypes.Context, source string, contractAddress sdktypes.AccAddress, codeID uint64, creatorAddress string, adminAddress string, label string, callerAddress sdktypes.AccAddress, msg []byte) *IndexerWriteListener {
	return &IndexerWriteListener{
		queue: []PendingWrite{},

		ctx:    &ctx,
		source: source,

		// Contract info.
		contractAddress: contractAddress.String(),
		codeID:          codeID,
		creatorAddress:  creatorAddress,
		adminAddress:    adminAddress,
		label:           label,
		msg:             string(msg),

		callerAddress: callerAddress.String(),
	}
}

// Add write events to queue.
func (wl *IndexerWriteListener) OnWrite(storeKey sdkstoretypes.StoreKey, key []byte, value []byte, delete bool) error {
	// If checking TX (simulating, not actually executing), do nothing.
	if wl.ctx.IsCheckTx() {
		return nil
	}

	wl.queue = append(wl.queue, PendingWrite{
		key:    key,
		value:  value,
		delete: delete,
	})

	return nil
}

// Commit entire queue.
func (wl *IndexerWriteListener) commit() {
	// If checking TX (simulating, not actually executing), do nothing.
	if wl.ctx.IsCheckTx() {
		return
	}

	suffix := fmt.Sprintf("Info:\n  blockHeight = %d\n  blockTimeUnixMs = %d\n  contractAddress = %s\n  codeID = %d\n  creatorAddress = %s\n  adminAddress = %s\n  label = %s\n  callerAddress = %s\n  msg = %s", wl.ctx.BlockHeight(), wl.ctx.BlockTime().UnixMicro(), wl.contractAddress, wl.codeID, wl.creatorAddress, wl.adminAddress, wl.label, wl.callerAddress, wl.msg)

	for _, write := range wl.queue {
		if write.delete {
			wl.ctx.Logger().Error(fmt.Sprintf("\n%s delete\nkey = %v (%s)\n%s", wl.source, write.key, write.key, suffix))
		} else {
			wl.ctx.Logger().Error(fmt.Sprintf("\n%s set\nkey = %v (%s)\nvalue = %s\n%s", wl.source, write.key, write.key, write.value, suffix))
		}
	}
}
