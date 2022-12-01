package keeper

import (
	"fmt"

	sdkstoretypes "github.com/cosmos/cosmos-sdk/store/types"
	sdktypes "github.com/cosmos/cosmos-sdk/types"
)

// IndexerWriteListener is used to configure listening to a KVStore by writing
// out to a PostgreSQL DB.
type IndexerWriteListener struct {
	ctx *sdktypes.Context

	// Contract info.
	contractAddress string
	codeID          uint64
	creatorAddress  string
	adminAddress    string
	label           string

	callerAddress string
	msg           string
}

// OnWrite satisfies the WriteListener interface by writing to a PostgreSQL DB.
func (wl *IndexerWriteListener) OnWrite(storeKey sdkstoretypes.StoreKey, key []byte, value []byte, delete bool) error {
	suffix := fmt.Sprintf("TX info:\n  blockHeight = %d\n  blockTimeUnixMs = %d\n  contractAddress = %s\n  codeID = %d\n  creatorAddress = %s\n  adminAddress = %s\n  label = %s\n  callerAddress = %s\n  msg = %s", wl.ctx.BlockHeight(), wl.ctx.BlockTime().UnixMicro(), wl.contractAddress, wl.codeID, wl.creatorAddress, wl.adminAddress, wl.label, wl.callerAddress, wl.msg)

	if delete {
		wl.ctx.Logger().Error(fmt.Sprintf("\nOnWrite delete\nkey = %v (%s)\n%s", key, key, suffix))
	} else {
		wl.ctx.Logger().Error(fmt.Sprintf("\nOnWrite set\nkey = %v (%s)\nvalue = %s\n%s", key, key, value, suffix))
	}

	return nil
}
