package keeper

import (
	"encoding/json"
	"os"
	"path/filepath"

	sdkstoretypes "github.com/cosmos/cosmos-sdk/store/types"
	sdktypes "github.com/cosmos/cosmos-sdk/types"
	"github.com/tendermint/tendermint/libs/log"
)

type IndexerConfigFilter struct {
	CodeIds           []uint64 `json:"codeIds"`
	ContractAddresses []string `json:"contractAddresses"`
	Output            string   `json:"output"`
}

type IndexerConfig struct {
	Filters []IndexerConfigFilter `json:"filters"`
}

func LoadIndexerConfig(wasmdDir string) IndexerConfig {
	var config IndexerConfig
	configFile, err := os.Open(filepath.Join(wasmdDir, "indexer", "config.json"))
	if err != nil {
		panic(err.Error())
	}
	defer configFile.Close()
	jsonParser := json.NewDecoder(configFile)
	jsonParser.Decode(&config)

	// Validate at least one filter exists.
	if len(config.Filters) == 0 {
		panic("indexer.json must have at least one filter")
	}

	// Validate filters have output set, and resolve path.
	updatedFilters := config.Filters[:0]
	for _, f := range config.Filters {
		if f.Output == "" {
			panic("indexer.json filters must have output set")
		}

		// Resolve path.
		f.Output = filepath.Join(wasmdDir, "indexer/output", f.Output)
		// Create folder if doesn't exist.
		dir := filepath.Dir(f.Output)
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			os.MkdirAll(dir, os.ModePerm)
		}

		updatedFilters = append(updatedFilters, f)
	}

	return config
}

type PendingIndexerEvent struct {
	BlockHeight     int64  `json:"blockHeight"`
	BlockTimeUnixMs int64  `json:"blockTimeUnixMs"`
	ContractAddress string `json:"contractAddress"`
	CodeId          uint64 `json:"codeId"`
	Key             []byte `json:"key"`
	Value           []byte `json:"value"`
	Delete          bool   `json:"delete"`
}

// IndexerWriteListener is used to configure listening to a KVStore by writing
// out to a PostgreSQL DB.
type IndexerWriteListener struct {
	parentIndexerListener *IndexerWriteListener
	logger                log.Logger
	output                string

	queue     []PendingIndexerEvent
	committed bool

	// Contract info.
	contractAddress string
	codeID          uint64
	blockHeight     int64
	blockTimeUnixMs int64
}

func NewIndexerWriteListener(config IndexerConfig, parentIndexerListener *IndexerWriteListener, ctx *sdktypes.Context, contractAddress sdktypes.AccAddress, codeID uint64) *IndexerWriteListener {
	// Find filter match.
	var filter IndexerConfigFilter
	found := false
	for _, f := range config.Filters {
		// If there are no filters, we match.
		if len(f.CodeIds) == 0 && len(f.ContractAddresses) == 0 {
			filter = f
			found = true
			break
		}

		// If filters exist, check them.
		for _, c := range f.CodeIds {
			if c == codeID {
				filter = f
				found = true
				break
			}
		}
		for _, c := range f.ContractAddresses {
			if c == contractAddress.String() {
				filter = f
				found = true
				break
			}
		}

		// If found filter, stop.
		if found {
			break
		}
	}

	// If no filter match, don't create listener.
	if !found {
		return nil
	}

	return &IndexerWriteListener{
		parentIndexerListener: parentIndexerListener,
		logger:                ctx.Logger(),
		output:                filter.Output,

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
		BlockHeight:     wl.blockHeight,
		BlockTimeUnixMs: wl.blockTimeUnixMs,
		ContractAddress: wl.contractAddress,
		CodeId:          wl.codeID,
		Key:             key,
		Value:           value,
		Delete:          delete,
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

	// Open output file, creating if doesn't exist.
	file, err := os.OpenFile(wl.output, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		wl.logger.Error("[INDEXER] Failed to open output file", "output", wl.output, "error", err)
		panic(err.Error())
	}
	defer file.Close()

	encoder := json.NewEncoder(file)

	// Export entire queue.
	for _, event := range wl.queue {
		encoder.Encode(event)
	}

	wl.logger.Info("[INDEXER] Exported events", "blockHeight", wl.blockHeight, "codeId", wl.codeID, "contractAddress", wl.contractAddress, "count", len(wl.queue), "output", wl.output)
}
