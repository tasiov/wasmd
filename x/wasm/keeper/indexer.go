package keeper

import (
	"encoding/base64"
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
}

type IndexerConfig struct {
	Filter IndexerConfigFilter `json:"filter"`
	// Set manually.
	Output string
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

	// Resolve output path.
	config.Output = filepath.Join(wasmdDir, "indexer", ".events.txt")
	// Create folder if doesn't exist.
	dir := filepath.Dir(config.Output)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.MkdirAll(dir, os.ModePerm)
	}

	return config
}

type PendingIndexerEvent struct {
	BlockHeight     int64  `json:"blockHeight"`
	BlockTimeUnixMs int64  `json:"blockTimeUnixMs"`
	ContractAddress string `json:"contractAddress"`
	CodeId          uint64 `json:"codeId"`
	Key             string `json:"key"`
	Value           string `json:"value"`
	Delete          bool   `json:"delete"`
}

// IndexerWriteListener is used to configure listening to a KVStore by writing
// out to a PostgreSQL DB.
type IndexerWriteListener struct {
	parentIndexerListener *IndexerWriteListener
	logger                log.Logger
	output                string

	queue     map[string]PendingIndexerEvent
	committed bool

	// Contract info.
	contractAddress string
	codeID          uint64
	blockHeight     int64
	blockTimeUnixMs int64
}

func NewIndexerWriteListener(config IndexerConfig, parentIndexerListener *IndexerWriteListener, ctx *sdktypes.Context, contractAddress sdktypes.AccAddress, codeID uint64) *IndexerWriteListener {
	// If there are any filters set, check them.
	if len(config.Filter.CodeIds) != 0 || len(config.Filter.ContractAddresses) != 0 {
		found := false

		for _, c := range config.Filter.CodeIds {
			if c == codeID {
				found = true
				break
			}
		}

		for _, c := range config.Filter.ContractAddresses {
			if c == contractAddress.String() {
				found = true
				break
			}
		}

		// If filter is set and we didn't find a match, don't create a listener.
		if !found {
			return nil
		}
	}

	return &IndexerWriteListener{
		parentIndexerListener: parentIndexerListener,
		logger:                ctx.Logger(),
		output:                config.Output,

		queue:     make(map[string]PendingIndexerEvent),
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
	keyBase64 := base64.StdEncoding.EncodeToString(key)
	valueBase64 := base64.StdEncoding.EncodeToString(value)

	wl.queue[keyBase64] = PendingIndexerEvent{
		BlockHeight:     wl.blockHeight,
		BlockTimeUnixMs: wl.blockTimeUnixMs,
		ContractAddress: wl.contractAddress,
		CodeId:          wl.codeID,
		Key:             keyBase64,
		Value:           valueBase64,
		Delete:          delete,
	}

	return nil
}

// Commit entire queue. This should be called if the transaction succeeds.
func (wl *IndexerWriteListener) commit() {
	// Add all events to parent listener queue if exists.
	if wl.parentIndexerListener != nil {
		for k, v := range wl.queue {
			wl.parentIndexerListener.queue[k] = v
		}
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
