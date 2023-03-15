package keeper

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	wasmvmtypes "github.com/CosmWasm/wasmvm/types"
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
	// Read config from file if it exists. Otherwise, use default config.
	if err == nil {
		defer configFile.Close()
		jsonParser := json.NewDecoder(configFile)
		jsonParser.Decode(&config)
	}

	// Resolve output path.
	config.Output = filepath.Join(wasmdDir, "indexer", "wasm.out")
	// Create folder if doesn't exist.
	dir := filepath.Dir(config.Output)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.MkdirAll(dir, os.ModePerm)
	}

	return config
}

// State

type PendingIndexerStateEvent struct {
	Type            string `json:"type"`
	BlockHeight     int64  `json:"blockHeight"`
	BlockTimeUnixMs int64  `json:"blockTimeUnixMs"`
	ContractAddress string `json:"contractAddress"`
	CodeId          uint64 `json:"codeId"`
	Key             string `json:"key"`
	Value           string `json:"value"`
	Delete          bool   `json:"delete"`
}

type IndexerStateWriteListener struct {
	parentIndexerListener *IndexerStateWriteListener
	logger                log.Logger
	output                string

	queue     map[string]PendingIndexerStateEvent
	committed bool

	// Contract info.
	contractAddress string
	codeID          uint64
	blockHeight     int64
	blockTimeUnixMs int64
}

func NewIndexerStateWriteListener(config IndexerConfig, parentIndexerListener *IndexerStateWriteListener, ctx *sdktypes.Context, contractAddress sdktypes.AccAddress, codeID uint64) *IndexerStateWriteListener {
	// If there are any filters set, check them.
	if len(config.Filter.CodeIds) != 0 || len(config.Filter.ContractAddresses) != 0 {
		found := false

		if len(config.Filter.CodeIds) != 0 {
			for _, c := range config.Filter.CodeIds {
				if c == codeID {
					found = true
					break
				}
			}
		}

		// Only check contract addresses if we have not yet found a match.
		if !found && len(config.Filter.ContractAddresses) != 0 {
			for _, c := range config.Filter.ContractAddresses {
				if c == contractAddress.String() {
					found = true
					break
				}
			}
		}

		// If filter is set and we didn't find a match, don't create a listener.
		if !found {
			return nil
		}
	}

	return &IndexerStateWriteListener{
		parentIndexerListener: parentIndexerListener,
		logger:                ctx.Logger(),
		output:                config.Output,

		queue:     make(map[string]PendingIndexerStateEvent),
		committed: false,

		// Contract info.
		contractAddress: contractAddress.String(),
		codeID:          codeID,
		blockHeight:     ctx.BlockHeight(),
		blockTimeUnixMs: ctx.BlockTime().UnixMilli(),
	}
}

// Add write events to queue.
func (wl *IndexerStateWriteListener) OnWrite(storeKey sdkstoretypes.StoreKey, key []byte, value []byte, delete bool) error {
	keyBase64 := base64.StdEncoding.EncodeToString(key)
	valueBase64 := base64.StdEncoding.EncodeToString(value)

	// Values are unique to the pair of contract address and key. The last value
	// write (set or delete) in a block wins. This listener/queue only exists in
	// the context of a single block, so we don't need to include block height in
	// this unique key.
	queueKey := wl.contractAddress + keyBase64

	wl.queue[queueKey] = PendingIndexerStateEvent{
		Type:            "state",
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
func (wl *IndexerStateWriteListener) commit() {
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
func (wl *IndexerStateWriteListener) finish() {
	// Move the current indexer listener pointer up one to our parent. If we are
	// the root listener, this will be nil.
	CurrentIndexerListener = wl.parentIndexerListener

	if !wl.committed || wl.parentIndexerListener != nil {
		return
	}

	// Open output file, creating if doesn't exist.
	file, err := os.OpenFile(wl.output, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		wl.logger.Error("[INDEXER][wasm/state] Failed to open output file", "output", wl.output, "error", err)
		panic(err.Error())
	}
	defer file.Close()

	encoder := json.NewEncoder(file)

	// Export entire queue.
	for _, event := range wl.queue {
		encoder.Encode(event)
	}

	wl.logger.Info("[INDEXER][wasm/state] Exported events", "blockHeight", wl.blockHeight, "codeId", wl.codeID, "contractAddress", wl.contractAddress, "count", len(wl.queue), "output", wl.output)
}

// TXs

type IndexerTxEvent struct {
	Type            string                `json:"type"`
	BlockHeight     int64                 `json:"blockHeight"`
	BlockTimeUnixMs int64                 `json:"blockTimeUnixMs"`
	ContractAddress string                `json:"contractAddress"`
	CodeId          uint64                `json:"codeId"`
	Action          string                `json:"action"`
	Sender          string                `json:"sender"`
	Msg             string                `json:"msg"`
	Reply           *wasmvmtypes.Reply    `json:"reply"`
	Funds           sdktypes.Coins        `json:"funds"`
	Response        *wasmvmtypes.Response `json:"response"`
	GasUsed         uint64                `json:"gasUsed"`
}

type IndexerTxWriter struct {
	output string
	file   *os.File
}

func NewIndexerTxWriter(config IndexerConfig) *IndexerTxWriter {
	// Open output file, creating if doesn't exist.
	file, err := os.OpenFile(config.Output, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(fmt.Errorf("[INDEXER][wasm/tx] Failed to open output file. output=%v, error=%w", config.Output, err))
	}

	return &IndexerTxWriter{
		output: config.Output,
		file:   file,
	}
}

// Write slash event to file.
func (iw *IndexerTxWriter) WriteTx(ctx *sdktypes.Context, contractAddress sdktypes.AccAddress, codeID uint64, action string, sender string, msg *[]byte, reply *wasmvmtypes.Reply, funds sdktypes.Coins, response *wasmvmtypes.Response, gasUsed uint64) {
	// If checking TX (simulating, not actually executing), do not index.
	if ctx.IsCheckTx() {
		return
	}

	encoder := json.NewEncoder(iw.file)

	// Export event.
	var msgBase64 string
	if msg != nil {
		msgBase64 = base64.StdEncoding.EncodeToString(*msg)
	}
	event := IndexerTxEvent{
		Type:            "tx",
		BlockHeight:     ctx.BlockHeight(),
		BlockTimeUnixMs: ctx.BlockTime().UnixMilli(),
		ContractAddress: contractAddress.String(),
		CodeId:          codeID,
		Action:          action,
		Sender:          sender,
		Msg:             msgBase64,
		Reply:           reply,
		Funds:           funds,
		Response:        response,
		GasUsed:         gasUsed,
	}
	encoder.Encode(event)

	ctx.Logger().Info("[INDEXER][wasm/tx] Exported event", "blockHeight", event.BlockHeight, "contractAddress", event.ContractAddress, "sender", event.Sender, "output", iw.output)
}

// Close file.
func (iw *IndexerTxWriter) Close() {
	iw.file.Close()
}
