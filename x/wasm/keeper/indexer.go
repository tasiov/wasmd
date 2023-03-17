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
	parentListener *IndexerStateWriteListener
	ctx            *sdktypes.Context
	output         string

	queue     map[string]PendingIndexerStateEvent
	committed bool

	// Contract info.
	contractAddress string
	codeID          uint64
}

func NewIndexerStateWriteListener(config IndexerConfig, parentListener *IndexerStateWriteListener, ctx *sdktypes.Context, contractAddress sdktypes.AccAddress, codeID uint64) *IndexerStateWriteListener {
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
		parentListener: parentListener,
		ctx:            ctx,
		output:         config.Output,

		queue:     make(map[string]PendingIndexerStateEvent),
		committed: false,

		// Contract info.
		contractAddress: contractAddress.String(),
		codeID:          codeID,
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
		BlockHeight:     wl.ctx.BlockHeight(),
		BlockTimeUnixMs: wl.ctx.BlockTime().UnixMilli(),
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
	if wl.parentListener != nil {
		for k, v := range wl.queue {
			wl.parentListener.queue[k] = v
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
	CurrentIndexerStateListener = wl.parentListener

	if !wl.committed || wl.parentListener != nil {
		return
	}

	// Open output file, creating if doesn't exist.
	file, err := os.OpenFile(wl.output, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		wl.ctx.Logger().Error("[INDEXER][wasm/state] Failed to open output file", "output", wl.output, "error", err)
		panic(err.Error())
	}
	defer file.Close()

	encoder := json.NewEncoder(file)

	// Export entire queue.
	for _, event := range wl.queue {
		encoder.Encode(event)
	}

	wl.ctx.Logger().Info("[INDEXER][wasm/state] Exported events", "blockHeight", wl.ctx.BlockHeight(), "codeId", wl.codeID, "contractAddress", wl.contractAddress, "count", len(wl.queue), "output", wl.output)
}

// TXs

type IndexerTxEvent struct {
	Type            string                `json:"type"`
	BlockHeight     int64                 `json:"blockHeight"`
	BlockTimeUnixMs int64                 `json:"blockTimeUnixMs"`
	TxIndex         uint32                `json:"txIndex"`
	MessageId       string                `json:"messageId"`
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
	parentWriter *IndexerTxWriter
	output       string
	file         *os.File

	ctx *sdktypes.Context
	env *wasmvmtypes.Env

	// The message index of the current message within its lineage of messages.
	// This will be appended to a string of all message indexes in the lineage
	// traversed via the parent writer to generate a unique message ID for every
	// message within a given transaction within a given block.
	messageIndex uint32
}

func NewIndexerTxWriter(config IndexerConfig, parentWriter *IndexerTxWriter, ctx *sdktypes.Context, env *wasmvmtypes.Env, messageIndex uint32) *IndexerTxWriter {
	// Open output file, creating if doesn't exist.
	file, err := os.OpenFile(config.Output, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(fmt.Errorf("[INDEXER][wasm/tx] Failed to open output file. output=%v, error=%w", config.Output, err))
	}

	return &IndexerTxWriter{
		parentWriter: parentWriter,
		output:       config.Output,
		file:         file,

		ctx: ctx,
		env: env,

		messageIndex: messageIndex,
	}
}

func (iw *IndexerTxWriter) getMessageId() string {
	currentMessageId := fmt.Sprintf("%d", iw.messageIndex)

	// If we have a parent writer, append our message index to its message ID.
	if iw.parentWriter != nil {
		return iw.parentWriter.getMessageId() + "." + currentMessageId
	}

	// Otherwise, we are the root writer, so just use our local message ID.
	return currentMessageId
}

func (iw *IndexerTxWriter) finish() {
	// Move the current indexer writer pointer up one to our parent. If we are
	// the root writer, this will be nil.
	CurrentIndexerTxWriter = iw.parentWriter

	// Increment the parent indexer's message index. If no parent, increment
	// the root TX-level message index.
	if iw.parentWriter != nil {
		iw.parentWriter.messageIndex++
	} else {
		CurrentIndexerTxRootMessageIndex++
	}

	// Close file.
	iw.file.Close()
}

// Write tx event to file.
func (iw *IndexerTxWriter) write(contractAddress sdktypes.AccAddress, codeID uint64, action string, sender string, msg *[]byte, reply *wasmvmtypes.Reply, funds sdktypes.Coins, response *wasmvmtypes.Response, gasUsed uint64) {
	// If checking TX (simulating, not actually executing), do not index.
	if iw.ctx.IsCheckTx() {
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
		BlockHeight:     iw.ctx.BlockHeight(),
		BlockTimeUnixMs: iw.ctx.BlockTime().UnixMilli(),
		TxIndex:         iw.env.Transaction.Index,
		MessageId:       iw.getMessageId(),
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

	iw.ctx.Logger().Info("[INDEXER][wasm/tx] Exported event", "blockHeight", event.BlockHeight, "txIndex", event.TxIndex, "messageId", event.MessageId, "contractAddress", event.ContractAddress, "sender", event.Sender, "output", iw.output)
}

// Close file.
func (iw *IndexerTxWriter) Close() {
	iw.file.Close()
}
