package types

import (
	"encoding/binary"

	sdk "github.com/cosmos/cosmos-sdk/types"
)

const (
	// ModuleName is the name of the contract module
	ModuleName = "wasm"

	// StoreKey is the string store representation
	StoreKey = ModuleName

	// TStoreKey is the string transient store representation
	TStoreKey = "transient_" + ModuleName

	// QuerierRoute is the querier route for the staking module
	QuerierRoute = ModuleName

	// RouterKey is the msg router key for the staking module
	RouterKey = ModuleName
)

const ( // event attributes
	AttributeKeyContract = "contract_address"
	AttributeKeyCodeID   = "code_id"
	AttributeKeySigner   = "signer"
)

// nolint
var (
	KeyLastCodeID     = []byte("lastCodeId")
	KeyLastInstanceID = []byte("lastContractId")

	CodeKeyPrefix       = []byte{0x01}
	ContractKeyPrefix   = []byte{0x02}
	ContractStorePrefix = []byte{0x03}
)

// GetCodeKey constructs the key for retreiving the ID for the WASM code
func GetCodeKey(codeID uint64) []byte {
	contractIDBz := sdk.Uint64ToBigEndian(codeID)
	return append(CodeKeyPrefix, contractIDBz...)
}

func decodeCodeKey(src []byte) uint64 {
	return binary.BigEndian.Uint64(src[len(CodeKeyPrefix):])
}

// GetContractAddressKey returns the key for the WASM contract instance
func GetContractAddressKey(addr sdk.AccAddress) []byte {
	return append(ContractKeyPrefix, addr...)
}

// GetContractStorePrefixKey returns the store prefix for the WASM contract instance
func GetContractStorePrefixKey(addr sdk.AccAddress) []byte {
	return append(ContractStorePrefix, addr...)
}
