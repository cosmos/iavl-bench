package iavlx

import (
	db "github.com/cosmos/cosmos-db"
)

type CosmosDBStore struct {
	db db.DB
}
