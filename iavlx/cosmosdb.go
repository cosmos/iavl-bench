package iavlx

import (
	"bytes"
	"encoding/binary"

	db "github.com/cosmos/cosmos-db"
)

type CosmosDBStore struct {
	*VersionSeqNodeKeyGen
	db   db.DB
	opts CosmosDBStoreOptions
}

type CosmosDBStoreOptions struct {
	Evict bool
}

func NewCosmosDBStore(db db.DB, opts CosmosDBStoreOptions) *CosmosDBStore {
	return &CosmosDBStore{
		db:                   db,
		VersionSeqNodeKeyGen: NewVersionSeqNodeKeyGen(),
		opts:                 opts,
	}
}

func (c CosmosDBStore) Load(pointer *NodePointer) (*Node, error) {
	bz := make([]byte, len(pointer.key)+1)
	bz[0] = 'n'
	copy(bz[1:], pointer.key[:])
	val, err := c.db.Get(bz)
	if err != nil {
		return nil, err
	}
	node, err := decodeNode(val)
	if err != nil {
		return nil, err
	}
	node.nodeKey = pointer.key
	return node, nil
}

func (c CosmosDBStore) SaveNode(node *Node) error {
	keyBz := make([]byte, len(node.nodeKey)+1)
	keyBz[0] = 'n'
	copy(keyBz[1:], node.nodeKey[:])
	valueBz, err := encodeNode(node, c.opts)
	if err != nil {
		return err
	}
	return c.db.Set(keyBz, valueBz)
}

func encodeNode(node *Node, opts CosmosDBStoreOptions) ([]byte, error) {
	buf := &bytes.Buffer{}

	// write key length as varint
	keyLen := len(node.key)
	varintBuf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(varintBuf, uint64(keyLen))
	buf.Write(varintBuf[:n])

	// write key
	buf.Write(node.key)

	// write subtree height as byte
	buf.WriteByte(byte(node.subtreeHeight))

	// write version
	versionBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(versionBuf, node.version)
	buf.Write(versionBuf)

	// write hash
	hashLen := len(node.hash)
	n = binary.PutUvarint(varintBuf, uint64(hashLen))
	buf.Write(varintBuf[:n])
	buf.Write(node.hash)

	if node.isLeaf() {
		// write value length as varint
		valueLen := len(node.value)
		n = binary.PutUvarint(varintBuf, uint64(valueLen))
		buf.Write(varintBuf[:n])

		// write value
		buf.Write(node.value)
	} else {
		// write size as varint
		n = binary.PutUvarint(varintBuf, uint64(node.size))
		buf.Write(varintBuf[:n])

		// write left child node key (12 bytes)
		if node.left.key.IsEmpty() {
			node.left.key = node.left.ptr.Load().nodeKey
		}
		buf.Write(node.left.key[:])

		if node.right.key.IsEmpty() {
			node.right.key = node.right.ptr.Load().nodeKey
		}
		// write right child node key (12 bytes)
		buf.Write(node.right.key[:])
	}

	return buf.Bytes(), nil
}

func decodeNode(bz []byte) (*Node, error) {
	buf := bytes.NewReader(bz)
	node := &Node{}

	// read key length as varint
	keyLen, err := binary.ReadUvarint(buf)
	if err != nil {
		return nil, err
	}

	// read key
	node.key = make([]byte, keyLen)
	if _, err := buf.Read(node.key); err != nil {
		return nil, err
	}

	// read subtree height as byte
	heightByte, err := buf.ReadByte()
	if err != nil {
		return nil, err
	}
	node.subtreeHeight = int8(heightByte)

	// read version
	versionBuf := make([]byte, 4)
	if _, err := buf.Read(versionBuf); err != nil {
		return nil, err
	}
	node.version = binary.BigEndian.Uint32(versionBuf)

	// read hash
	hashLen, err := binary.ReadUvarint(buf)
	if err != nil {
		return nil, err
	}
	node.hash = make([]byte, hashLen)
	if _, err := buf.Read(node.hash); err != nil {
		return nil, err
	}

	if node.isLeaf() {
		// read value length as varint
		valueLen, err := binary.ReadUvarint(buf)
		if err != nil {
			return nil, err
		}

		// read value
		node.value = make([]byte, valueLen)
		if _, err := buf.Read(node.value); err != nil {
			return nil, err
		}
	} else {
		// read size as varint
		size, err := binary.ReadUvarint(buf)
		if err != nil {
			return nil, err
		}
		node.size = int64(size)

		// read left child node key (12 bytes)
		leftKey := make([]byte, 12)
		if _, err := buf.Read(leftKey); err != nil {
			return nil, err
		}
		var leftNodeKey NodeKey
		copy(leftNodeKey[:], leftKey)
		node.left = &NodePointer{key: leftNodeKey}

		// read right child node key (12 bytes)
		rightKey := make([]byte, 12)
		if _, err := buf.Read(rightKey); err != nil {
			return nil, err
		}
		var rightNodeKey NodeKey
		copy(rightNodeKey[:], rightKey)
		node.right = &NodePointer{key: rightNodeKey}
	}

	return node, nil
}

func (c CosmosDBStore) DeleteNode(version int64, deleteKey NodeKey, node *Node) error {
	if node.isLeaf() {
		bz := make([]byte, len(deleteKey)+2)
		bz[0] = 'n'
		copy(bz[1:], deleteKey[:])
		bz[len(bz)-1] = 'd' // mark as deleted
		return c.db.Set(bz, node.nodeKey[:])
	} else {
		// TODO all children are orphans too
		bz := make([]byte, len(node.nodeKey)+5)
		bz[0] = 'o' // mark as orphan
		// write version
		binary.BigEndian.PutUint32(bz[1:5], uint32(version))
		// write node key
		copy(bz[5:], node.nodeKey[:])
		return c.db.Set(bz, []byte{})
	}
}

func (c CosmosDBStore) SaveRoot(version int64, root *Node) error {
	bz := make([]byte, 5)
	bz[0] = 'r' // mark as root
	// write version
	binary.BigEndian.PutUint32(bz[1:5], uint32(version))
	var valueBz []byte
	if root != nil {
		valueBz = root.nodeKey[:]
	} else {
		valueBz = []byte{}
	}
	err := c.db.Set(bz, valueBz)
	if err != nil {
		return err
	}
	// save latest version pointer
	return c.db.Set([]byte("latest"), bz)
}

var _ NodeWriter = &CosmosDBStore{}
