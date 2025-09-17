package iavlx

import "encoding/binary"

// NodeKey uniquely represents nodes.
type NodeKey [12]byte

type NodeKeyGenerator interface {
	AssignNodeKey(*Node)
	SetVersion(uint32)
	Version() uint32
}

type VersionSeqNodeKeyGen struct {
	version   uint32
	leafSeq   uint32
	branchSeq uint32
}

func (v *VersionSeqNodeKeyGen) SetVersion(x uint32) {
	v.version = x
}

func (v *VersionSeqNodeKeyGen) Version() uint32 {
	return v.version
}

func (v *VersionSeqNodeKeyGen) AssignNodeKey(node *Node) {
	var seq uint32
	if node.isLeaf() {
		v.leafSeq++
		seq = v.leafSeq
	} else {
		v.branchSeq++
		seq = v.branchSeq
	}
	binary.BigEndian.PutUint32(node.nodeKey[0:4], v.version)
	binary.BigEndian.PutUint32(node.nodeKey[4:8], seq)
	// last 4 bytes are zero (for now)
}

var _ NodeKeyGenerator = (*VersionSeqNodeKeyGen)(nil)

type KeyVersionSeqNodeKeyGen struct {
	version  uint32
	sequence uint32
}
