package iavlx

import "encoding/binary"

// NodeKey uniquely represents nodes.
type NodeKey [12]byte

var EmptyNodeKey NodeKey = NodeKey{}

type NodeKeyGenerator interface {
	AssignNodeKey(*Node)
	AssignDeleteLeafKey(*Node) NodeKey
	SetNodeKeyVersion(uint32)
	Version() uint32
}

type VersionSeqNodeKeyGen struct {
	version   uint32
	leafSeq   uint32
	branchSeq uint32
}

func NewVersionSeqNodeKeyGen() *VersionSeqNodeKeyGen {
	res := &VersionSeqNodeKeyGen{}
	res.SetNodeKeyVersion(1)
	return res
}

func (v *VersionSeqNodeKeyGen) SetNodeKeyVersion(x uint32) {
	v.version = x
	v.branchSeq = 0
	// leafSeq has high bit set to distinguish from branch nodes
	v.leafSeq = 1 << 31
}

func (v *VersionSeqNodeKeyGen) Version() uint32 {
	return v.version
}

func (v *VersionSeqNodeKeyGen) AssignNodeKey(node *Node) {
	node.nodeKey = v.assignNodeKey(node)
}

func (v *VersionSeqNodeKeyGen) assignNodeKey(node *Node) NodeKey {
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
	return node.nodeKey
}

func (v *VersionSeqNodeKeyGen) AssignDeleteLeafKey(node *Node) NodeKey {
	return v.assignNodeKey(node)
}

var _ NodeKeyGenerator = (*VersionSeqNodeKeyGen)(nil)

type KeyVersionSeqNodeKeyGen struct {
	version  uint32
	sequence uint32
}
