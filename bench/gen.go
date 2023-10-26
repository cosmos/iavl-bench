package bench

import (
	"fmt"
	"math/rand"

	"github.com/cosmos/iavl-bench/bench/metrics"
	api "github.com/kocubinski/costor-api"
)

type ChangesetIterator interface {
	Next() error
	Valid() bool
	Nodes() api.NodeIterator
	Version() int64
}

type ChangesetGenerator struct {
	StoreKey         string
	Seed             int64
	KeyMean          int
	KeyStdDev        int
	ValueMean        int
	ValueStdDev      int
	InitialSize      int
	FinalSize        int
	Versions         int64
	ChangePerVersion int
	DeleteFraction   float64
}

const byteChunkSize = 512
const byteChunkCount = 500_000

func (c ChangesetGenerator) Iterator() (ChangesetIterator, error) {
	if c.FinalSize < c.InitialSize {
		return nil, fmt.Errorf("final size must be greater than initial size")
	}

	itr := &ChangesetItr{
		gen:               c,
		rand:              rand.New(rand.NewSource(c.Seed)),
		createsPerVersion: float64(c.FinalSize-c.InitialSize) / float64(c.Versions-1),
		keys:              make([][]byte, c.FinalSize),
		freeList:          make(chan int, c.FinalSize),
		deleteMiss:        metrics.Default.NewCounter("iterator.delete_miss"),
		updateMiss:        metrics.Default.NewCounter("iterator.update_miss"),
		byteChunks:        make([][]byte, byteChunkCount),
	}

	for i := 0; i < c.FinalSize; i++ {
		itr.freeList <- i
	}

	for i := 0; i < len(itr.byteChunks); i++ {
		itr.rand.Read(itr.byteChunks[i][:])
	}

	itr.NodeItr = &generatingNodeItr{changesetItr: itr}

	err := itr.Next()
	return itr, err
}

type generatingNodeItr struct {
	// version 2 API -- do not store entire changeset in memory.
	ops []changesetOp
	// keys created this version, neither deleted nor updated. therefore defer adding to keys until next version.
	createdKeys []*deferredKey

	changesetItr *ChangesetItr

	node *api.Node
}

func (itr *generatingNodeItr) Next() error {
	if len(itr.ops) == 0 {
		itr.node = nil
		return nil
	}

	op := itr.ops[0]
	n := op.genNode(itr.changesetItr)
	if op.op == 1 {
		itr.createdKeys = append(itr.createdKeys, &deferredKey{
			key: n.Key,
			idx: int(n.LastVersion),
		})
		n.LastVersion = 0
	}
	itr.node = n

	itr.ops = itr.ops[1:]
	return nil
}

func (itr *generatingNodeItr) Valid() bool {
	return itr.node != nil
}

func (itr *generatingNodeItr) GetNode() *api.Node {
	return itr.node
}

type deferredKey struct {
	key []byte
	idx int
}

type ChangesetItr struct {
	NodeItr *generatingNodeItr

	version           int64
	rand              *rand.Rand
	gen               ChangesetGenerator
	keys              [][]byte
	freeList          chan int
	createsPerVersion float64
	createAccumulator float64
	deleteMiss        *metrics.Counter
	updateMiss        *metrics.Counter
	byteChunks        [][]byte
}

type changesetOp struct {
	op int
}

func (o *changesetOp) genNode(itr *ChangesetItr) *api.Node {
	switch o.op {
	case -1:
		// delete
		for {
			i := itr.rand.Intn(itr.gen.FinalSize)
			if itr.keys[i] != nil {
				// return the frame to the free list for create to use
				itr.freeList <- i
				// remove the key from state so subsequent updates/deletes will not find it
				k := itr.keys[i]
				itr.keys[i] = nil
				return &api.Node{
					StoreKey: itr.gen.StoreKey,
					Block:    itr.version,
					Key:      k,
					Delete:   true,
				}
			}
		}
	case 0:
		// update
		for {
			i := itr.rand.Intn(itr.gen.FinalSize)
			if itr.keys[i] != nil {
				return &api.Node{
					StoreKey: itr.gen.StoreKey,
					Block:    itr.version,
					Key:      itr.keys[i],
					Value:    itr.genBytes(itr.gen.ValueMean, itr.gen.ValueStdDev),
				}
			}
		}
	case 1:
		// create
		node := &api.Node{
			StoreKey: itr.gen.StoreKey,
			Key:      itr.genBytes(itr.gen.KeyMean, itr.gen.KeyStdDev),
			Value:    itr.genBytes(itr.gen.ValueMean, itr.gen.ValueStdDev),
			Block:    itr.version,
		}
		i := <-itr.freeList
		// hack re-use of field
		node.LastVersion = int64(i)
		return node
	default:
		panic(fmt.Sprintf("invalid op %d", o.op))
	}
}

func (itr *ChangesetItr) nextVersionV2() (*generatingNodeItr, error) {
	itr.version++

	deletes := int(itr.gen.DeleteFraction * float64(itr.gen.ChangePerVersion))
	updates := itr.gen.ChangePerVersion - deletes
	var creates int
	if itr.version == 1 {
		creates = itr.gen.InitialSize
	} else {
		itr.createAccumulator += itr.createsPerVersion
		clamped := int(itr.createAccumulator)
		creates = clamped + deletes
		itr.createAccumulator -= float64(clamped)
	}

	nodeItr := &generatingNodeItr{changesetItr: itr}

	// only delete past version 1
	if itr.version > 1 {
		for i := 0; i < deletes; i++ {
			nodeItr.ops = append(nodeItr.ops, changesetOp{op: -1})
		}
	}

	// only update past version 1
	if itr.version > 1 {
		for i := 0; i < updates; i++ {
			nodeItr.ops = append(nodeItr.ops, changesetOp{op: 0})
		}
	}

	for i := 0; i < creates; i++ {
		nodeItr.ops = append(nodeItr.ops, changesetOp{op: 1})
	}

	itr.rand.Shuffle(len(nodeItr.ops), func(i, j int) {
		nodeItr.ops[i], nodeItr.ops[j] = nodeItr.ops[j], nodeItr.ops[i]
	})

	err := nodeItr.Next()
	if err != nil {
		return nil, err
	}
	return nodeItr, nil
}

func (itr *ChangesetItr) Next() error {
	if itr.version == itr.gen.Versions {
		itr.NodeItr = nil
		return nil
	}

	// save created keys from previous version
	if itr.NodeItr != nil {
		for _, dk := range itr.NodeItr.createdKeys {
			itr.keys[dk.idx] = dk.key
		}
	}

	var err error
	itr.NodeItr, err = itr.nextVersionV2()
	if err != nil {
		return err
	}
	return nil
}

func (itr *ChangesetItr) Valid() bool {
	return itr.NodeItr != nil
}

func (itr *ChangesetItr) Nodes() api.NodeIterator {
	return itr.NodeItr
}

func (itr *ChangesetItr) Version() int64 { return itr.version }

func (itr *ChangesetItr) genBytes(mean, stdDev int) []byte {
	length := int(itr.rand.NormFloat64()*float64(stdDev) + float64(mean))
	// length must be at least 1
	// explanation: normal distribution is a poor approximation of certain data sets where std dev is skewed
	// by outliers on the upper bound.  mean - std dev can be negative, which is not a valid length.
	// we could just clamp length at 1, but that would skew the distribution of lengths towards 0 which is
	// not realistic.  instead we just generate again closer to the mean with a std dev of mean / 3.
	// this is not perfect but good enough for test sets.
	if length < 1 {
		length = int(itr.rand.NormFloat64()*float64(mean/3) + float64(mean))
		// much lower probability of this happening twice, but just in case
		if length < 1 {
			length = 1
		}
	}
	b := make([]byte, length)
	rem := length % byteChunkSize
	for i := 0; i < length-rem; i += byteChunkSize {
		copy(b[i:], itr.byteChunks[itr.rand.Intn(byteChunkCount)])
	}
	// fill remainder (entire buffer if length < byteChunkSize)
	itr.rand.Read(b[length-rem:])
	return b
}

type multiNodeIterator struct {
	iterators []api.NodeIterator
	idx       int
	node      *api.Node
	buf       []*api.Node
}

// CONTRACT: all iterators are initialized, no need to call Next()
func newMultiNodeIterator(itrs []api.NodeIterator) *multiNodeIterator {
	itr := &multiNodeIterator{
		iterators: itrs,
	}
	for i, it := range itrs {
		if i == 0 {
			itr.node = it.GetNode()
		} else {
			itr.buf = append(itr.buf, it.GetNode())
		}
	}
	return itr
}

func (itr *multiNodeIterator) Next() error {
	if len(itr.iterators) == 0 {
		itr.node = nil
		return nil
	}

	if len(itr.buf) != 0 {
		itr.node = itr.buf[0]
		itr.buf = itr.buf[1:]
		return nil
	}

	i := itr.idx
	if i < 0 {
		panic("invalid iterator index")
	}
	err := itr.iterators[i].Next()
	if err != nil {
		return err
	}
	if !itr.iterators[i].Valid() {
		itr.iterators = append(itr.iterators[:i], itr.iterators[i+1:]...)
		itr.idx = 0
		return itr.Next()
	}

	itr.node = itr.iterators[i].GetNode()

	if itr.idx == len(itr.iterators)-1 {
		itr.idx = 0
	} else {
		itr.idx++
	}

	return nil
}

func (itr *multiNodeIterator) Valid() bool {
	return itr.node != nil
}

func (itr *multiNodeIterator) GetNode() *api.Node {
	return itr.node
}

type ChangesetIterators struct {
	NodeItr *multiNodeIterator

	version   int64
	iterators []ChangesetIterator
	idx       int
}

func NewChangesetIterators(gens []ChangesetGenerator) (*ChangesetIterators, error) {
	if len(gens) == 0 {
		return nil, fmt.Errorf("must provide at least one generator")
	}
	itr := &ChangesetIterators{}
	var nodeItrs []api.NodeIterator
	versions := gens[0].Versions
	for _, gen := range gens {
		if gen.Versions != versions {
			return nil, fmt.Errorf("all generators must have the same number of versions")
		}
		i, err := gen.Iterator()
		if err != nil {
			return nil, err
		}
		itr.iterators = append(itr.iterators, i)
		nodeItrs = append(nodeItrs, i.Nodes())
	}

	itr.version = itr.iterators[0].Version()
	itr.NodeItr = newMultiNodeIterator(nodeItrs)
	//if err := itr.NodeItr.Next(); err != nil {
	//	return nil, err
	//}

	return itr, nil
}

func (itr *ChangesetIterators) Next() error {
	var nodes []api.NodeIterator
	for _, i := range itr.iterators {
		err := i.Next()
		if err != nil {
			return err
		}
		if !i.Valid() {
			continue
		}
		nodes = append(nodes, i.Nodes())
	}
	if len(nodes) == 0 {
		itr.NodeItr = nil
		return nil
	}

	itr.NodeItr = newMultiNodeIterator(nodes)
	itr.version = itr.iterators[0].Version()
	return nil
}

func (itr *ChangesetIterators) Valid() bool {
	return itr.NodeItr != nil
}

func (itr *ChangesetIterators) Nodes() api.NodeIterator {
	return itr.NodeItr
}

func (itr *ChangesetIterators) Version() int64 { return itr.version }

func (itr *ChangesetIterators) StoreKeys() []string {
	var keys []string
	for _, i := range itr.iterators {
		keys = append(keys, i.Nodes().GetNode().StoreKey)
	}
	return keys
}
