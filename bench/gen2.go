package bench

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"

	"math/rand/v2"

	"cosmossdk.io/api/cosmos/store/v1beta1"
	"github.com/tidwall/btree"
	"google.golang.org/protobuf/encoding/protodelim"
)

type MultiChangesetGenerator struct {
	RandSource rand.Source
	Versions   int64
	Generators []ChangesetGenerator
}

func GenerateChangeSets(g MultiChangesetGenerator, outDir string) error {
	// ensure directory exists
	err := os.MkdirAll(outDir, 0o755)
	if err != nil {
		return err
	}

	multiStoreState := map[string]*storeState{}
	for _, gen := range g.Generators {
		multiStoreState[gen.StoreKey] = newStoreState(gen)
	}
	version := int64(1)
	rng := rand.New(g.RandSource)
	for ; version <= g.Versions; version++ {
		fmt.Printf("Generating changeset for version %d\n", version)

		// generate plans for each store
		plans := map[string]changesetPlan{}
		for storeKey, state := range multiStoreState {
			plans[storeKey] = state.genChangesetPlan(version)
		}

		// generate todo list
		todo := newChangesetTodo(plans)
		filename := fmt.Sprintf("%s/%06d.delimpb", outDir, version)
		outWriter, err := os.Create(filename)
		if err != nil {
			return fmt.Errorf("error creating changeset file for version %d: %w", version, err)
		}

		err = todo.apply(outWriter, rng, multiStoreState)
		if err != nil {
			return fmt.Errorf("error generating changeset for version %d: %w", version, err)
		}

		err = outWriter.Close()
		if err != nil {
			return fmt.Errorf("error closing changeset file for version %d: %w", version, err)
		}

		fmt.Printf("Wrote changeset for version %d to %s\n", version, filename)
	}

	return nil
}

type storeState struct {
	gen               ChangesetGenerator
	existingKeys      *btree.BTreeG[[]byte]
	createsPerVersion float64
	createAccumulator float64
}

func newStoreState(c ChangesetGenerator) *storeState {
	return &storeState{
		gen: c,
		existingKeys: btree.NewBTreeG(func(a, b []byte) bool {
			return bytes.Compare(a, b) < 0
		}),
		createsPerVersion: float64(c.FinalSize-c.InitialSize) / float64(c.Versions-1),
	}
}

type opType int

const (
	opCreate opType = iota
	opUpdate
	opDelete
)

func genOp(w io.Writer, st *storeState, op opType, rng *rand.Rand) error {
	var err error
	switch op {
	case opCreate:
		return st.genCreate(w, rng)
	case opUpdate:
		err = st.genUpdate(w, rng)
		return err
	case opDelete:
		err = st.genDelete(w, rng)
	default:
		return fmt.Errorf("unknown operation type: %d", op)
	}

	if errors.Is(err, NoKeys) {
		// no keys to update, create instead
		return st.genCreate(w, rng)
	}

	return nil
}

type changesetTodo struct {
	// leftTodo is a map from store key to a map of operation type to count of operations remaining
	leftTodo *btree.Map[string, *btree.Map[opType, int]]
}

func newChangesetTodo(plans map[string]changesetPlan) *changesetTodo {
	leftTodo := &btree.Map[string, *btree.Map[opType, int]]{}
	for storeKey, plan := range plans {
		fmt.Printf("  plan for store %s:\n", storeKey)
		opMap := &btree.Map[opType, int]{}
		if plan.creates > 0 {
			fmt.Printf("		creates: %d\n", plan.creates)
			opMap.Set(opCreate, plan.creates)
		}
		if plan.updates > 0 {
			fmt.Printf("		updates: %d\n", plan.updates)
			opMap.Set(opUpdate, plan.updates)
		}
		if plan.deletes > 0 {
			fmt.Printf("		deletes: %d\n", plan.deletes)
			opMap.Set(opDelete, plan.deletes)
		}
		leftTodo.Set(storeKey, opMap)
	}
	return &changesetTodo{
		leftTodo: leftTodo,
	}
}

func (todo *changesetTodo) apply(w io.Writer, rng *rand.Rand, storeStates map[string]*storeState) error {
	i := 0
	for todo.leftTodo.Len() > 0 {
		if i%10000 == 0 && i > 0 {
			fmt.Printf("  created %d operations\n", i)
		}
		storeKey, op, err := todo.selectOperation(rng)
		if err != nil {
			return err
		}
		st, ok := storeStates[storeKey]
		if !ok {
			return fmt.Errorf("logic error: store state for %s not found", storeKey)
		}
		err = genOp(w, st, op, rng)
		if err != nil {
			return fmt.Errorf("error generating operation for store %s: %w", storeKey, err)
		}
		i++
	}
	fmt.Printf("  created %d operations\n", i)
	return nil
}

func (todo *changesetTodo) selectOperation(rng *rand.Rand) (string, opType, error) {
	storeIdx := rng.IntN(todo.leftTodo.Len())
	selectedStore, opMap, ok := todo.leftTodo.GetAt(storeIdx)
	if !ok {
		return "", 0, fmt.Errorf("logic error: no store to select")
	}
	opIdx := rng.IntN(opMap.Len())
	op, count, ok := opMap.GetAt(opIdx)
	if !ok {
		return "", 0, fmt.Errorf("logic error: no operation to select")
	}
	if count <= 0 {
		return "", 0, fmt.Errorf("logic error: operation count is zero")
	}
	// decrement count
	if count == 1 {
		opMap.Delete(op)
		if opMap.Len() == 0 {
			todo.leftTodo.Delete(selectedStore)
		}
	} else {
		opMap.Set(op, count-1)
	}
	return selectedStore, op, nil
}

type changesetPlan struct {
	deletes int
	updates int
	creates int
}

func (c *storeState) genChangesetPlan(version int64) changesetPlan {
	if version == 1 {
		return changesetPlan{
			deletes: 0,
			updates: 0,
			creates: c.gen.InitialSize,
		}
	}

	deletes := int(c.gen.DeleteFraction * float64(c.gen.ChangePerVersion))
	updates := c.gen.ChangePerVersion - deletes
	var creates int
	c.createAccumulator += c.createsPerVersion
	clamped := int(c.createAccumulator)
	creates = clamped + deletes
	c.createAccumulator -= float64(clamped)

	return changesetPlan{
		deletes: deletes,
		updates: updates,
		creates: creates,
	}
}

var NoKeys = errors.New("no keys")

func (c *storeState) genCreate(w io.Writer, rng *rand.Rand) error {
	key := c.genKey(rng)
	for c.has(key) {
		key = c.genKey(rng)
	}
	c.existingKeys.Set(key)
	return c.writeKVStorePair(w, key, c.genValue(rng), false)
}

func (c *storeState) genUpdate(w io.Writer, rng *rand.Rand) error {
	n := c.existingKeys.Len()
	if n == 0 {
		return NoKeys
	}
	idx := rng.IntN(n)
	key, ok := c.existingKeys.GetAt(idx)
	if !ok {
		return fmt.Errorf("logic error: no key to update")
	}
	return c.writeKVStorePair(w, key, c.genValue(rng), false)
}

func (c *storeState) genDelete(w io.Writer, rng *rand.Rand) error {
	n := c.existingKeys.Len()
	if n == 0 {
		return NoKeys
	}
	idx := rng.IntN(n)
	key, ok := c.existingKeys.GetAt(idx)
	if !ok {
		return fmt.Errorf("logic error: no key to delete")
	}
	c.existingKeys.Delete(key)
	return c.writeKVStorePair(w, key, nil, true)
}

func (c *storeState) writeKVStorePair(w io.Writer, key, value []byte, delete bool) error {
	_, err := protodelim.MarshalTo(w, &storev1beta1.StoreKVPair{
		StoreKey: c.gen.StoreKey,
		Key:      key,
		Value:    value,
		Delete:   delete,
	})
	return err
}

func (c *storeState) genKey(rng *rand.Rand) []byte {
	return genBytes(rng, c.gen.KeyMean, c.gen.KeyStdDev)
}

func (c *storeState) genValue(rng *rand.Rand) []byte {
	return genBytes(rng, c.gen.ValueMean, c.gen.ValueStdDev)
}

func (c *storeState) has(key []byte) bool {
	_, ok := c.existingKeys.Get(key)
	return ok
}

func genBytes(rng *rand.Rand, mean, stdDev int) []byte {
	length := int(rng.NormFloat64()*float64(stdDev) + float64(mean))
	// length must be at least 1
	// explanation: normal distribution is a poor approximation of certain data sets where std dev is skewed
	// by outliers on the upper bound.  mean - std dev can be negative, which is not a valid length.
	// we could just clamp length at 1, but that would skew the distribution of lengths towards 0 which is
	// not realistic.  instead we just generate again closer to the mean with a std dev of mean / 3.
	// this is not perfect but good enough for test sets.
	if length < 1 {
		length = int(rng.NormFloat64()*float64(mean/3) + float64(mean))
		// much lower probability of this happening twice, but just in case
		if length < 1 {
			length = 1
		}
	}
	b := make([]byte, length)
	for i := 0; i < length; i++ {
		b[i] = byte(rng.IntN(256))
	}
	return b
}
