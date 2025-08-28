package bench

//type Tree interface {
//	Set(key, value []byte) (bool, error)
//	Get(key []byte) ([]byte, error)
//	Remove(key []byte) ([]byte, bool, error)
//	SaveVersion() ([]byte, int64, error)
//	Size() int64
//	Height() int8
//	Hash() ([]byte, error)
//}
//
//type MultiTree interface {
//	GetTree(key string) (Tree, error)
//	SaveVersions() ([]byte, int64, error)
//	V2Hash() []byte
//}
//
//type NaiveMultiTree struct {
//	Trees map[string]Tree
//}
//
//func (nmt *NaiveMultiTree) GetTree(key string) (Tree, error) {
//	tree, ok := nmt.Trees[key]
//	if !ok {
//		return nil, fmt.Errorf("tree with key %s not found", key)
//	}
//	return tree, nil
//}
//
//func (nmt *NaiveMultiTree) SaveVersions() ([]byte, int64, error) {
//	var hashes []byte
//	version := int64(-1)
//	for _, tree := range nmt.Trees {
//		hash, v, err := tree.SaveVersion()
//		if err != nil {
//			return nil, 0, err
//		}
//		if version != -1 && version != v {
//			return nil, 0, fmt.Errorf("unexpected; trees are at different versions: %d != %d", version, v)
//		}
//		version = v
//		hashes = append(hashes, hash...)
//	}
//	h := sha256.Sum256(hashes)
//	return h[:], version, nil
//}
//
//func NewMultiTree() *NaiveMultiTree {
//	return &NaiveMultiTree{
//		Trees: make(map[string]Tree),
//	}
//}
//
//func (nmt *NaiveMultiTree) V2Hash() []byte {
//	var (
//		storeKeys []string
//		hashes    []byte
//	)
//	for k := range nmt.Trees {
//		storeKeys = append(storeKeys, k)
//	}
//
//	sort.Strings(storeKeys)
//	for _, k := range storeKeys {
//		tree := nmt.Trees[k]
//		h, err := tree.Hash()
//		if err != nil {
//			panic(err)
//		}
//		hashes = append(hashes, h...)
//	}
//	hash := sha256.Sum256(hashes)
//	return hash[:]
//}
