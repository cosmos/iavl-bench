package internal

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBasicTest(t *testing.T) {
	dir, err := os.MkdirTemp("", "iavlx")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	commitTree, err := NewCommitTree(dir, false)
	require.NoError(t, err)
	tree := commitTree.Branch()
	require.NoError(t, tree.Set([]byte{0}, []byte{1}))
	// renderTree(t, tree)

	val, err := tree.Get([]byte{0})
	require.NoError(t, err)
	require.Equal(t, []byte{1}, val)

	require.NoError(t, tree.Set([]byte{1}, []byte{2}))
	//renderTree(t, tree)

	val, err = tree.Get([]byte{0})
	require.NoError(t, err)
	require.Equal(t, []byte{1}, val)
	val, err = tree.Get([]byte{1})
	require.NoError(t, err)
	require.Equal(t, []byte{2}, val)

	require.NoError(t, tree.Set([]byte{2}, []byte{3}))
	//renderTree(t, tree)

	val, err = tree.Get([]byte{0})
	require.NoError(t, err)
	require.Equal(t, []byte{1}, val)
	val, err = tree.Get([]byte{1})
	require.NoError(t, err)
	require.Equal(t, []byte{2}, val)
	val, err = tree.Get([]byte{2})
	require.NoError(t, err)
	require.Equal(t, []byte{3}, val)

	val, err = tree.Get([]byte{3})
	require.NoError(t, err)
	require.Nil(t, val)

	require.NoError(t, tree.Remove([]byte{1}))
	//renderTree(t, tree)

	val, err = tree.Get([]byte{1})
	require.NoError(t, err)
	require.Nil(t, val)

	require.NoError(t, commitTree.Apply(tree))
	hash, err := commitTree.Commit()
	require.NoError(t, err)
	require.NotNil(t, hash)
	t.Logf("committed with root hash: %X", hash)
	require.NoError(t, commitTree.Close())
}

func renderTree(t *testing.T, tree *Tree) {
	graph := &bytes.Buffer{}
	require.NoError(t, RenderDotGraph(graph, tree.root))
	t.Logf("tree graph:\n%s", graph.String())
}
