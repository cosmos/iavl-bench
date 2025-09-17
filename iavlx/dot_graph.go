package iavlx

import (
	"fmt"
	"io"
)

func RenderDotGraph(writer io.Writer, tree *Tree) error {
	root, err := tree.root.Get(tree.store)
	if err != nil {
		return fmt.Errorf("failed to load root node: %w", err)
	}

	_, err = fmt.Fprintln(writer, "graph G {")
	if err != nil {
		return err
	}
	finishGraph := func() error {
		_, err := fmt.Fprintln(writer, "}")
		return err
	}
	if root == nil {
		return finishGraph()
	}

	nodeIdx := uint64(1)

	var traverse func(node *Node, parent string, direction string) error
	traverse = func(node *Node, parent string, direction string) error {
		var label string
		// TODO render node keys
		if node.isLeaf() {
			label = fmt.Sprintf("K:0x%X V:0x%X\n", node.key, node.value)
		} else {
			label = fmt.Sprintf("K:0x%x H:%d S:%d\n", node.key, node.subtreeHeight, node.size)
		}

		nodeId := fmt.Sprintf("n%d", nodeIdx)
		nodeIdx++

		_, err := fmt.Fprintf(writer, "%s [label=\"%s\"];\n", nodeId, label)
		if err != nil {
			return err
		}
		if parent != "" {
			_, err = fmt.Fprintf(writer, "%s -> %s [label=\"%s\"];\n", parent, nodeId, direction)
		}
		if node.isLeaf() {
			return nil
		}

		leftNode, err := node.left.Get(tree.store)
		if err != nil {
			return err
		}

		rightNode, err := node.right.Get(tree.store)
		if err != nil {
			return err
		}

		if leftNode != nil {
			err = traverse(leftNode, nodeId, "l")
			if err != nil {
				return err
			}
		}
		if rightNode != nil {
			err = traverse(rightNode, nodeId, "r")
			if err != nil {
				return err
			}
		}
		return nil
	}

	err = traverse(root, "", "")
	if err != nil {
		return err
	}

	return finishGraph()
}
