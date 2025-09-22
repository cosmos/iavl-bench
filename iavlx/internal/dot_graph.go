package internal

import (
	"fmt"
	"io"
)

func RenderDotGraph(writer io.Writer, nodePtr *NodePointer) error {
	_, err := fmt.Fprintln(writer, "digraph G {")
	if err != nil {
		return err
	}
	finishGraph := func() error {
		_, err := fmt.Fprintln(writer, "}")
		return err
	}
	if nodePtr == nil {
		return finishGraph()
	}

	root, err := nodePtr.Resolve()
	if err != nil {
		return fmt.Errorf("failed to load root node: %w", err)
	}

	nodeIdx := uint64(1)

	var traverse func(node Node, parent string, direction string) error
	traverse = func(node Node, parent string, direction string) error {
		var label string
		key, err := node.Key()
		if err != nil {
			return err
		}

		if node.IsLeaf() {
			value, err := node.Value()
			if err != nil {
				return err
			}

			label = fmt.Sprintf("K:0x%X V:0x%X\n", key, value)
		} else {
			label = fmt.Sprintf("K:0x%x H:%d S:%d\n", key, node.Height(), node.Size())
		}

		nodeId := fmt.Sprintf("n%d", nodeIdx)
		nodeIdx++

		_, err = fmt.Fprintf(writer, "%s [label=\"%s\"];\n", nodeId, label)
		if err != nil {
			return err
		}
		if parent != "" {
			_, err = fmt.Fprintf(writer, "%s -> %s [label=\"%s\"];\n", parent, nodeId, direction)
		}
		if node.IsLeaf() {
			return nil
		}

		leftNode, err := node.Left().Resolve()
		if err != nil {
			return err
		}

		rightNode, err := node.Right().Resolve()
		if err != nil {
			return err
		}

		err = traverse(leftNode, nodeId, "l")
		if err != nil {
			return err
		}
		err = traverse(rightNode, nodeId, "r")
		if err != nil {
			return err
		}
		return nil
	}

	err = traverse(root, "", "")
	if err != nil {
		return err
	}

	return finishGraph()
}
