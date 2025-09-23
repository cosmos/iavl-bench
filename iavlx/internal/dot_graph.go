package internal

import (
	"fmt"
	"io"
)

func DebugTraverse(nodePtr *NodePointer, onNode func(node Node, selfId, parent NodeID, direction string) error) error {
	if nodePtr == nil {
		return nil
	}

	var traverse func(np *NodePointer, parent NodeID, direction string) error
	traverse = func(np *NodePointer, parent NodeID, direction string) error {
		node, err := np.Resolve()
		if err != nil {
			return err
		}

		nodeId := np.id
		if err := onNode(node, nodeId, parent, direction); err != nil {
			return err
		}

		if node.IsLeaf() {
			return nil
		}

		err = traverse(node.Left(), nodeId, "l")
		if err != nil {
			return err
		}
		err = traverse(node.Right(), nodeId, "r")
		if err != nil {
			return err
		}
		return nil
	}

	return traverse(nodePtr, 0, "")
}

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

	err = DebugTraverse(nodePtr, func(node Node, nodeId, parent NodeID, direction string) error {
		key, err := node.Key()
		if err != nil {
			return err
		}

		version := node.Version()
		idx := nodeId.Index()

		label := fmt.Sprintf("ver: %d idx %d key:0x%x ", version, idx, key)
		if node.IsLeaf() {
			value, err := node.Value()
			if err != nil {
				return err
			}

			label += fmt.Sprintf("val:0x%X", value)
		} else {
			label += fmt.Sprintf("ht:%d sz:%d", node.Height(), node.Size())
		}

		nodeName := fmt.Sprintf("n%d", nodeId)

		_, err = fmt.Fprintf(writer, "%s [label=\"%s\"];\n", nodeName, label)
		if err != nil {
			return err
		}
		if parent != 0 {
			parentName := fmt.Sprintf("n%d", parent)
			_, err = fmt.Fprintf(writer, "%s -> %s [label=\"%s\"];\n", parentName, nodeName, direction)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	return finishGraph()
}
