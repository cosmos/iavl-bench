package iavlx

import (
	"fmt"

	"github.com/emicklei/dot"
)

func RenderDotGraph(store NodeReader, root *Node) (string, error) {
	graph := dot.NewGraph(dot.Directed)

	if root == nil {
		return graph.String(), nil
	}

	var traverse func(node *Node, parent *dot.Node, direction string) error
	traverse = func(node *Node, parent *dot.Node, direction string) error {
		var label string
		// TODO render node keys
		if node.isLeaf() {
			label = fmt.Sprintf("K:0x%X V:0x%X\n", node.key, node.value)
		} else {
			label = fmt.Sprintf("K:0x%x H:%d S:%d\n", node.key, node.subtreeHeight, node.size)
		}

		n := graph.Node(label)
		if parent != nil {
			parent.Edge(n, direction)
		}
		if node.isLeaf() {
			return nil
		}

		leftNode, err := node.left.Get(store)
		if err != nil {
			return err
		}

		rightNode, err := node.right.Get(store)
		if err != nil {
			return err
		}

		if leftNode != nil {
			err = traverse(leftNode, &n, "l")
			if err != nil {
				return err
			}
		}
		if rightNode != nil {
			err = traverse(rightNode, &n, "r")
			if err != nil {
				return err
			}
		}
		return nil
	}

	err := traverse(root, nil, "")
	if err != nil {
		return "", err
	}

	return graph.String(), nil
}
