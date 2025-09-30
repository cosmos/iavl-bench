package x3

//func (cs *Changeset) compactBranches(retainCriteria RetainCriteria, newBranches *StructReader[BranchLayout]) error {
//	if !cs.sealed {
//		return fmt.Errorf("changeset is not sealed")
//	}
//
//	n := cs.branchesData.OnDiskCount()
//	skipped := 0
//	for i := uint32(0); i < n; i++ {
//		branch := cs.branchesData.Item(i)
//		if retainCriteria(uint32(branch.id.Version()), branch.orphanVersion) {
//			// TODO update relative pointers
//			// TODO save key data to KV store if needed
//			err := newBranches.Append(&branch)
//			if err != nil {
//				return fmt.Errorf("failed to compact branch node %s: %w", branch.id, err)
//			}
//		} else {
//			skipped++
//			// TODO remove key from KV store if possible
//		}
//	}
//
//	return nil
//}
//
//func (cs *Changeset) compactLeaves(retainCriteria RetainCriteria, newBranches *StructReader[LeafLayout]) error {
//	if !cs.sealed {
//		return fmt.Errorf("changeset is not sealed")
//	}
//
//	n := cs.leavesData.OnDiskCount()
//	for i := uint32(0); i < n; i++ {
//		leaf := cs.leavesData.Item(i)
//		if retainCriteria(uint32(leaf.id.Version()), leaf.orphanVersion) {
//			// TODO save key data to KV store if needed
//			err := newBranches.Append(&leaf)
//			if err != nil {
//				return fmt.Errorf("failed to compact leaf node %s: %w", leaf.id, err)
//			}
//		} else {
//			// TODO remove key from KV store if possible
//		}
//	}
//
//	return nil
//}
//
//func (cs *Changeset) MarkOrphans(version uint32, nodeIds []NodeID) error {
//	// TODO add locking
//	for _, nodeId := range nodeIds {
//		if nodeId.IsLeaf() {
//			leaf, err := cs.ResolveLeaf(nodeId, 0)
//			if err != nil {
//				return err
//			}
//			if leaf.orphanVersion == 0 {
//				leaf.orphanVersion = version
//			}
//		} else {
//			branch, err := cs.ResolveBranch(nodeId, 0)
//			if err != nil {
//				return err
//			}
//			if branch.orphanVersion == 0 {
//				branch.orphanVersion = version
//			}
//		}
//	}
//
//	// TODO flush changes to disk
//
//	return nil
//}
