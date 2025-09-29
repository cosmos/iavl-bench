package x3

type RetainCriteria func(version, orphanVersion uint32) bool
