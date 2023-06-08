package main

import (
	"sync"
)

var CreateJobEntryPool = &sync.Pool{
	New: func() interface{} {
		return &createJobEntry{}
	},
}

var NotifyProgressJobEntryPool = &sync.Pool{
	New: func() interface{} {
		return &notifyProgressJob{}
	},
}

var deleteJobEntryPool = &sync.Pool{
	New: func() interface{} {
		return &deleteJobEntry{}
	},
}
