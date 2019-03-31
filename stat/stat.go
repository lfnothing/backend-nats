package stat

import (
	"sync"
	"time"
)

//--------------------------------------
// statistics
//--------------------------------------

type Statistics struct {
	request uint64
	success uint64
	failed  uint64
	inMsgs  uint64
	outMsgs uint64
	start   time.Time
	finish  time.Time
	lock    *sync.RWMutex
}

func NewStatistics() *Statistics {
	return &Statistics{
		start: time.Now(),
		lock:  &sync.RWMutex{},
	}
}

func (this *Statistics) Request(add int) *Statistics {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.request += uint64(add)
	return this
}

func (this *Statistics) Success(add int) *Statistics {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.success += uint64(add)
	return this
}

func (this *Statistics) Failed(add int) *Statistics {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.failed += uint64(add)
	return this
}

func (this *Statistics) InMsgs(add int) *Statistics {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.inMsgs += uint64(add)
	return this
}

func (this *Statistics) OutMsgs(add int) *Statistics {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.outMsgs += uint64(add)
	return this
}

func (this *Statistics) StartTime(st time.Time) *Statistics {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.start = st
	return this
}

func (this *Statistics) FinishTime(ft time.Time) *Statistics {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.finish = ft
	return this
}

func (this *Statistics) Reset() *Statistics {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.request = 0
	this.success = 0
	this.failed = 0
	this.inMsgs = 0
	this.outMsgs = 0
	this.start = time.Now()
	return this
}

func (this *Statistics) GetRequest() uint64 {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.request
}

func (this *Statistics) GetSuccess() uint64 {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.success
}

func (this *Statistics) GetFailed() uint64 {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.failed
}

func (this *Statistics) GetInMsgs() uint64 {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.inMsgs
}

func (this *Statistics) GetOutMsgs() uint64 {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.outMsgs
}

func (this *Statistics) GetStartTime() time.Time {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.start
}

func (this *Statistics) GetFinishTime() time.Time {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.finish
}
