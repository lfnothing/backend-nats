package tasks

import (
	"backend-nats/stat"
	"encoding/json"
	"github.com/nats-io/go-nats"
)

//--------------------------------------
//  sync test
//--------------------------------------

const (
	default_sync_task_name    = "sync"
	default_sync_task_subject = "sync"
)

type SyncMsg struct {
	Id string `json:"id"`
}

type SyncTask struct {
	name       string
	subject    string
	async      bool
	msg        SyncMsg
	statistics *stat.Statistics
}

func NewSyncTask() *SyncTask {
	return &SyncTask{
		name:       default_sync_task_name,
		subject:    default_sync_task_subject,
		async:      false,
		statistics: stat.NewStatistics(),
	}
}

func (this *SyncTask) Name() string {
	return this.name
}

func (this *SyncTask) Subject() string {
	return this.subject
}

func (this *SyncTask) Async() bool {
	return this.async
}

func (this *SyncTask) TaskMsgHandler(msg *nats.Msg) (err error, in int, out int) {
	in = len(msg.Data)
	defer func() {
		this.statistics.Request(1).InMsgs(in).OutMsgs(out)
	}()

	err = json.Unmarshal(msg.Data, &this.msg)
	if err != nil {
		this.statistics.Failed(1)
		return
	}
	this.statistics.Success(1)
	return
}

func (this *SyncTask) Stat() *stat.Statistics {
	return this.statistics
}
