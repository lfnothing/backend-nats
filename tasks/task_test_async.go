package tasks

import (
	"encoding/json"
	"github.com/nats-io/go-nats"
)

//--------------------------------------
//  async test
//--------------------------------------

const (
	default_async_task_name    = "async"
	default_async_task_subject = "async"
)

type AsyncMsg struct {
	Id string `json:"id"`
}

type AsyncTask struct {
	name       string
	subject    string
	async      bool
	msg        SyncMsg
	statistics *stat.Statistics
}

func NewAsyncTask() *AsyncTask {
	return &AsyncTask{
		name:       default_async_task_name,
		subject:    default_async_task_subject,
		async:      false,
		statistics: stat.NewStatistics(),
	}
}

func (this *AsyncTask) Name() string {
	return this.name
}

func (this *AsyncTask) Subject() string {
	return this.subject
}

func (this *AsyncTask) Async() bool {
	return this.async
}

func (this *AsyncTask) TaskMsgHandler(msg *nats.Msg) (err error, in int, out int) {
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

func (this *AsyncTask) Stat() *stat.Statistics {
	return this.statistics
}

