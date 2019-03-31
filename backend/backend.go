package backend

import (
	"backend-nats/stat"
	"backend-nats/tasks"
	"context"
	"fmt"
	"github.com/astaxie/beego/logs"
	"github.com/go-ini/ini"
	"github.com/nats-io/go-nats"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

//--------------------------------------
// config
//--------------------------------------

const (
	default_backend_config_file = "backend.ini"
)

var (
	config = NewConfig()
)

type Config struct {
	cfg *ini.File
}

func NewConfig() *Config {
	c := new(Config)
	dir, _ := filepath.Abs(filepath.Dir(os.Args[0]))
	fp := path.Join(dir, default_backend_config_file)

	var err error
	c.cfg, err = ini.Load(fp)
	if err != nil {
		panic(fmt.Sprintf("Load [%s] error: [%s]", default_backend_config_file, err.Error()))
	}
	return c
}

func (this *Config) Get(sec string, key string) *ini.Key {
	v, err := this.cfg.Section(sec).GetKey(key)
	if err != nil {
		panic(fmt.Sprintf("Get config from section [%s] key [%s] Error: [%s]", sec, key, err.Error()))
	}
	return v
}

//--------------------------------------
// log
//--------------------------------------

func init() {
	if err := logs.Async(10000).SetLogger(logs.AdapterFile, `{"filename":"backend.log", "perm": "0666"}`); err != nil {
		panic(fmt.Sprintf("Setting log file [%s] error：[%s]\n", "backend.log", err.Error()))
	}
}

//--------------------------------------
// app
//--------------------------------------

type AppConfig struct {
	Mode                string        `json:"mode"`
	TaskInteralTime     time.Duration `json:"taskInteralTime"`
	TaskRotationTime    time.Duration `json:"taskRotationTime"`
	BackendRotationTime time.Duration `json:"backendRotationTime"`
}

func GetAppConfig() (app *AppConfig) {
	var err error
	app = new(AppConfig)
	app.Mode = config.Get("app", "mode").String()
	taskit := config.Get("app", "task_interal_time").String()
	taskrt := config.Get("app", "task_rotation_time").String()
	backendrt := config.Get("app", "backend_rotation_time").String()
	if app.TaskInteralTime, err = time.ParseDuration(taskit); err != nil {
		panic(fmt.Sprintf("Can't parse [task_interal_time] : [%s]", err.Error()))
	}
	if app.TaskRotationTime, err = time.ParseDuration(taskrt); err != nil {
		panic(fmt.Sprintf("Can't parse [task_rotation_time] : [%s]", err.Error()))
	}
	if app.BackendRotationTime, err = time.ParseDuration(backendrt); err != nil {
		panic(fmt.Sprintf("Can't parse [task_rotation_time] : [%s]", err.Error()))
	}
	return
}

//--------------------------------------
// time
//--------------------------------------

func Date(format string, timestamp ...int64) string {
	t := time.Now()
	if len(timestamp) != 0 {
		t = time.Unix(timestamp[0], 0)
	}

	Y, m, d := t.Date()
	H := t.Hour()
	i := t.Minute()
	s := t.Second()
	format = strings.Replace(format, "Y", strconv.Itoa(Y), -1)
	format = strings.Replace(format, "m", fmt.Sprintf("%02d", m), -1)
	format = strings.Replace(format, "d", fmt.Sprintf("%02d", d), -1)
	format = strings.Replace(format, "H", fmt.Sprintf("%02d", H), -1)
	format = strings.Replace(format, "i", fmt.Sprintf("%02d", i), -1)
	format = strings.Replace(format, "s", fmt.Sprintf("%02d", s), -1)
	return format
}

//--------------------------------------
// nats
//--------------------------------------

type NatsConfig struct {
	Host     string `json:"host"`
	Port     string `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
}

func GetNatsConfig() *NatsConfig {
	natsc := new(NatsConfig)
	natsc.Host = config.Get("nats", "host").String()
	natsc.Port = config.Get("nats", "port").String()
	natsc.User = config.Get("nats", "user").String()
	natsc.Password = config.Get("nats", "password").String()
	return natsc
}

func GetNatsConn() *nats.Conn {
	natsc := GetNatsConfig()
	opts := nats.GetDefaultOptions()
	opts.User = natsc.User
	opts.Password = natsc.Password
	opts.Servers = []string{fmt.Sprintf("%s:%s", natsc.Host, natsc.Port)}

	// Connect to NATS
	nc, err := opts.Connect()
	if err != nil {
		panic(fmt.Sprintf("Connect to nats server [%s:%s] error: %s", natsc.Host, natsc.Port, err.Error()))
	}
	return nc
}

//--------------------------------------
// task
//--------------------------------------

type Task interface {
	Name() string
	Subject() string
	Async() bool
	TaskMsgHandler(*nats.Msg) (error, int, int)
	Stat() *stat.Statistics
}

//--------------------------------------
// backends
//--------------------------------------

var (
	backend = NewBackend()
)

type Backend struct {
	self       *sync.WaitGroup
	groups     *sync.WaitGroup
	closing    chan bool
	tasks      []Task
	statistics *stat.Statistics
	nc         *nats.Conn
	app        *AppConfig
}

func NewBackend() *Backend {
	return &Backend{
		self:       &sync.WaitGroup{},
		groups:     &sync.WaitGroup{},
		closing:    make(chan bool),
		tasks:      make([]Task, 0, 10),
		statistics: stat.NewStatistics(),
		nc:         GetNatsConn(),
		app:        GetAppConfig(),
	}
}

func (this *Backend) Serve() {
	this.self.Add(1)
	defer this.self.Done()
	defer this.nc.Close()

	fmt.Printf("%s Backend start connecting to nats server [%s]\n", Date("Y:m:d H:i:s"), this.nc.Opts.Servers[0])
	ctx, cancel := context.WithCancel(context.Background())
	this.groups.Add(len(this.tasks))
	for i := 0; i < len(this.tasks); i++ {
		if this.tasks[i].Async() {
			go this.AsyncTaskCallback(this.tasks[i], ctx) // 传递task，不直接使用nc.Subscribe在匿名函数中实现taskMsgHander回调，避免匿名函数传递上下文task出错
		} else {
			sub, err := this.nc.SubscribeSync(this.tasks[i].Subject())
			if err != nil {
				panic(fmt.Sprintf("Sync task [%s] failed to subscribe subject [%s]", this.tasks[i].Name(), this.tasks[i].Subject()))
			}
			go this.SyncTaskWaitForMsg(this.tasks[i], sub, ctx)
		}
	}

	taskTicker := time.NewTicker(this.app.TaskRotationTime)
	backendTicker := time.NewTicker(this.app.BackendRotationTime)
	defer taskTicker.Stop()
	defer backendTicker.Stop()
	for {
		select {
		case <-this.closing:
			cancel()
			this.groups.Wait()
			return

		case <-taskTicker.C:
			now := time.Now()
			for _, task := range this.tasks {
				dur := now.Sub(task.Stat().GetStartTime())
				fmt.Printf("[%s] - [%s]这段时间，任务[%s]在订阅subject[%s]的统计数据情况如下：\n", Date("Y:m:d H:i:s", task.Stat().GetStartTime().Unix()), Date("Y:m:d H:i:s", now.Unix()), task.Name(), task.Subject())
				fmt.Printf("接收到的请求数：%d\n", task.Stat().GetRequest())
				fmt.Printf("请求失败数：%d\n", task.Stat().GetFailed())
				fmt.Printf("请求成功数：%d\n", task.Stat().GetSuccess())
				fmt.Printf("请求接收的数据量：%d\n", task.Stat().GetInMsgs())
				fmt.Printf("请求发出的数据量：%d\n", task.Stat().GetOutMsgs())
				fmt.Printf("请求的并发量：%0.2f r/s\n", float64(task.Stat().GetRequest())/dur.Seconds())
				fmt.Printf("请求的吞吐量：%0.2f b/s\n", float64(task.Stat().GetInMsgs()+task.Stat().GetOutMsgs())/dur.Seconds())
				task.Stat().Reset()
			}

		case <-backendTicker.C:
			now := time.Now()
			dur := now.Sub(this.statistics.GetStartTime())
			fmt.Printf("[%s] - [%s]这段时间，backend的统计数据情况如下：\n", Date("Y:m:d H:i:s", this.statistics.GetStartTime().Unix()), Date("Y:m:d H:i:s", now.Unix()))
			fmt.Printf("接收到的请求数：%d\n", this.statistics.GetRequest())
			fmt.Printf("请求失败数：%d\n", this.statistics.GetFailed())
			fmt.Printf("请求成功数：%d\n", this.statistics.GetSuccess())
			fmt.Printf("请求接收的数据量：%d\n", this.statistics.GetInMsgs())
			fmt.Printf("请求发出的数据量：%d\n", this.statistics.GetOutMsgs())
			fmt.Printf("请求的并发量：%0.2f r/s\n", float64(this.statistics.GetRequest())/dur.Seconds())
			fmt.Printf("请求的吞吐量：%0.2f b/s\n", float64(this.statistics.GetInMsgs()+this.statistics.GetOutMsgs())/dur.Seconds())
			this.statistics.Reset()
		}
	}
}

func (this *Backend) Stop() {
	close(this.closing)
	this.self.Wait()
}

func (this *Backend) AsyncTaskCallback(task Task, ctx context.Context) {
	defer this.groups.Done()
	sub, err := this.nc.Subscribe(task.Subject(), func(msg *nats.Msg) {
		var (
			in  int
			out int
			err error
		)
		time.Sleep(this.app.TaskInteralTime)
		err, in, out = task.TaskMsgHandler(msg)
		if err == nil {
			this.statistics.Success(1)
		} else {
			if msg == nil {
				logs.Error("Async task [%s] failed [%s]", task.Name(), err.Error())
			} else {
				logs.Error("Async task [%s] failed [%s] with msg [%s]", task.Name(), err.Error(), string(msg.Data))
			}
			this.statistics.Failed(1)
		}
		this.statistics.Request(1).InMsgs(in).OutMsgs(out)
		if this.app.Mode == "dev" {
			fmt.Printf("%s Async task [%s] subscribe subject [%s] receive msg [%s]\n", Date("Y:m:d H:i:s"), task.Name(), task.Subject(), string(msg.Data))
		}
	})
	if err != nil {
		panic(fmt.Sprintf("Async task [%s] failed to subscribe subject [%s]", task.Name(), task.Subject()))
	}
	fmt.Printf("%s Async task [%s] start to subscribe subject [%s]\n", Date("Y:m:d H:i:s"), task.Name(), task.Subject())
	for {
		select {
		case <-ctx.Done():
			sub.Unsubscribe()
			return
		}
	}
}

func (this *Backend) SyncTaskWaitForMsg(task Task, sub *nats.Subscription, ctx context.Context) {
	defer this.groups.Done()
	defer sub.Unsubscribe()

	fmt.Printf("%s Sync task [%s] start to subscribe subject [%s]\n", Date("Y:m:d H:i:s"), task.Name(), task.Subject())
	for {
		var (
			in  int
			out int
			err error
			msg *nats.Msg
		)
		msg, err = sub.NextMsg(this.app.TaskInteralTime)
		if err == nil {
			err, in, out = task.TaskMsgHandler(msg)
			if err == nil {
				this.statistics.Success(1)
			} else {
				this.statistics.Failed(1)
			}
			this.statistics.Request(1).InMsgs(in).OutMsgs(out)
		}
		if err == nil {
			if this.app.Mode == "dev" {
				fmt.Printf("%s sync task [%s] subscribe subject [%s] receive msg [%s]\n", Date("Y:m:d H:i:s"), task.Name(), task.Subject(), string(msg.Data))
			}
		} else if err != nats.ErrTimeout {
			if msg == nil {
				logs.Error("Sync task [%s] failed [%s]", task.Name(), err.Error())
			} else {
				logs.Error("Sync task [%s] failed [%s] with msg [%s]", task.Name(), err.Error(), string(msg.Data))
			}
		}

		select {
		case <-ctx.Done():
			return
		default: // 防止协程阻塞在监听退出信号上
		}
	}
}

func RegisterBackend(t Task) {
	for _, task := range backend.tasks {
		if task.Name() == t.Name() {
			panic("Duplicate task name")
		}
		if task.Subject() == t.Subject() {
			panic("Duplicate task subject")
		}
	}
	backend.tasks = append(backend.tasks, t)
}

func GetBackendInstance() *Backend {
	RegisterBackend(tasks.NewSyncTask())
	RegisterBackend(tasks.NewAsyncTask())
	return backend
}
