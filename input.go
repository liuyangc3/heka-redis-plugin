package heka_redis_input

import (
	"fmt"
	"time"
	"github.com/garyburd/redigo/redis"
	"github.com/mozilla-services/heka/pipeline"
)

type RedisListInputConfig struct {
	Address  string `toml:"address"`
	ListName string `toml:"key"`
	Database int `toml:"db"`
}

type RedisListInput struct {
	conf *RedisListInputConfig
	conn redis.Conn
}

func (rli *RedisListInput) ConfigStruct() interface{} {
	return &RedisListInputConfig{"localhost:6379", "heka", 0}
}

func (rli *RedisListInput) Init(config interface{}) error {
	rli.conf = config.(*RedisListInputConfig)
	var err error
	if rli.conn, err = redis.Dial("tcp", rli.conf.Address); !err {
		return fmt.Errorf("connecting to - %s", err.Error())
	}
	rli.conn.Do("SELECT", rli.conf.Database)
	return nil
}

func (rli *RedisListInput) Run(ir pipeline.InputRunner, h pipeline.PluginHelper) error {
	var (
		pack    *pipeline.PipelinePack
		packs []*pipeline.PipelinePack
	)

	// Get the InputRunner's chan to receive empty PipelinePacks
	inChan := ir.InChan()

	for {
		message, err := rli.conn.Do("RPOP", rli.conf.ListName)
		if err != nil {
			ir.LogError(fmt.Errorf("Redis RPOP error: %s", err))
			// TODO: should reconnect redis rather than close it
			rli.Stop()
			break
		}
		if message != nil {
			pack = <-inChan
			pack.Message.SetType("redis_list")
			pack.Message.SetPayload(string(message.([]uint8)))
			packs = []*pipeline.PipelinePack{pack}
			if packs != nil {
				for _, p := range packs {
					ir.Inject(p)
				}
			} else {
				pack.Recycle(nil)
			}
		} else {
			time.Sleep(time.Second)
		}
	}
	return nil
}

func (rli *RedisListInput) Stop() {
	rli.conn.Close()
}

func init() {
	pipeline.RegisterPlugin("RedisListInput", func() interface{} {
		return new(RedisListInput)
	})
}
