package heka_redis_output

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/mozilla-services/heka/pipeline"
)

type RedisOutputConfig struct {
	Address  string `toml:"address"`
	ListName string `toml:"key"`
}

type RedisListOutput struct {
	conf *RedisOutputConfig
	conn redis.Conn
}

func (rlo *RedisListOutput) ConfigStruct() interface{} {
	return &RedisOutputConfig{"localhost:6379", "heka"}
}

func (rlo *RedisListOutput) Init(config interface{}) error {
	rlo.conf = config.(*RedisOutputConfig)
	var err error
	rlo.conn, err = redis.Dial("tcp", rlo.conf.Address)
	if err != nil {
		return fmt.Errorf("connecting to - %s", err.Error())
	}
	return nil
}

func (rlo *RedisListOutput) Run(or pipeline.OutputRunner, h pipeline.PluginHelper) error {
	inChan := or.InChan()
	for pack := range inChan {
		payload := pack.Message.GetPayload()
		_, err := rlo.conn.Do("LPUSH", rlo.conf.ListName, payload)
		if err != nil {
			or.LogError(fmt.Errorf("Redis LPUSH error: %s", err))
			continue
		}
		pack.Recycle(nil)
	}
	return nil
}

func (rlo *RedisListOutput) Stop() {
	rlo.conn.Close()
}

func init() {
	pipeline.RegisterPlugin("RedisListOutput", func() interface{} {
		return new(RedisListOutput)
	})
}
