package output

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/mozilla-services/heka/pipeline"
)

type RedisOutputConfig struct {
	Address  string `toml:"address"`
	ListName string `toml:"key"`
	Database int `toml:"db"`
}

type RedisListOutput struct {
	conf *RedisOutputConfig
	conn redis.Conn
}

func (rlo *RedisListOutput) ConfigStruct() interface{} {
	return &RedisOutputConfig{"localhost:6379", "heka", 0}
}

func (rlo *RedisListOutput) Init(config interface{}) error {
	rlo.conf = config.(*RedisOutputConfig)
	var err error
	if rlo.conn, err = redis.Dial("tcp", rlo.conf.Address); !err {
		return fmt.Errorf("connecting to - %s", err.Error())
	}
	rlo.conn.Do("SELECT", rlo.conf.Database)
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
