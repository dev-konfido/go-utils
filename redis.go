package lib

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"time"

	"github.com/bsm/redislock"
	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
)

type RedisClient struct {
	ServerClient *redis.Client
	Environment  string
	Host         string
	Locker       *redislock.Client
}

func GetRedisClient(serverURL string, env string) *RedisClient {
	client := RedisClient{}

	client.Environment = env
	client.Host = serverURL

	client.connect()
	return &client
}

func (c *RedisClient) connect() {

	ctx := context.Background()

	log.Info("Conectando no Redis...", c.Host)
	c.ServerClient = redis.NewClient(&redis.Options{
		Addr:     c.Host,
		Password: "",
		DB:       0, // use default DB
	})
	pong, err := c.ServerClient.Ping(ctx).Result()
	if err != nil {
		panic(err)
	}
	log.Info("Redis ok: ", pong)

	// Create a new lock client.
	c.Locker = redislock.New(c.ServerClient)
}

func (c *RedisClient) Close() {
	c.ServerClient.Close()
}

func (c *RedisClient) Del(key string) {
	ctx := context.Background()
	c.ServerClient.Del(ctx, key)
}

func (c *RedisClient) RPush(key string, values ...interface{}) {
	ctx := context.Background()
	c.ServerClient.RPush(ctx, key, values...)
}

func (c *RedisClient) Set(key string, value interface{}, expTime time.Duration) {
	ctx := context.Background()
	c.ServerClient.Set(ctx, key, value, expTime)
}

func (c *RedisClient) HMSet(key string, fields map[string]interface{}) {
	ctx := context.Background()
	c.ServerClient.HMSet(ctx, key, fields)
}

func (c *RedisClient) HMGet(key string, fields ...string) []string {
	ctx := context.Background()
	res := c.ServerClient.HMGet(ctx, key, fields...)

	if res.Val()[0] == nil {
		return []string{}
	}

	ret := []string{}
	for _, item := range res.Val() {
		ret = append(ret, item.(string))
	}
	return ret

}

func (c *RedisClient) LPop(key string) string {
	ctx := context.Background()
	res, err := c.ServerClient.LPop(ctx, key).Result()
	if err != nil {
		res = ""
	}
	return res
}

func (c *RedisClient) GetBin(key string) []byte {
	ctx := context.Background()
	val := c.ServerClient.Get(ctx, key)
	buffer, err := val.Bytes()
	if err != nil {
		log.Debug("GetBin", key, err)
		return nil
	}
	return buffer
}

func (c *RedisClient) Get(key string) string {
	ctx := context.Background()
	res := c.ServerClient.Get(ctx, key)
	return res.Val()
}

func (c *RedisClient) GetInt(key string) int64 {
	ctx := context.Background()
	res := c.ServerClient.Get(ctx, key)
	val, err := res.Int64()
	if err != nil {
		log.Debug("GetInt", key, err)
		val = 0
	}
	return val
}

func (c *RedisClient) GetLock(ctx context.Context, key string, ttl time.Duration) (*redislock.Lock, error) {

	// Try to obtain lock.
	lock, err := c.Locker.Obtain(ctx, key, ttl, nil)
	if err == redislock.ErrNotObtained {
		return nil, fmt.Errorf("could not obtain lock")
	} else if err != nil {
		log.Warn("Erro lock.", err)
		return nil, fmt.Errorf("erro lock: %v", err)
	}

	return lock, nil

}

func (c *RedisClient) StructToBin(s interface{}) []byte {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	if err := enc.Encode(s); err != nil {
		log.Error("encode error: ", err)
	}
	return buffer.Bytes()
}

func (c *RedisClient) GetConfigInt(instance string, group string, field string) int64 {
	/*
		"cfg_gateway_config_al_event_samples_strap_cut"
		"cfg_gateway_config_se_event_samples_strap_cut_alert"
		"cfg_gateway_config_rn_event_param_strap_cut_address"
		"cfg_gateway_config_rn_event_samples_strap_cut"
	*/
	keyInst := fmt.Sprintf("cfg_%v_%v_%v", group, instance, field)
	val := c.GetInt(keyInst)
	return val
}

func (c *RedisClient) GetGtwConfigInt(instance string, field string) int64 {
	return c.GetConfigInt(instance, "gateway_config", field)
}

func (c *RedisClient) GetConfigString(instance string, group string, field string) string {
	keyInst := fmt.Sprintf("cfg_%v_%v_%v", group, instance, field)
	val := c.Get(keyInst)
	return val
}

func (c *RedisClient) GetGtwConfigString(instance string, field string) string {
	return c.GetConfigString(instance, "gateway_config", field)
}

func (c *RedisClient) SAdd(key string, value string) {
	ctx := context.Background()
	c.ServerClient.SAdd(ctx, key, value)
}

/** Deprecated: Perigo de Lock se a lista for grande. Usar o .SScan no lugar. */
func (c *RedisClient) SMembers(key string) []string {
	ctx := context.Background()
	ret := c.ServerClient.SMembers(ctx, key).Val()
	return ret
}

/** Deprecated: Perigo de Lock se a lista for grande. Usar o .Scan no lugar. */
func (c *RedisClient) Keys(pattern string) []string {
	ctx := context.Background()
	cmdRet := c.ServerClient.Keys(ctx, pattern)
	ret, err := cmdRet.Result()
	if err != nil {
		log.Error("keys error: ", err)
	}
	return ret
}

func (c *RedisClient) Scan(pattern string) []string {
	ret := []string{}
	ctx := context.Background()
	var cursor uint64
	var err error
	for {
		var keys []string
		if keys, cursor, err = c.ServerClient.Scan(ctx, cursor, pattern, 1e6).Result(); err != nil {
			log.Error("scan error: ", err)
		}

		if len(keys) > 0 {
			ret = append(ret, keys...)
		}

		if cursor == 0 {
			break
		}
	}

	return ret
}

func (c *RedisClient) SScan(setKey string, pattern string) []string {
	ret := []string{}
	ctx := context.Background()
	var cursor uint64
	var err error
	for {
		var keys []string
		if keys, cursor, err = c.ServerClient.SScan(ctx, setKey, cursor, pattern, 1e5).Result(); err != nil {
			log.Error("sscan error: ", err)
		}

		if len(keys) > 0 {
			ret = append(ret, keys...)
		}

		if cursor == 0 {
			break
		}
	}

	return ret
}
