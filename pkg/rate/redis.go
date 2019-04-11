package rate

import (
	"os"
	"github.com/gomodule/redigo/redis"
	"github.com/astaxie/beego/logs"
	"encoding/json"
			"time"
	"fmt"
	"strconv"
	"strings"
	"github.com/FZambia/sentinel"
	"github.com/golang/glog"
)

const (
	EncodeError       	= "encode error: %#v"
	DecodeError       	= "decode error: %#v"
	RedisSentinelMode 	= "sentinel"
	RedisSingleMode   	= "single"

	TokenKey 			= "rateToken"
	TimestampKey 		= "rateTimestamp"
)

const rateScript = `
local token_key = KEYS[1]
local timestamp_key = KEYS[2]
local rate = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])
local fill_time = capacity / rate
local ttl = math.floor(fill_time * 2)

local last_free_tokens = tonumber(redis.call("get", token_key))
if last_free_tokens == nil then
    last_free_tokens = capacity
end

local last_refreshed = tonumber(redis.call("get", timestamp_key))
if last_refreshed == nil then
    last_refreshed = 0
end

local delta = math.max(0, now - last_refreshed)
local available_tokens = math.min(capacity, last_free_tokens + (delta*rate))
local allowed = (available_tokens >= requested)
local free_tokens = available_tokens
if allowed then
    free_tokens = available_tokens - requested
end

redis.call("setex", token_key, ttl, free_tokens)
redis.call("setex", timestamp_key, ttl, now)
return { allowed, free_tokens }
`

type RedisKeeper struct {
	URL				string
	Pool 			*redis.Pool
	Script 			*redis.Script
}

func (r *RedisKeeper)SetString(key string, value string) error {
	c := r.Pool.Get()
	defer r.close(c)

	_, err := c.Do("SET", key, value)
	if err != nil {
		logs.Error("redis set key:%s failed:%s", key, err.Error())
		return err
	}
	return nil
}

func (r *RedisKeeper)GetString(key string) (string, error) {
	c := r.Pool.Get()
	defer r.close(c)

	value, err := redis.String(c.Do("GET", key))
	if err != nil {
		logs.Error("redis get key:%s failed:%s", key, err.Error())
		return "", err
	}
	return value, err
}

func (r *RedisKeeper)MultiGetString(keys []string) ([]string, error) {
	c := r.Pool.Get()
	defer r.close(c)

	var args = make([]interface{}, len(keys))
	for i, k := range keys {
		args[i] = k
	}
	values, err := redis.Strings(c.Do("MGET", args...))
	if err != nil {
		logs.Error("redis multi get key:%#v failed:%s", keys, err.Error())
		return nil, err
	}
	return values, nil
}

func (r *RedisKeeper)SetNotExist(key string, obj interface{}) error {
	c := r.Pool.Get()
	defer r.close(c)

	v, err := json.Marshal(obj)
	if err != nil {
		logs.Error(EncodeError, err)
		return err
	}
	n, err := c.Do("SETNX", key, v)
	if err != nil {
		logs.Error("redis set if not exist key:%s failed:%s", key, err.Error())
		return err
	}
	if n == int64(1) {
		return nil
	}
	return &RedisError{Key: key}
}

func (r *RedisKeeper)Set(key string, obj interface{}) error {
	c := r.Pool.Get()
	defer r.close(c)

	v, err := json.Marshal(obj)
	if err != nil {
		logs.Error(EncodeError, err)
		return err
	}
	_, err = c.Do("SET", key, v)
	if err != nil {
		logs.Error("redis set object key:%s failed:%s", key, err.Error())
		return err
	}
	return nil
}

func (r *RedisKeeper)Eval(rate int, capacity int, request int) (bool, error) {
	c := r.Pool.Get()
	defer r.close(c)

	if err :=r.Script.Load(c); err != nil {
		glog.Errorf("redis EVAL load script failed: %v\n", err)
		return false, err
	}

	glog.V(5).Infof("Eval Rate %d Capacity %d request %d\n", rate, capacity, request)

	res, err := r.Script.Do(c, TokenKey, TimestampKey, rate, capacity, time.Now().Unix(), request)
	glog.V(5).Infof("Debug info Eval: %v\n", res)

	if err != nil {
		return false, err
	}
	result := res.([]interface{})
	if result[0] == nil {
		return false, nil
	}

	return true, nil
}

func (r *RedisKeeper)Get(key string, obj interface{}) error {
	c := r.Pool.Get()
	defer r.close(c)

	value, err := redis.Bytes(c.Do("GET", key))
	if err != nil {
		logs.Error("redis get object key:%s failed: %s", key, err.Error())
		return err
	}

	err = json.Unmarshal(value, &obj)
	if err != nil {
		logs.Error(DecodeError, err)
		return err
	}
	return nil
}

func (r *RedisKeeper)HashSet(key, field string, obj interface{}) error {
	c := r.Pool.Get()
	defer r.close(c)

	v, err := json.Marshal(obj)
	if err != nil {
		logs.Error(EncodeError, err)
		return err
	}
	n, err := c.Do("HSET", key, field, v)
	if err != nil {
		logs.Error("redis hash set key:%s failed:%s", key, err.Error())
		return err
	}
	if n == int64(1) {
		return nil
	}
	return &RedisError{Key: key}
}

// multiValue: key_name field1 value1... fieldN valueN
// []string{"key_name", "field1", "value1", "field2", "value2"}
func (r *RedisKeeper)MultiHashSet(multiValue []interface{}) error {
	c := r.Pool.Get()
	defer r.close(c)

	var args = make([]interface{}, len(multiValue))
	for i, k := range multiValue {
		args[i] = k
	}

	n, err := c.Do("HMSET", args...)
	if err != nil {
		logs.Error("redis multi hash set key:%s failed:%s", multiValue[0], err.Error())
		return err
	}
	if n == string("OK") {
		return nil
	}
	return &RedisError{Key: multiValue[0].(string)}
}

func (r *RedisKeeper)HashGet(key, field string, obj interface{}) error {
	c := r.Pool.Get()
	defer r.close(c)

	value, err := redis.Bytes(c.Do("HGET", key, field))
	if err != nil {
		logs.Error("redis hash get key:%s failed:%s", key, err.Error())
		return err
	}

	err = json.Unmarshal(value, &obj)
	if err != nil {
		logs.Error(DecodeError, err)
		return err
	}
	return nil
}

func (r *RedisKeeper)HashDelete(key, field string) error {
	c := r.Pool.Get()
	defer r.close(c)

	_, err := c.Do("HDEL", key, field)
	if err != nil {
		logs.Error("redis hash delete key:%s failed:%s", key, err.Error())
		return err
	}
	return nil
}

func (r *RedisKeeper)HashGetAll(key string) ([]interface{}, error) {
	c := r.Pool.Get()
	defer r.close(c)

	values, err := redis.Values(c.Do("HVALS", key))
	if err != nil {
		logs.Error("redis hash get key:%s all failed:%s", key, err.Error())
		return nil, err
	}
	return values, nil
}

func (r *RedisKeeper)Delete(key string) error {
	c := r.Pool.Get()
	defer r.close(c)

	_, err := c.Do("DEL", key)
	if err != nil {
		logs.Error("redis delete key:%s failed:%s", key, err)
		return err
	}
	return nil
}

func (r *RedisKeeper)Exist(key string) (bool, error) {
	c := r.Pool.Get()
	defer r.close(c)

	e, err := redis.Bool(c.Do("GET", key))
	if err != nil {
		logs.Error("redis exist key: %s failed:%s", key, err.Error())
		return e, err
	}
	return e, nil
}

func (r *RedisKeeper)Expire(key string, expiration int) error {
	c := r.Pool.Get()
	defer r.close(c)

	n, _ := c.Do("EXPIRE", key, expiration)
	if n == int64(1) {
		return nil
	}
	return &RedisError{Key: key}
}

func (r *RedisKeeper)close(conn redis.Conn) {
	conn.Close()
}

// custom error
type RedisError struct {
	Key string
}

func (r *RedisError) Error() string {
	return fmt.Sprintf("redis operation failed, key:%s", r.Key)
}

func getSinglePool()*redis.Pool{
	redisDbStr := os.Getenv("REDIS_DB")
	redisDb, _ := strconv.Atoi(redisDbStr)
	redisPass := os.Getenv("REDIS_PASSWD")
	redisHost := os.Getenv("REDIS_HOST")

	redisPool := &redis.Pool{
		MaxIdle:     1,
		MaxActive:   10,
		IdleTimeout: 180 * time.Second,
		Dial: func() (redis.Conn, error) {
			db := redis.DialDatabase(redisDb)
			password := redis.DialPassword(redisPass)
			c, err := redis.Dial("tcp", redisHost, password, db)
			if err != nil {
				return nil, err
			}
			return c, nil
		},
	}

	return redisPool
}

func getSentinelPool() (*redis.Pool) {
	redisHosts := os.Getenv("REDIS_HOSTS")
	redisAddrs := strings.Split(redisHosts, ",")
	redisDbstr := os.Getenv("REDIS_DB")
	redisDb, _ := strconv.Atoi(redisDbstr)
	redisPass := os.Getenv("REDIS_PASSWD")
	redisMaster := os.Getenv("REDIS_MASTER_NAME")

	sntnl := &sentinel.Sentinel{
		Addrs:      redisAddrs,
		MasterName: redisMaster,
		Dial: func(addr string) (redis.Conn, error) {
			timeout := 5000 * time.Millisecond
			c, err := redis.Dial("tcp", addr, redis.DialConnectTimeout(timeout), redis.DialReadTimeout(timeout), redis.DialWriteTimeout(timeout))
			if err != nil {
				return nil, err
			}
			return c, nil
		},
	}

	RedisPool := &redis.Pool{
		MaxIdle:     6,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			db := redis.DialDatabase(redisDb)
			password := redis.DialPassword(redisPass)
			masterAddr, err := sntnl.MasterAddr()
			if err != nil {
				return nil, err
			}
			c, err := redis.Dial("tcp", masterAddr, password, db)
			if err != nil {
				return nil, err
			}
			return c, nil
		},
	}

	return RedisPool
}

func NewRedisClient()(*RedisKeeper, error){
	redisMode := os.Getenv("REDIS_MODE")
	var redisPool *redis.Pool
	switch redisMode {
	case RedisSentinelMode:
		redisPool = getSentinelPool()
	case RedisSingleMode:
		redisPool = getSinglePool()
	default:
		redisPool = getSinglePool()
	}

	redisURL := os.Getenv("REDIS_HOSTS") + os.Getenv("REDIS_HOST")
	//TODO: verify redis server is available

	return &RedisKeeper{
		URL : redisURL,
		Pool: redisPool,
		Script: redis.NewScript(2, rateScript),
	}, nil
}