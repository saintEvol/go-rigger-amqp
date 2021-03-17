package riggerAmqp

import (
	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/streadway/amqp"
)
type ConsumeConfig struct {
	Pid       *actor.PID // 消费者进程id
	Queue     string     // 队列名称
	Consumer  string     // 消费者名称
	AutoAck   bool
	Exclusive bool
	NoLocal   bool
	NoWait    bool
	Args      amqp.Table
}

type Publish struct {
	Exchange string
	Key string
	Mandatory bool
	Immediate bool
	Message *amqp.Publishing
}

type Ack struct {
	tag uint64 // 确认的消息tag
	multiple bool // 是否确认多个
}

type Nack struct {
	tag uint64 // 确认的消息tag
	multiple bool // 是否确认多个
	requeue bool // 是否重新入列
}

type Reject struct {
	tag uint64 // 确认的消息tag
	requeue bool // 是否重新入列
}



type ExchangeKind string
// The common types are "direct", "fanout", "topic" and
//"headers".
const (
	ExchangeKindDirect ExchangeKind = "direct"
	ExchangeKindFanout ExchangeKind = "fanout"
	ExchangeKindTopic ExchangeKind = "topic"
	ExchangeKindHeaders ExchangeKind = "headers"
)

type ExchangeDeclareConfig struct {
	Name string
	Kind ExchangeKind
	IsPassive bool // 是否为passive模式,默认为false,如果为passive模式,则假定Exchange已经存在
	Durable bool
	AutoDelete bool
	Internal bool
	NoWait bool
	Args amqp.Table
}

type ExchangeDeleteConfig struct {
	Name string
	IfUnused bool
	NoWait bool
}

type ExchangeBindConfig struct {
	Destination string
	Key string
	Source string
	NoWait bool
	Args amqp.Table
}

type ExchangeUnbindConfig struct {
	Destination string
	Key string
	Source string
	NoWait bool
	Args amqp.Table
}

type QueueDeclareConfig struct {
	Name string
	Durable bool
	IsPassive bool // 是否为passive模式,默认为false,如果为passive模式,则假定Exchange已经存在
	AutoDelete bool
	Exclusive bool
	NoWait bool
	Args amqp.Table
}

type QueueDeleteConfig struct {
	Name string
	IfUsed bool
	IfEmpty bool
	NoWait bool
}

type QueueBindConfig struct {
	// name, key, exchange string, noWait bool, args Table
	Name string
	Key string
	Exchange string
	NoWait bool
	Args amqp.Table
}

type QueueUnbindConfig struct {
	Name string
	Key string
	Exchange string
	Args amqp.Table
}

type QueueInspectConfig struct {
	Name string
}

type QueuePurgeConfig struct {
	Name string
	NoWait bool
}
