package riggerAmqp

import (
	"errors"
	"fmt"
	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/golang/protobuf/proto"
	"github.com/saintEvol/go-rigger/rigger"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

type Closed struct {
	error *amqp.Error
}

const channelServerName = "go-rigger-amqp-channel-server"

func init() {
	rigger.Register(channelServerName, rigger.GeneralServerBehaviourProducer(func() rigger.GeneralServerBehaviour {
		return &channelServer{}
	}))
}

type subscriber struct {
	tag string
	pid *actor.PID
}
type channelServer struct {
	channel *amqp.Channel
	closeError *amqp.Error
	controlChannel chan bool // 控制信道,暂时只使用布尔值控制退出及唤醒协程
	notifyChanClose chan *amqp.Error
	notifyChanCancel chan string // TODO 暂时不处理
	//notifyChanAck chan uint64
	//notifyChanNack chan uint64
	notifyChanFlow chan bool // TODO 暂时不处理
	notifyChanReturn chan amqp.Return
	notifyChanPublish chan amqp.Confirmation
	subscriber *subscriber // 定阅者 TODO 暂时只允许一个定阅者
	delivery <- chan amqp.Delivery
	//subscribers map[*actor.PID]bool // 所有的订阅者
}

func (c *channelServer) OnRestarting(ctx actor.Context) {
}

func (c *channelServer) OnStarted(ctx actor.Context, args interface{}) error {
	//c.subscribers = make(map[*actor.PID]bool)
	c.channel = args.(*amqp.Channel)

	// 初始化各种频道
	c.controlChannel = make(chan bool)

	c.notifyChanCancel = make(chan string)
	c.channel.NotifyCancel(c.notifyChanCancel)

	// 关闭
	c.notifyChanClose = make(chan *amqp.Error)
	c.channel.NotifyClose(c.notifyChanClose)

	// 消息确认频道
	//c.notifyChanAck = make(chan uint64)
	//c.notifyChanNack = make(chan uint64)
	//c.channel.NotifyConfirm(c.notifyChanAck, c.notifyChanAck)

	// 流控
	c.notifyChanFlow = make(chan bool)
	c.channel.NotifyFlow(c.notifyChanFlow)

	// return
	c.notifyChanReturn = make(chan amqp.Return)
	c.channel.NotifyReturn(c.notifyChanReturn)

	c.notifyChanPublish = make(chan amqp.Confirmation)
	c.channel.NotifyPublish(c.notifyChanPublish)

	go c.amqpLoop(ctx)

	return nil

}

func (c *channelServer) OnPostStarted(ctx actor.Context, args interface{}) {
}

func (c *channelServer) OnStopping(ctx actor.Context) {
}

func (c *channelServer) OnStopped(ctx actor.Context) {
	logrus.Trace("now channel server stopped")
	// 退出协程, 避免重复处理MQ信道关闭的事件
	c.controlChannel <- true

	// 关闭MQ频道
	if c.channel != nil {
		_ = c.channel.Close()
		c.channel = nil
	}

	// 释放各种信道
	if c.notifyChanClose != nil {
		if _, ok := <- c.notifyChanClose; ok {
			close(c.notifyChanClose)
			c.notifyChanClose = nil
		}
	}

	if c.notifyChanCancel != nil {
		if _, ok := <- c.notifyChanCancel; ok {
			close(c.notifyChanCancel)
			c.notifyChanCancel = nil
		}
	}

	if c.notifyChanFlow != nil {
		if _, ok := <- c.notifyChanFlow; ok {
			close(c.notifyChanFlow)
			c.notifyChanFlow = nil
		}
	}

	if c.notifyChanReturn != nil {
		if _, ok := <- c.notifyChanReturn; ok {
			close(c.notifyChanReturn)
			c.notifyChanReturn = nil
		}
	}

	if c.notifyChanPublish != nil {
		if _, ok := <- c.notifyChanPublish; ok {
			close(c.notifyChanPublish)
			c.notifyChanPublish = nil
		}
	}

	if c.delivery != nil {
		c.delivery = nil
	}

	if c.subscriber != nil {
		ctx.Send(c.subscriber.pid, &Closed{error: c.closeError})
		c.subscriber = nil
	}
}

func (c *channelServer) OnMessage(ctx actor.Context, message interface{}) proto.Message {
	switch msg := message.(type) {
	case *ConsumeConfig:
		return rigger.FromError(c.consume(msg))
	case *CancelConsume:
		return rigger.FromError(c.cancel(msg.Consumer, msg.NoWait))
	case *Publish:
		return rigger.FromError(c.publish(msg))
	case *Ack:
		return rigger.FromError(c.ack(msg))
	case *Nack:
		return rigger.FromError(c.nack(msg))
	case *Reject:
		return rigger.FromError(c.reject(msg))
	case *ExchangeDeclareConfig:
		// 先判空再显示返回 nil可以避免
		//if err := c.exchangeDeclare(msg); err == nil {
		//	return nil
		//} else {
		//	return rigger.FromError(err)
		//}
		return rigger.FromError(c.exchangeDeclare(msg)) //如果直接用这个函数,则最后的结果interface就必须有类型信息
	case *ExchangeDeleteConfig:
		return rigger.FromError(c.exchangeDelete(msg))
	case *ExchangeBindConfig:
		return rigger.FromError(c.exchangeBind(msg))
	case *ExchangeUnbindConfig:
		return rigger.FromError(c.exchangeUnbind(msg))
	case *QueueDeclareConfig:
		q, err := c.queueDeclare(msg)
		if err == nil {
			return &QueueInspection{
				Error:     "",
				Name:      q.Name,
				Messages:  int32(q.Messages),
				Consumers: int32(q.Consumers),
			}
		} else {
			return &QueueInspection{
				Error:     err.Error(),
				Name:      "",
				Messages:  0,
				Consumers: 0,
			}
		}

	case *QueueDeleteConfig:
		p, err := c.queueDelete(msg)
		if err == nil {
			return &QueueDeleteResp{
				Error:  "",
				Purged: int32(p),
			}
		} else {
			return &QueueDeleteResp{
				Error:  err.Error(),
				Purged: 0,
			}
		}
	case *QueueBindConfig:
		return rigger.FromError(c.queueBind(msg))
	case *QueueUnbindConfig:
		return rigger.FromError(c.queueUnbind(msg))
	case *QueueInspectConfig:
		q, err := c.queueInspect(msg)
		if err == nil {
			return &QueueInspection{
				Error:     "",
				Name:      q.Name,
				Messages:  int32(q.Messages),
				Consumers: int32(q.Consumers),
			}
		}
	case *QueuePurgeConfig:
		n, err := c.queuePurge(msg)
		if err == nil {
			return &QueueDeleteResp{
				Error:  "",
				Purged: int32(n),
			}
		} else {
			return &QueueDeleteResp{
				Error:  err.Error(),
				Purged: 0,
			}
		}
	}
	return nil
}

func (c *channelServer) handleChannel(channel *amqp.Channel)  {
	c.channel = channel

}

func (c *channelServer) amqpLoop(ctx actor.Context)  {
	for  {
		select {
		case c := <- c.controlChannel:
			// 退出
			if c {
				return
			}
		case err := <- c.notifyChanClose:
			logrus.Trace("notify chan close")
			// 设置错误
			c.closeError = err
			// 退出进程
			ctx.Stop(ctx.Self())
			// 退出协程
			return
		case tag := <- c.notifyChanCancel: // 消费者退出
			logrus.Trace("notify chan cancel")
			if c.subscriber != nil && c.subscriber.tag == tag{
				c.subscriber = nil
			}
		case confirmation := <- c.notifyChanPublish:
			logrus.Trace("notify chan publish")
			if c.subscriber != nil && c.subscriber.pid != nil {
				ctx.Send(c.subscriber.pid, &confirmation)
			}
		case delivery := <- c.delivery:
			logrus.Trace("notify chan delivery")
			if c.subscriber != nil && c.subscriber.pid != nil {
				ctx.Send(c.subscriber.pid, &Delivery{Delivery: &delivery, channel: &Channel{Pid: ctx.Self()}})
			}
		}
	}
}

func (c *channelServer) onSubscripberDown(pid *actor.PID)  {
	if c.subscriber != nil && c.subscriber.pid != nil && c.subscriber.pid.Address == pid.Address && pid.Id == c.subscriber.pid.Id {
		c.subscriber = nil
	}
}

// 消费, 为了简单, 目前一个channel只允许一个消费者
func (c *channelServer) consume(config *ConsumeConfig) error {
	if c.subscriber != nil {
		return errors.New("currently allow only one consumer")
	}

	if config.Pid == nil {
		return errors.New("has specify a valid Pid")
	}

	// 因为暂时无法获取自动创建的tag,所以强制要求consumer不为空
	if config.Consumer == "" {
		return errors.New("consumer could not be empty")
	}

	delivery, err := c.channel.Consume(config.Queue, config.Consumer, config.AutoAck,
		config.Exclusive, config.NoLocal, config.NoWait, config.Args)
	if err == nil {
		c.subscriber = &subscriber{
			tag: config.Consumer,
			pid: config.Pid,
		}
		c.delivery = delivery
		// 唤醒下监听信道的协程, 以更新deliver信道
		c.controlChannel <- false
		//go c.delayConsume()
		return nil
	} else {
		return err
	}

}

func (c *channelServer) delayConsume() {
	if c.delivery != nil {
		for  {
			select {
			case deliver := <- c.delivery:
				logrus.Tracef(" >> receive msg in server, msg: %s", string(deliver.Body))
			}

		}
	}
}

func (c *channelServer) cancel(consumer string, noWait bool) error {
	// 如果没有订阅者,也算取消成功
	if c.subscriber == nil {
		return nil
	}

	if c.subscriber.tag == consumer {
		c.subscriber = nil
		err := c.channel.Cancel(consumer, noWait)
		if err == nil {
			c.delivery = nil
			return nil
		} else {
			return err
		}
	} else {
		return errors.New(fmt.Sprintf("consumer not match: %s : %s", consumer, c.subscriber.tag))
	}
}

func (c *channelServer) publish(p *Publish) error {
	return c.channel.Publish(p.Exchange, p.Key, p.Mandatory, p.Immediate, *p.Message)
}

func (c *channelServer) ack(cfg *Ack) error {
	return c.channel.Ack(cfg.tag, cfg.multiple)
}

func (c *channelServer) nack(n *Nack) error {
	return c.channel.Nack(n.tag, n.multiple, n.requeue)
}

func (c *channelServer) reject(r *Reject) error {
	return c.channel.Reject(r.tag, r.requeue)
}


func (c *channelServer) exchangeDeclare(config *ExchangeDeclareConfig) error {
	if !config.IsPassive {
		return c.channel.ExchangeDeclare(config.Name, string(config.Kind), config.Durable, config.AutoDelete,
			config.Internal, config.NoWait, config.Args)
	} else {
		return c.channel.ExchangeDeclarePassive(config.Name, string(config.Kind), config.Durable, config.AutoDelete,
			config.Internal, config.NoWait, config.Args)

	}
}

func (c *channelServer) exchangeBind(config *ExchangeBindConfig) error {
	return c.channel.ExchangeBind(config.Destination, config.Key, config.Source, config.NoWait, config.Args)
}

func (c *channelServer) exchangeUnbind(config *ExchangeUnbindConfig) error {
	return c.channel.ExchangeUnbind(config.Destination, config.Key, config.Source, config.NoWait, config.Args)
}

func (c *channelServer) exchangeDelete(config *ExchangeDeleteConfig) error {
	return c.channel.ExchangeDelete(config.Name, config.IfUnused, config.NoWait)
}

func (c *channelServer) queueDeclare(config *QueueDeclareConfig) (amqp.Queue, error) {
	if config.IsPassive {
		return c.channel.QueueDeclarePassive(config.Name, config.Durable, config.AutoDelete,
			config.Exclusive, config.NoWait, config.Args)
	} else {
		return c.channel.QueueDeclare(config.Name, config.Durable, config.AutoDelete,
			config.Exclusive, config.NoWait, config.Args)
	}
}

func (c *channelServer) queueDelete(config *QueueDeleteConfig) (int, error) {
	return c.channel.QueueDelete(config.Name, config.IfUsed, config.IfEmpty, config.NoWait)
}

func (c *channelServer) queueBind(config *QueueBindConfig) error {
	return c.channel.QueueBind(config.Name, config.Key, config.Exchange, config.NoWait, config.Args)
}

func (c *channelServer) queueUnbind(config *QueueUnbindConfig) error {
	return c.channel.QueueUnbind(config.Name, config.Key, config.Exchange, config.Args)
}

func (c *channelServer) queueInspect(config *QueueInspectConfig ) (amqp.Queue, error) {
	return c.channel.QueueInspect(config.Name)
}

func (c *channelServer) queuePurge(config *QueuePurgeConfig) (int, error) {
	return c.channel.QueuePurge(config.Name, config.NoWait)
}

func (c *channelServer) getDelivery() <- chan amqp.Delivery{
	logrus.Tracef(">>>>> get deliver")
	return c.delivery
}