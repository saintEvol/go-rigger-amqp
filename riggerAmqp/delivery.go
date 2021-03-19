package riggerAmqp

import (
	"github.com/streadway/amqp"
)

/*
如果通过 riggerAmqp.Channel.Consume 接口消费消息,
则当有消费时,消费进程会收到 *riggerAmqp.Delivery 消息
*/
type Delivery struct {
	*amqp.Delivery
	channel *Channel
}

// 确认消息
func (d *Delivery) Ack(multiple bool) error {
	return d.channel.Ack(d.DeliveryTag, multiple)
}

// 消息处理失败
func (d *Delivery) Nack(multiple, requeue bool) error {
	return d.channel.Nack(d.DeliveryTag, multiple, requeue)
}

// 拒绝消息
func (d *Delivery) Reject(requeue bool) error {
	return d.channel.Reject(d.DeliveryTag, requeue)
}

