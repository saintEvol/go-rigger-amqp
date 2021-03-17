package riggerAmqp

import (
	"errors"
	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/saintEvol/go-rigger/rigger"
	"time"
)
/*
MQ 信道, 定义在messages.proto中
*/
// 关闭频道
func (channel *Channel) Close() error {
	if channel.Pid == nil {
		return nil
	}
	f := rigger.Root().Root.StopFuture(channel.Pid)
	return f.Wait()
}

// 开始消费消息
func (channel *Channel) Consume(config ConsumeConfig) error {
	if channel.Pid == nil {
		return ErrorFindNoPid("nil")
	}
	f := rigger.Root().Root.RequestFuture(channel.Pid, &config, 3 * time.Second)

	return fetchFutureError(f)
}

// 停止消费消息
func (channel *Channel) CancelConsume(consumer string, noWait bool) error  {
	if channel.Pid == nil {
		return ErrorFindNoPid("nil")
	}

	f := rigger.Root().Root.RequestFuture(channel.Pid, &CancelConsume{
		Consumer: consumer,
		NoWait:   noWait,
	}, 3 * time.Second)
	return fetchFutureError(f)
}

// 同步发送
func (channel *Channel) Publish(publish *Publish) error {
	f := rigger.Root().Root.RequestFuture(channel.Pid, publish, 3 * time.Second)
	return fetchFutureError(f)
}

// 异步发送
func (channel *Channel) PublishAsync(publish *Publish) {
	rigger.Root().Root.Send(channel.Pid, publish)
}

func (channel *Channel) Ack(tag uint64, multiple bool) error {
	r := rigger.Root().Root.RequestFuture(channel.Pid, &Ack{
		tag:      tag,
		multiple: multiple,
	}, 3 * time.Second)
	return fetchFutureError(r)
}

func (channel *Channel) Nack(tag uint64, multiple bool, requeue bool) error {
	r := rigger.Root().Root.RequestFuture(channel.Pid, &Nack{
		tag:      tag,
		multiple: multiple,
		requeue: requeue,
	}, 3 * time.Second)
	return fetchFutureError(r)
}
func (channel *Channel) Reject(tag uint64, requeue bool) error {
	r := rigger.Root().Root.RequestFuture(channel.Pid, &Reject{
		tag:      tag,
		requeue: requeue,
	}, 3 * time.Second)
	return fetchFutureError(r)
}

func (channel *Channel) ExchangeDeclare(config *ExchangeDeclareConfig) error {
	r := rigger.Root().Root.RequestFuture(channel.Pid, config, 3 * time.Second)
	return fetchFutureError(r)
}

func (channel *Channel) ExchangeDelete(config *ExchangeDeleteConfig) error {
	r := rigger.Root().Root.RequestFuture(channel.Pid, config, 3 * time.Second)
	return fetchFutureError(r)
}

func (channel *Channel) ExchangeBind(config *ExchangeBindConfig) error {
	r := rigger.Root().Root.RequestFuture(channel.Pid, config, 3 * time.Second)
	return fetchFutureError(r)
}

func (channel *Channel) ExchangeUnbind(config *ExchangeUnbindConfig) error {
	r := rigger.Root().Root.RequestFuture(channel.Pid, config, 3 * time.Second)
	return fetchFutureError(r)
}

func (channel *Channel) QueueDeclare(config *QueueDeclareConfig) (*QueueInspection, error) {
	r := rigger.Root().Root.RequestFuture(channel.Pid, config, 3 * time.Second)
	if ret, err := r.Result(); err == nil {
		resp := ret.(*QueueInspection)
		if resp.Error == "" {
			return resp, nil
		} else {
			return nil, errors.New(resp.Error)
		}
	} else {
		return nil, err
	}
}

func (channel *Channel) QueueDelete(config *QueueDeleteConfig) (int, error) {
	f := rigger.Root().Root.RequestFuture(channel.Pid, config, 3 * time.Second)
	if r, err := f.Result(); err == nil {
		resp := r.(*QueueDeleteResp)
		if resp.Error == "" {
			return int(resp.Purged), nil
		} else {
			return 0, errors.New(resp.Error)
		}
	} else {
		return 0, err
	}
}

func (channel *Channel) QueueBind(config *QueueBindConfig) error {
	f := rigger.Root().Root.RequestFuture(channel.Pid, config, 3 * time.Second)
	return fetchFutureError(f)
}

func (channel *Channel) QueueUnbind(config *QueueUnbindConfig) error {
	f := rigger.Root().Root.RequestFuture(channel.Pid, config, 3 * time.Second)
	return fetchFutureError(f)
}

func (channel *Channel) QueueInspect(config *QueueInspectConfig) (*QueueInspection, error) {
	f := rigger.Root().Root.RequestFuture(channel.Pid, config, 3 * time.Second)
	if r, err := f.Result(); err == nil {
		resp := r.(*QueueInspection)
		if resp.Error == "" {
			return resp, nil
		} else {
			return nil, errors.New(resp.Error)
		}
	} else {
		return nil, err
	}
}


func (channel *Channel) QueuePurge(config *QueuePurgeConfig) (int, error) {
	f := rigger.Root().Root.RequestFuture(channel.Pid, config, 3 * time.Second)
	if r, err := f.Result(); err == nil {
		resp := r.(*QueueDeleteResp)
		if resp.Error == "" {
			return int(resp.Purged), nil
		} else {
			return 0, errors.New(resp.Error)
		}
	} else {
		return 0, err
	}
}

func toError(maybeError interface{}) error {
	err, ok := maybeError.(*rigger.Error)
	if ok {
		if err == nil {
			return  nil
		} else {
			return err
		}
	} else {
		return maybeError.(error)
	}
}

/*
如果 future的结果是error, 则此函数可以快捷的取出future的错误,如果有错,则返回error,否则返回nil
*/
func fetchFutureError(future *actor.Future) error {
	if ret, err := future.Result(); err != nil {
		return err
	} else {
		return toError(ret)
	}
}
