package riggerAmqp

import (
	"errors"
	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/saintEvol/go-rigger/rigger"
	"github.com/sirupsen/logrus"
	"time"
)

type closeConn struct {
	
}

/*
在连接上打开一个频道
*/
func (conn *Connection) OpenChannel() (*Channel, error) {
	f := rigger.Root().Root.RequestFuture(conn.Pid, &OpenChannel{}, 3 * time.Second)
	if ret, err := f.Result(); err == nil {
		resp := ret.(*OpenChannelResp)
		if resp.Error == "" {
			return resp.Channel, nil
		} else {
			return nil, errors.New(err.Error())
		}
	} else {
		return nil, err
	}
}

// (同步)关闭连接
func (conn *Connection) Close() error {
	f := rigger.Root().Root.StopFuture(conn.Pid)
	if ret, err := f.Result(); err == nil {
		r := ret.(*actor.Terminated)
		logrus.Trace(r)
		return nil
	} else {
		return err
	}
}

/*
注册连接的关闭事件, 注册后,连接关闭时,会向监听进程发送消息: *riggerAmqp.ConnectionClosed
如果使用此函数注册监听时, 连接已经关闭, 则会立即触发一次事件
*/
func (conn *Connection) NotifyClosed(pid *actor.PID) error {
	f := rigger.Root().Root.RequestFuture(conn.Pid, &notifyClose{pid: pid}, 3 * time.Second)
	return fetchFutureError(f)
}

func (conn *Connection) UnNotifyClosed(pid *actor.PID) {
	rigger.Root().Root.Send(conn.Pid, &unNotifyClose{pid: pid})
}

/*
注册连接连接成功的事件, 注册后,如果连接连接成功, 则监听进程会收到消息: *riggerAmqp.Connected
如果注册时, 连接已经连接成功, 则会立即触发一次事件
*/
func (conn *Connection) NotifyConnected(pid *actor.PID) error {
	f := rigger.Root().Root.RequestFuture(conn.Pid, &notifyConnected{pid: pid}, 3 * time.Second)
	return fetchFutureError(f)
}

func (conn *Connection) UnNotifyConnected(pid *actor.PID)  {
	rigger.Root().Root.Send(conn.Pid, &unNotifyConnected{pid: pid})
}
