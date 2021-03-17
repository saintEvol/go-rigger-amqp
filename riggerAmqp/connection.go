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
