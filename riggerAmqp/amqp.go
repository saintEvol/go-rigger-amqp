package riggerAmqp

import (
	"errors"
	"fmt"
	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/saintEvol/go-rigger/rigger"
	"time"
)

/**
连接配置
 */
// 根据配置信息生成uri
func (config *ConnectConfig) Uri() string {
	// "amqp://admin:admin@localhost:5672/"
	return fmt.Sprintf("amqp://%s:%s@%s:%d/%s",
		config.User, config.Password, config.Host, config.Port, config.VirtualHost)
}

// 连接MQ
func Connect(config ConnectConfig) (*Connection, error) {
	if pid, err := makeSureConnectManagingServerPid(); err != nil {
		return nil, err
	} else {
		f := rigger.Root().Root.RequestFuture(pid, &config, 3 * time.Second)
		if ret, err := f.Result(); err == nil {
			resp := ret.(*ConnectResp)
			if resp.Error == "" {
				return resp.Conn, nil
			} else {
				return nil, errors.New(resp.Error)
			}
		} else {
			return nil, err
		}
	}

}

var (
	connectionManagingSerPid *actor.PID
)

func makeSureConnectManagingServerPid() (*actor.PID, error)  {
	if connectionManagingSerPid == nil {
		if pid, exists := rigger.GetPid(connManagingServerName); !exists {
			return nil, ErrorFindNoPid(connManagingServerName)
		} else {
			connectionManagingSerPid = pid
			return pid, nil
		}
	} else {
		return connectionManagingSerPid, nil
	}
}