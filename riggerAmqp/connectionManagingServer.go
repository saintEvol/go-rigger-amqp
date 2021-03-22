package riggerAmqp

import (
	"errors"
	"fmt"
	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/golang/protobuf/proto"
	"github.com/saintEvol/go-rigger/rigger"
	"strings"
	"time"
)

const connManagingServerName = "go-rigger-amqp-conn-managing-ser"

func init() {
	rigger.Register(connManagingServerName, rigger.GeneralServerBehaviourProducer(func() rigger.GeneralServerBehaviour {
		return &connectionManagingServer{}
	}))
}

type connectionManagingServer struct {
	connections map[string]*actor.PID
	connectionSupPid *actor.PID
}

func (c *connectionManagingServer) OnRestarting(ctx actor.Context) {
}

func (c *connectionManagingServer) OnStarted(ctx actor.Context, args interface{}) error {
	c.connections = make(map[string]*actor.PID)
	if pid, exists := rigger.GetPid(connectionSupName); exists {
		c.connectionSupPid = pid
		return nil
	} else {
		return errors.New("failed to get Connection sup Pid")
	}
}

func (c *connectionManagingServer) OnPostStarted(ctx actor.Context, args interface{}) {
}

func (c *connectionManagingServer) OnStopping(ctx actor.Context) {
}

func (c *connectionManagingServer) OnStopped(ctx actor.Context) {
}

func (c *connectionManagingServer) OnMessage(ctx actor.Context, message interface{}) proto.Message {
	switch msg := message.(type) {
	case *ConnectConfig:
		conn, err := c.connect(ctx, msg)
		return c.makeConnectResp(conn, err)
	case *actor.Terminated:
		c.onConnectionDown(msg.Who)
	}
	return nil
}

func (c *connectionManagingServer) connect(ctx actor.Context, config *ConnectConfig) (*Connection, error) {
	// 检查连接是否已经存在
	if c.exists(config.Tag) {
		return nil, errors.New(fmt.Sprintf("Connection tag: %s has existed", config.Tag))
	}

	// 启动连接子进程
	return c.startConnector(ctx, config)
}

func (c *connectionManagingServer) onConnectionDown(pid *actor.PID)  {
	tag := makeTagFromPid(pid)
	delete(c.connections, tag)
}

func (c *connectionManagingServer) exists(tag string) bool {
	// 如果tag为空,则会随机生成
	if tag == "" {
		return false
	}

	if _, ok := c.connections[tag]; ok {
		return true
	} else {
		return false
	}
}

func (c *connectionManagingServer) startConnector(ctx actor.Context, config *ConnectConfig) (*Connection, error) {
	connPid, err := rigger.StartChildSync(ctx, c.connectionSupPid, config, 3 * time.Second)
	if err == nil {
		// 存入信息
		tag := makeTagFromPid(connPid)
		c.connections[tag] = connPid
		// 监控
		ctx.Watch(connPid)

		return &Connection{Tag: tag, Pid: connPid}, nil
	} else {
		return nil, err
	}
}

func (c *connectionManagingServer) makeConnectResp(connection *Connection, err error) *ConnectResp {
	if err == nil {
		return &ConnectResp{
			Error: "",
			Conn:  connection,
		}
	} else {
		return &ConnectResp{
			Error: err.Error(),
			Conn:  nil,
		}
	}
}

func makeTagFromPid(pid *actor.PID) string {
	id := pid.Id
	arr := strings.Split(id, "/")
	return arr[len(arr) - 1]
}
