package riggerAmqp

import (
	"errors"
	"fmt"
	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/golang/protobuf/proto"
	"github.com/saintEvol/go-rigger/rigger"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"time"
)

const connectionServerName = "go-rigger-amqp-connection-server"

type waitReadyCmd struct {

}

func init() {
	rigger.RegisterStartFun(connectionServerName, rigger.GeneralServerBehaviourProducer(func() rigger.GeneralServerBehaviour {
		return &connectionServer{}
	}), func(parent actor.SpawnerContext, props *actor.Props, args interface{}) (pid *actor.PID, err error) {
		config := args.(*ConnectConfig)
		tag := config.Tag
		if tag == "" {
			pid := parent.SpawnPrefix(props, "go-rigger-amqp-connection")
			return pid, nil
		} else {
			return parent.SpawnNamed(props, tag)
		}
	})
}

type connectionServer struct {
	channelSupPid *actor.PID // 信道监控进程ID
	controlChannel chan bool // 控制频道
	reconnectTimeoutChannel <- chan time.Time// 重连超时频道
	notifyConnClose chan *amqp.Error // 连接关闭监听频道

	config *ConnectConfig // 连接配置
	connectError error // 连接错误
	connection *amqp.Connection //当前连接
}

func (c *connectionServer) OnRestarting(ctx actor.Context) {
}

func (c *connectionServer) OnStarted(ctx actor.Context, args interface{}) error {
	logrus.Tracef("connection server started")
	if pid, exists := rigger.GetPid(channelSupName); exists {
		c.channelSupPid = pid
		c.config = args.(*ConnectConfig)
		c.connectError = c.connect(c.config)
		if c.connectError != nil {
			return errors.New(fmt.Sprintf("error when init connection, reason: %s",
				c.connectError.Error()))
		}
		// 初始化控制频道
		c.controlChannel = make(chan bool)
		c.reconnectTimeoutChannel = nil

		// 开启连接循环
		go c.connectionLoop()
		return nil
	} else {
		return errors.New(fmt.Sprintf("faild to get Pid of %s\r\n", channelSupName))
	}
}

func (c *connectionServer) OnPostStarted(ctx actor.Context, args interface{}) {
}

func (c *connectionServer) OnStopping(ctx actor.Context) {
	// 退出协程
	c.controlChannel <- true
	_ = c.connection.Close()
}

func (c *connectionServer) OnStopped(ctx actor.Context) {
}

func (c *connectionServer) OnMessage(ctx actor.Context, message interface{}) proto.Message {
	switch message.(type) {
	case *OpenChannel:
		channel, err := c.openChannel(ctx)
		return c.handleOpenChannelRet(channel, err)
	case *waitReadyCmd:
		if c.connection != nil {
			return nil
		} else {

		}
	}
	return nil
}

func (c *connectionServer) connect(config *ConnectConfig) error  {
	if c.notifyConnClose != nil {
		_, ok := <- c.notifyConnClose
		if ok {
			close(c.notifyConnClose)
		}
		c.notifyConnClose = nil
	}

	if c.connection != nil{
		if !c.connection.IsClosed() {
			_ = c.connection.Close()
		}
		c.connection = nil
	}

	conn, err := amqp.Dial(config.Uri())
	if err == nil {
		c.connection = conn
		c.notifyConnClose = make(chan *amqp.Error)
		c.connection.NotifyClose(c.notifyConnClose)
		return nil
	} else {
		return err
	}
}

func (c *connectionServer) handleReconnect()  {
	c.reconnectTimeoutChannel = nil
	// 开始重连,
	if err := c.connect(c.config); err != nil {
		c.connectError = err
		// 设置超时时间, 超时后继续尝试
		c.reconnectTimeoutChannel = time.After(2 * time.Second)
	}
}

func (c *connectionServer) openChannel(ctx actor.Context) (*Channel, error) {
	if c.connection == nil {
		return nil, ErrorConnInvalid("")
	}

	// 打开频道
	channel, err := c.connection.Channel()
	if err == nil {
		// 启动channel进程
		// TODO 优化rigger启动子进程的接口,增加可以直接回复给自定义进程ID的接口,
		pid, err := rigger.StartChildSync(ctx, c.channelSupPid, channel, 3 * time.Second)
		if err == nil {
			return &Channel{Pid: pid}, nil
		} else {
			return nil, err
		}
	} else {
		return nil, err
	}

}

func (c *connectionServer) handleOpenChannelRet(chanel *Channel, err error) *OpenChannelResp {
	if err == nil {
		return &OpenChannelResp{
			Error:   "",
			Channel: chanel,
		}
	} else {
		return &OpenChannelResp{
			Error:   err.Error(),
			Channel: nil,
		}
	}
}

func (c *connectionServer) connectionLoop()  {
	for {
		select {
		case <- c.controlChannel:
			// 退出
			return
		case <- c.reconnectTimeoutChannel:
			logrus.Trace("重连超时")
			c.handleReconnect()
		case err := <- c.notifyConnClose:
			c.connectError = err
			if err != nil {
				logrus.Error("connection closed, reason: %s", err.Error())
				// 开始重连,
				c.handleReconnect()
			}
		}

	}
}

func (c *connectionServer) getReconnectChannel() <- chan time.Time {
	return c.reconnectTimeoutChannel
}
