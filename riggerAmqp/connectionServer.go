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

const connectionServerName = "go-rigger-amqp-Connection-server"

type notifyClose struct {
	pid *actor.PID
}

type unNotifyClose struct {
	pid *actor.PID
}

type unNotifyConnected struct {
	pid *actor.PID
}

type notifyConnected struct {
	pid *actor.PID
}

// 连接关闭时的消息
type ConnectionClosed struct {
	Connection *Connection // 哪个连接断开了
	Error      error       //  如果错误为空
}

// 连接成功的消息
type Connected struct {
	Connection  *Connection // 连接
	IsReconnect bool        // 是否是重连
}

func init() {
	rigger.RegisterStartFun(connectionServerName, rigger.GeneralServerBehaviourProducer(func() rigger.GeneralServerBehaviour {
		return &connectionServer{}
	}), func(parent actor.SpawnerContext, props *actor.Props, args interface{}) (pid *actor.PID, err error) {
		config := args.(*ConnectConfig)
		tag := config.Tag
		if tag == "" {
			pid := parent.SpawnPrefix(props, "go-rigger-amqp-Connection")
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

	/*
	关闭消息订阅者
	当连接关闭时,会将消息通知给所有的进程,如果是异常关闭,则错误为非空
	*/
	closeSubscriber map[string]*actor.PID
	connectSubscriber map[string]*actor.PID
}

func (c *connectionServer) OnRestarting(ctx actor.Context) {
}

func (c *connectionServer) OnStarted(ctx actor.Context, args interface{}) error {
	logrus.Tracef("Connection server started")
	if pid, exists := rigger.GetPid(channelSupName); exists {
		// 初始化关闭订阅map
		c.closeSubscriber = make(map[string]*actor.PID)
		c.connectSubscriber = make(map[string]*actor.PID)

		c.channelSupPid = pid
		c.config = args.(*ConnectConfig)
		c.connectError = c.connect(c.config)
		if c.connectError != nil {
			return errors.New(fmt.Sprintf("Error when init Connection, reason: %s",
				c.connectError.Error()))
		}
		// 发出连接成功的消息
		c.onConnected(ctx, false, nil)
		// 初始化控制频道
		c.controlChannel = make(chan bool)
		c.reconnectTimeoutChannel = nil

		// 开启连接循环
		go c.connectionLoop(ctx)
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
	// 通知连接断开
	c.onConnectionClosed(ctx, nil, nil)
}

func (c *connectionServer) OnStopped(ctx actor.Context) {
}

func (c *connectionServer) OnMessage(ctx actor.Context, message interface{}) proto.Message {
	switch msg := message.(type) {
	case *OpenChannel:
		channel, err := c.openChannel(ctx)
		return c.handleOpenChannelRet(channel, err)
	case *notifyClose:
		err := c.notifyClose(ctx, msg)
		return rigger.FromError(err)
	case *unNotifyClose:
		err := c.unNotifyClose(ctx, msg)
		return rigger.FromError(err)
	case *notifyConnected:
		return rigger.FromError(c.notifyConnected(ctx, msg))
	case *unNotifyConnected:
		return rigger.FromError(c.unNotifyConnected(ctx, msg))
	case *actor.Terminated:
		c.onSubscriperDown(msg.Who)
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

	if c.connection != nil {
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

func (c *connectionServer) handleReconnect(ctx actor.Context)  {
	c.reconnectTimeoutChannel = nil
	// 开始重连,
	if err := c.connect(c.config); err != nil {
		c.connectError = err
		// 设置超时时间, 超时后继续尝试
		c.reconnectTimeoutChannel = time.After(2 * time.Second)
	} else {
		c.onConnected(ctx, true, nil)
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

func (c *connectionServer) connectionLoop(ctx actor.Context)  {
	for {
		select {
		case <- c.controlChannel:
			// 退出
			return
		case <- c.reconnectTimeoutChannel:
			logrus.Trace("重连超时")
			c.handleReconnect(ctx)
		case err := <- c.notifyConnClose:
			c.connectError = err
			// 通知连接关闭
			c.onConnectionClosed(ctx, err, nil)
			if err != nil {
				logrus.Error("Connection closed, reason: %s", err.Error())
				// 开始重连,
				c.handleReconnect(ctx)
			}
		}

	}
}

func (c *connectionServer) notifyClose(ctx actor.Context, data *notifyClose) error {
	if data == nil {
		return fmt.Errorf("got nil *notifyClose")
	}

	//
	if data.pid == nil {
		return fmt.Errorf("pid could not be nil in *notifyClose")
	}

	key := data.pid.String()
	// 如果已经有了,算成功
	if _, ok := c.closeSubscriber[key]; ok {
		return nil
	}

	ctx.Watch(data.pid)
	c.closeSubscriber[key] = data.pid
	// 如果当前处于关闭状态,触发一次事件
	if c.connection == nil {
		c.onConnectionClosed(ctx, c.connectError, data.pid)
	}

	return nil
}

func (c *connectionServer) unNotifyClose(ctx actor.Context, data *unNotifyClose) error {
	if data == nil {
		return fmt.Errorf("got nil *unNotifyClose")
	}

	//
	if data.pid == nil {
		return fmt.Errorf("pid could not be nil in *unNotifyClose")
	}

	key := data.pid.String()
	delete(c.closeSubscriber, key)
	// 如果这个进程没有任何事件了,就取消进程监听
	if _, ok := c.connectSubscriber[key]; !ok {
		ctx.Unwatch(data.pid)
	}

	return nil
}


func (c *connectionServer) notifyConnected(ctx actor.Context, data *notifyConnected) error {
	if data == nil {
		return fmt.Errorf("got nil *notifyClose")
	}

	//
	if data.pid == nil {
		return fmt.Errorf("pid could not be nil in *notifyClose")
	}

	key := data.pid.String()
	// 如果已经有了,算成功
	if _, ok := c.connectSubscriber[key]; ok {
		return nil
	}

	ctx.Watch(data.pid)
	c.connectSubscriber[key] = data.pid
	// 如果处于连接状态,触发一次事件
	if c.connection != nil {
		c.onConnected(ctx, false, data.pid)
	}
	return nil
}

func (c *connectionServer) unNotifyConnected(ctx actor.Context, data *unNotifyConnected) error {
	if data == nil {
		return fmt.Errorf("got nil *unNotifyConnected")
	}

	//
	if data.pid == nil {
		return fmt.Errorf("pid could not be nil in *unNotifyConnected")
	}

	key := data.pid.String()
	delete(c.connectSubscriber, key)
	// 如果这个进程没有任何事件了,就取消进程监听
	if _, ok := c.closeSubscriber[key]; !ok {
		ctx.Unwatch(data.pid)
	}

	return nil
}

func (c *connectionServer) onSubscriperDown(pid *actor.PID)  {
	key := pid.String()
	if _, ok := c.closeSubscriber[key]; ok {
		delete(c.closeSubscriber, key)
	}

	if _, ok := c.connectSubscriber[key]; ok {
		delete(c.connectSubscriber, key)
	}
}

func (c *connectionServer) onConnectionClosed(ctx actor.Context, err error, pid *actor.PID)  {
	connection := &Connection{
		Tag: makeTagFromPid(ctx.Self()),
		Pid: ctx.Self(),
	}
	msg := &ConnectionClosed{
		Connection: connection,
		Error:      err,
	}

	if pid != nil {
		ctx.Send(pid, msg)
		return
	}

	for _, pid := range c.closeSubscriber {
		ctx.Send(pid, msg)
	}
}

func (c *connectionServer) onConnected(ctx actor.Context, isRe bool, pid *actor.PID) {
	connection := &Connection{
		Tag: makeTagFromPid(ctx.Self()),
		Pid: ctx.Self(),
	}
	msg := &Connected{
		Connection:  connection,
		IsReconnect: isRe,
	}

	if pid != nil {
		ctx.Send(pid, msg)
		return
	}

	for _, pid := range c.connectSubscriber {
		ctx.Send(pid, msg)
	}
}