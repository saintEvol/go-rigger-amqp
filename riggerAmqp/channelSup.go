package riggerAmqp

import (
	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/saintEvol/go-rigger/rigger"
	"time"
)

const channelSupName = "go-rigger-amqp-channel-sup"

func init() {
	rigger.Register(channelSupName, rigger.SupervisorBehaviourProducer(func() rigger.SupervisorBehaviour {
		return &channelSup{}
	}))
}
type channelSup struct {
	
}

func (c *channelSup) OnRestarting(ctx actor.Context) {
}

func (c *channelSup) OnStarted(ctx actor.Context, args interface{}) error {
	return nil
}

func (c *channelSup) OnPostStarted(ctx actor.Context, args interface{}) {
}

func (c *channelSup) OnStopping(ctx actor.Context) {
}

func (c *channelSup) OnStopped(ctx actor.Context) {
}

func (c *channelSup) OnGetSupFlag(ctx actor.Context) (supFlag rigger.SupervisorFlag, childSpecs []*rigger.SpawnSpec) {
	supFlag.MaxRetries = 10
	supFlag.WithinDuration = 2 * time.Second
	supFlag.StrategyFlag = rigger.SimpleOneForOne
	supFlag.Decider = func(reason interface{}) actor.Directive {
		// channel不重启
		return actor.StopDirective
	}

	childSpecs = []*rigger.SpawnSpec{
		rigger.DefaultSpawnSpec(channelServerName),
	}

	return
}

