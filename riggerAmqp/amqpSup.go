package riggerAmqp

import (
	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/saintEvol/go-rigger/rigger"
	"time"
)

const supName = "go-rigger-amqp-sup"

func init() {
	rigger.Register(supName, rigger.SupervisorBehaviourProducer(func() rigger.SupervisorBehaviour {
		return &topSup{}
	}))
}

type topSup struct {

}

func (t *topSup) OnRestarting(ctx actor.Context) {
}

func (t *topSup) OnStarted(ctx actor.Context, args interface{}) error {
	return nil
}

func (t *topSup) OnPostStarted(ctx actor.Context, args interface{}) {
}

func (t *topSup) OnStopping(ctx actor.Context) {
}

func (t *topSup) OnStopped(ctx actor.Context) {
}

func (t *topSup) OnGetSupFlag(ctx actor.Context) (supFlag rigger.SupervisorFlag, childSpecs []*rigger.SpawnSpec) {
	supFlag.MaxRetries = 10
	supFlag.WithinDuration = 2 * time.Second
	supFlag.StrategyFlag = rigger.OneForOne
	supFlag.Decider = func(reason interface{}) actor.Directive {
		return actor.RestartDirective
	}

	childSpecs = []*rigger.SpawnSpec{
		rigger.DefaultSpawnSpec(connManagingServerName),
		rigger.DefaultSpawnSpec(connectionSupName),
		rigger.DefaultSpawnSpec(channelSupName),
	}

	return
}

