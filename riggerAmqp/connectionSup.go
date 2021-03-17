package riggerAmqp

import (
	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/saintEvol/go-rigger/rigger"
	"time"
)

const connectionSupName = "go-rigger-conn-sup"

func init() {
	rigger.Register(connectionSupName, rigger.SupervisorBehaviourProducer(func() rigger.SupervisorBehaviour {
		return &connectionSup{}
	}))
}

type connectionSup struct {
	
}

func (c *connectionSup) OnRestarting(ctx actor.Context) {
}

func (c *connectionSup) OnStarted(ctx actor.Context, args interface{}) error {
	return nil
}

func (c *connectionSup) OnPostStarted(ctx actor.Context, args interface{}) {
}

func (c *connectionSup) OnStopping(ctx actor.Context) {
}

func (c *connectionSup) OnStopped(ctx actor.Context) {
}

func (c *connectionSup) OnGetSupFlag(ctx actor.Context) (supFlag rigger.SupervisorFlag, childSpecs []*rigger.SpawnSpec) {
	supFlag.MaxRetries = 10
	supFlag.WithinDuration = 2 * time.Second
	supFlag.StrategyFlag = rigger.SimpleOneForOne
	supFlag.Decider = func(reason interface{}) actor.Directive {
		return actor.RestartDirective
	}

	childSpecs = []*rigger.SpawnSpec{
		rigger.DefaultSpawnSpec(connectionServerName),
	}

	return
}

