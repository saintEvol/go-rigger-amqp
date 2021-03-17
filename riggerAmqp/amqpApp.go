package riggerAmqp

import (
	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/saintEvol/go-rigger/rigger"
	"time"
)

const AppName = "go-rigger-amqp-app"

func init() {
	rigger.Register(AppName, rigger.ApplicationBehaviourProducer(func() rigger.ApplicationBehaviour {
		return &App{}
	}))
}

type App struct {

}

func (a *App) OnRestarting(ctx actor.Context) {
}

func (a *App) OnStarted(ctx actor.Context, args interface{}) error {
	return nil
}

func (a *App) OnPostStarted(ctx actor.Context, args interface{}) {
}

func (a *App) OnStopping(ctx actor.Context) {
}

func (a *App) OnStopped(ctx actor.Context) {
}

func (a *App) OnGetSupFlag(ctx actor.Context) (supFlag rigger.SupervisorFlag, childSpecs []*rigger.SpawnSpec) {
	supFlag.StrategyFlag = rigger.OneForOne
	supFlag.MaxRetries = 10
	supFlag.WithinDuration = 10 * time.Second
	supFlag.Decider = func(reason interface{}) actor.Directive {
		return actor.RestartDirective
	}
	childSpecs = []*rigger.SpawnSpec{
		rigger.DefaultSpawnSpec(supName),
	}

	return
}



