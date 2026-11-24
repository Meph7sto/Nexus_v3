package entry

import (
	"net"
	"net/http"
	"sync/atomic"

	"github.com/Wei-Shaw/sub2api/internal/handler"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

type entryProcessState struct {
	version string
	tasks   *service.SharedTaskGate
	conns   atomic.Int64
}

func provideEntryProcessState(build handler.BuildInfo, tasks *service.SharedTaskGate) *entryProcessState {
	return &entryProcessState{version: build.Version, tasks: tasks}
}

func (p *entryProcessState) bind(server *http.Server) {
	old := server.ConnState
	server.ConnState = func(conn net.Conn, state http.ConnState) {
		switch state {
		case http.StateNew:
			p.conns.Add(1)
		case http.StateClosed, http.StateHijacked:
			p.conns.Add(-1)
		}
		if old != nil {
			old(conn, state)
		}
	}
}

func (p *entryProcessState) status() service.EntryProcessStatus {
	return service.EntryProcessStatus{
		Version: p.version, ActiveConnections: int(p.conns.Load()), SharedTasksRunning: p.tasks.Running(),
	}
}
