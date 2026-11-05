//go:build windows

// Command winsvc is the U1-0 spike (delete after U8b lands): from an elevated
// prompt it installs itself as a demand-start service through
// golang.org/x/sys/windows/svc/mgr, starts it, queries it until Running,
// stops it and deletes it. Started by the SCM (svc.IsWindowsService) it is the
// service: it reports Running and exits on Stop. A plain java.exe cannot do
// that handshake, so U8b's Connect service needs this wrapper role.
package main

import (
	"fmt"
	"os"
	"time"

	"golang.org/x/sys/windows/svc"
	"golang.org/x/sys/windows/svc/mgr"
)

const name = "piperoom-spike"

type handler struct{}

func (handler) Execute(_ []string, r <-chan svc.ChangeRequest, s chan<- svc.Status) (bool, uint32) {
	s <- svc.Status{State: svc.Running, Accepts: svc.AcceptStop | svc.AcceptShutdown}
	for c := range r {
		switch c.Cmd {
		case svc.Interrogate:
			s <- c.CurrentStatus
		case svc.Stop, svc.Shutdown:
			s <- svc.Status{State: svc.StopPending}
			return false, 0
		}
	}
	return false, 0
}

func main() {
	if is, err := svc.IsWindowsService(); err == nil && is {
		_ = svc.Run(name, handler{}) // the SCM records a failure; nobody reads stdout here
		return
	}
	if err := spike(); err != nil {
		fmt.Fprintln(os.Stderr, "spike:", err)
		os.Exit(1)
	}
}

func spike() error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	m, err := mgr.Connect() // needs an elevated prompt
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer m.Disconnect()
	s, err := m.CreateService(name, exe, mgr.Config{DisplayName: "Piperoom spike", StartType: mgr.StartManual})
	if err != nil {
		return fmt.Errorf("create: %w", err)
	}
	defer s.Close()
	defer func() { fmt.Println("delete:", s.Delete()) }()
	fmt.Println("installed", name, "->", exe)
	if err := s.Start(); err != nil {
		return fmt.Errorf("start: %w", err)
	}
	if err := waitFor(s, svc.Running); err != nil {
		return err
	}
	if _, err := s.Control(svc.Stop); err != nil {
		return fmt.Errorf("stop: %w", err)
	}
	return waitFor(s, svc.Stopped)
}

func waitFor(s *mgr.Service, want svc.State) error {
	for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); time.Sleep(300 * time.Millisecond) {
		st, err := s.Query()
		if err != nil {
			return fmt.Errorf("query: %w", err)
		}
		fmt.Printf("query: state=%d pid=%d\n", st.State, st.ProcessId)
		if st.State == want {
			return nil
		}
	}
	return fmt.Errorf("state %d not reached in 15s", want)
}
