//go:build windows

package host

import (
	"errors"
	"fmt"
	"syscall"

	"golang.org/x/sys/windows"
	"golang.org/x/sys/windows/svc"
	"golang.org/x/sys/windows/svc/mgr"
)

// The Windows binding of the service target (row U8b): golang.org/x/sys/
// windows/svc/mgr for the manager, svc.Run and svc.IsWindowsService for the
// service host. Everything the target decides lives in winsvc.go and
// servicehost.go, which compile on every OS; this file is the thin adapter
// (one mgr call per method, the two error mappings) and the build tag.

// defaultServiceAPI connects to the local service control manager. It needs an
// elevated prompt: installing, starting and deleting a machine service are
// administrator operations, and a refusal here is how "not elevated" surfaces.
func defaultServiceAPI() (serviceAPI, error) {
	m, err := mgr.Connect()
	if err != nil {
		return nil, mapSCMError(err)
	}
	return &mgrConnection{m: m}, nil
}

// mapSCMError turns the Win32 codes this target branches on into sentinels, and
// keeps the original code wrapped for the message.
func mapSCMError(err error) error {
	if err == nil {
		return nil
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		switch errno {
		case windows.ERROR_SERVICE_DOES_NOT_EXIST:
			return fmt.Errorf("%w: %w", errServiceNotExist, err)
		case windows.ERROR_SERVICE_MARKED_FOR_DELETE:
			return fmt.Errorf("%w: %w", errServiceMarkedForDelete, err)
		case windows.ERROR_SERVICE_NOT_ACTIVE:
			return fmt.Errorf("%w: %w", errServiceNotActive, err)
		}
	}
	return err
}

// mgrConnection is serviceAPI over *mgr.Mgr.
type mgrConnection struct{ m *mgr.Mgr }

func (c *mgrConnection) Close() error { return c.m.Disconnect() }

func (c *mgrConnection) Create(name, exe string, args []string, cfg serviceConfig) (serviceHandle, error) {
	start := uint32(mgr.StartManual)
	if cfg.Automatic {
		start = mgr.StartAutomatic
	}
	s, err := c.m.CreateService(name, exe, mgr.Config{
		DisplayName: cfg.DisplayName,
		Description: cfg.Description,
		StartType:   start,
	}, args...)
	if err != nil {
		return nil, mapSCMError(err)
	}
	return &mgrService{s: s}, nil
}

func (c *mgrConnection) Open(name string) (serviceHandle, error) {
	s, err := c.m.OpenService(name)
	if err != nil {
		return nil, mapSCMError(err)
	}
	return &mgrService{s: s}, nil
}

// mgrService is serviceHandle over *mgr.Service.
type mgrService struct{ s *mgr.Service }

func (m *mgrService) Start() error { return mapSCMError(m.s.Start()) }

func (m *mgrService) Query() (serviceStatus, error) {
	st, err := m.s.Query()
	return fromSvcStatus(st), mapSCMError(err)
}

func (m *mgrService) Control(cmd serviceControl) (serviceStatus, error) {
	st, err := m.s.Control(svc.Cmd(cmd))
	return fromSvcStatus(st), mapSCMError(err)
}

func (m *mgrService) Delete() error { return mapSCMError(m.s.Delete()) }

func (m *mgrService) Close() error { return m.s.Close() }

// fromSvcStatus and toSvcStatus convert between svc.Status and the mirror this
// package defines so the manager and the supervisor compile on every OS. The
// numeric values are the Windows API's own, so the conversion is a cast.
func fromSvcStatus(st svc.Status) serviceStatus {
	return serviceStatus{
		State:         serviceState(st.State),
		Accepts:       serviceAccept(st.Accepts),
		CheckPoint:    st.CheckPoint,
		WaitHint:      st.WaitHint,
		ProcessID:     st.ProcessId,
		Win32ExitCode: st.Win32ExitCode,
	}
}

func toSvcStatus(st serviceStatus) svc.Status {
	return svc.Status{
		State:         svc.State(st.State),
		Accepts:       svc.Accepted(st.Accepts),
		CheckPoint:    st.CheckPoint,
		WaitHint:      st.WaitHint,
		ProcessId:     st.ProcessID,
		Win32ExitCode: st.Win32ExitCode,
	}
}

// platformServiceHost is svc.Run and svc.IsWindowsService: the only two calls
// into the service package that need the build tag.
type platformServiceHost struct{}

func (platformServiceHost) IsService() (bool, error) { return svc.IsWindowsService() }

func (platformServiceHost) Run(name string, sup *serviceSupervisor) error {
	return svc.Run(name, svcHandler{sup: sup})
}

// svcHandler bridges svc's channels to the supervisor's own types. Statuses are
// forwarded synchronously by the report callback, so the Stopped that ends the
// service is never dropped; only the request channel needs a forwarder, and it
// stops when Execute returns.
type svcHandler struct{ sup *serviceSupervisor }

func (h svcHandler) Execute(_ []string, requests <-chan svc.ChangeRequest, statuses chan<- svc.Status) (bool, uint32) {
	// Execute's args are the service name and any Start parameters; the
	// service's own command line (run-service --project ...) is what
	// RunService parsed, so nothing here is read from them.
	done := make(chan struct{})
	reqs := make(chan changeRequest)
	go func() {
		defer close(reqs)
		for {
			select {
			case <-done:
				return
			case r, ok := <-requests:
				if !ok {
					return
				}
				select {
				case reqs <- changeRequest{Cmd: serviceControl(r.Cmd), CurrentStatus: fromSvcStatus(r.CurrentStatus)}:
				case <-done:
					return
				}
			}
		}
	}()
	specific, code := h.sup.serve(reqs, func(st serviceStatus) { statuses <- toSvcStatus(st) })
	close(done)
	return specific, code
}
