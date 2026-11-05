//go:build windows

package host

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
	"golang.org/x/sys/windows/svc"
	"golang.org/x/sys/windows/svc/mgr"
)

// The Windows binding of the service target (row U8b): golang.org/x/sys/
// windows/svc/mgr for the manager, svc.Run and svc.IsWindowsService for the
// service host. Everything the target decides lives in winsvc.go and
// servicehost.go, which compile on every OS; this file is the thin adapter
// (one mgr call per method, the Win32 error mappings) and the build tag.

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
		case windows.ERROR_SERVICE_EXISTS:
			return fmt.Errorf("%w: %w", errServiceExists, err)
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
	// ServiceStartName and SidType are what keep the service off LocalSystem:
	// the SCM logs it on as its own virtual account (NT SERVICE\<name>), whose
	// SID the unrestricted SID type writes into the token.
	sidType := uint32(windows.SERVICE_SID_TYPE_NONE)
	if cfg.ServiceSID {
		sidType = windows.SERVICE_SID_TYPE_UNRESTRICTED
	}
	s, err := c.m.CreateService(name, exe, mgr.Config{
		DisplayName:      cfg.DisplayName,
		Description:      cfg.Description,
		StartType:        start,
		ServiceStartName: cfg.Account,
		SidType:          sidType,
	}, args...)
	if err != nil {
		return nil, mapSCMError(err)
	}
	if len(cfg.Privileges) > 0 {
		// The privilege set is a second call: CreateService has no field for it.
		// A service left with the SCM's default set would keep SeImpersonate,
		// which is the step from a service account to SYSTEM, so a failure here
		// removes the service rather than leaving it wider than its record.
		if err := setRequiredPrivileges(s.Handle, cfg.Privileges); err != nil {
			_ = s.Delete()
			_ = s.Close()
			return nil, fmt.Errorf("%w: restricting the privileges of %s to %s: %w", ErrServiceManager, q(name), strings.Join(cfg.Privileges, ", "), err)
		}
	}
	return &mgrService{s: s}, nil
}

// setRequiredPrivileges sets SERVICE_CONFIG_REQUIRED_PRIVILEGES_INFO on a
// service: the whole set of privileges its token keeps, written as the
// double-null-terminated multi-string the API takes.
func setRequiredPrivileges(h windows.Handle, privileges []string) error {
	var buf []uint16
	for _, p := range privileges {
		buf = append(buf, windows.StringToUTF16(p)...)
	}
	buf = append(buf, 0)
	// SERVICE_REQUIRED_PRIVILEGES_INFO is one LPWSTR; x/sys has no mirror, so
	// the one-field struct is declared where it is used.
	info := struct{ Privileges *uint16 }{Privileges: &buf[0]}
	err := windows.ChangeServiceConfig2(h, windows.SERVICE_CONFIG_REQUIRED_PRIVILEGES_INFO, (*byte)(unsafe.Pointer(&info)))
	runtime.KeepAlive(buf)
	return err
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
		State:                   serviceState(st.State),
		Accepts:                 serviceAccept(st.Accepts),
		CheckPoint:              st.CheckPoint,
		WaitHint:                st.WaitHint,
		ProcessID:               st.ProcessId,
		Win32ExitCode:           st.Win32ExitCode,
		ServiceSpecificExitCode: st.ServiceSpecificExitCode,
	}
}

func toSvcStatus(st serviceStatus) svc.Status {
	return svc.Status{
		State:                   svc.State(st.State),
		Accepts:                 svc.Accepted(st.Accepts),
		CheckPoint:              st.CheckPoint,
		WaitHint:                st.WaitHint,
		ProcessId:               st.ProcessID,
		Win32ExitCode:           st.Win32ExitCode,
		ServiceSpecificExitCode: st.ServiceSpecificExitCode,
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
// forwarded synchronously by the report callback. The supervisor reports
// Running, stop pending and interrogate answers, never Stopped: svc.Run sends
// the final Stopped itself, with the code Execute returns (a JVM exit as
// ServiceSpecificExitCode). Only the request channel needs a forwarder, and it
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

func (platformServiceHost) ProgramFiles() (string, error) { return defaultProgramFiles() }

func (platformServiceHost) CheckTrusted(path string, tree bool) error {
	return windowsTrust{}.CheckTrusted(path, tree)
}
