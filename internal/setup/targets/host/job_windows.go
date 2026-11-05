//go:build windows

package host

import (
	"fmt"
	"unsafe"

	"golang.org/x/sys/windows"
)

// containChild puts the JVM in a Job object that Windows kills when this
// process goes away (JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE): if the service host
// crashes, the SCM kills it at the wait hint, or the machine shuts down before
// the host relayed Stop, the JVM does not outlive it with port 8083 still
// bound. Closing the handle is what triggers the kill, so processChild keeps it
// open for the JVM's whole life and closes it in Wait.
func containChild(pid int) (processContainment, error) {
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return nil, fmt.Errorf("creating a job object: %w", err)
	}
	info := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{}
	info.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	if _, err := windows.SetInformationJobObject(job, windows.JobObjectExtendedLimitInformation,
		uintptr(unsafe.Pointer(&info)), uint32(unsafe.Sizeof(info))); err != nil {
		_ = windows.CloseHandle(job)
		return nil, fmt.Errorf("setting the job's kill-on-close limit: %w", err)
	}
	// PROCESS_SET_QUOTA is what AssignProcessToJobObject requires and
	// PROCESS_TERMINATE is what the job's kill needs.
	proc, err := windows.OpenProcess(windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE, false, uint32(pid))
	if err != nil {
		_ = windows.CloseHandle(job)
		return nil, fmt.Errorf("opening the JVM process: %w", err)
	}
	defer windows.CloseHandle(proc)
	if err := windows.AssignProcessToJobObject(job, proc); err != nil {
		_ = windows.CloseHandle(job)
		return nil, fmt.Errorf("assigning the JVM to the job: %w", err)
	}
	return windowsJob{handle: job}, nil
}

// windowsJob is the Job object handle; closing it kills what the job holds.
type windowsJob struct{ handle windows.Handle }

func (j windowsJob) Close() error { return windows.CloseHandle(j.handle) }
