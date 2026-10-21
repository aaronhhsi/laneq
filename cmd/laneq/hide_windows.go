package main

import (
	"log"
	"os/exec"
	"sync"
	"syscall"
	"unsafe"
)

// hideWindow keeps a console worker (and anything it runs, like ffmpeg) from
// opening a window when laneq itself runs without one.
func hideWindow(c *exec.Cmd) {
	const createNoWindow = 0x08000000
	c.SysProcAttr = &syscall.SysProcAttr{HideWindow: true, CreationFlags: createNoWindow}
}

// Windows doesn't kill child processes when their parent dies, so a crashed or
// force-stopped laneq would leave its worker running, and the restarted laneq
// would start a second one. Workers are put in a job object that is closed —
// killing everything in it — when laneq's process ends, however it ends.
var (
	kernel32                 = syscall.NewLazyDLL("kernel32.dll")
	createJobObject          = kernel32.NewProc("CreateJobObjectW")
	setInformationJobObject  = kernel32.NewProc("SetInformationJobObject")
	assignProcessToJobObject = kernel32.NewProc("AssignProcessToJobObject")

	jobOnce   sync.Once
	jobHandle uintptr
)

type basicLimits struct {
	PerProcessUserTimeLimit int64
	PerJobUserTimeLimit     int64
	LimitFlags              uint32
	MinimumWorkingSetSize   uintptr
	MaximumWorkingSetSize   uintptr
	ActiveProcessLimit      uint32
	Affinity                uintptr
	PriorityClass           uint32
	SchedulingClass         uint32
}

type extendedLimits struct {
	Basic                 basicLimits
	IoInfo                [6]uint64
	ProcessMemoryLimit    uintptr
	JobMemoryLimit        uintptr
	PeakProcessMemoryUsed uintptr
	PeakJobMemoryUsed     uintptr
}

func killWithParent(c *exec.Cmd) {
	jobOnce.Do(func() {
		h, _, err := createJobObject.Call(0, 0)
		if h == 0 {
			log.Printf("worker: no job object (%v); it may outlive laneq", err)
			return
		}
		const extendedLimitInformation = 9
		const killOnJobClose = 0x2000
		info := extendedLimits{Basic: basicLimits{LimitFlags: killOnJobClose}}
		if ok, _, err := setInformationJobObject.Call(h, extendedLimitInformation,
			uintptr(unsafe.Pointer(&info)), unsafe.Sizeof(info)); ok == 0 {
			log.Printf("worker: job object limits: %v", err)
			return
		}
		jobHandle = h // deliberately never closed: the OS closes it when laneq exits
	})
	if jobHandle == 0 {
		return
	}
	const processSetQuota, processTerminate = 0x0100, 0x0001
	p, err := syscall.OpenProcess(processSetQuota|processTerminate, false, uint32(c.Process.Pid))
	if err != nil {
		log.Printf("worker: open process: %v", err)
		return
	}
	defer syscall.CloseHandle(p)
	if ok, _, err := assignProcessToJobObject.Call(jobHandle, uintptr(p)); ok == 0 {
		log.Printf("worker: assign to job object: %v", err)
	}
}
