package mount

import (
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/harvester/node-disk-manager/pkg/controller/blockdevice"
	"github.com/harvester/node-disk-manager/pkg/option"
	"github.com/harvester/node-disk-manager/pkg/utils"
	ndmutils "github.com/harvester/node-disk-manager/pkg/utils"
)

type Monitor struct {
	namespace string
	nodeName  string
	scanner   *blockdevice.Scanner
	startOnce sync.Once
}

func NewMonitor(opt *option.Option, scanner *blockdevice.Scanner) *Monitor {
	return &Monitor{
		startOnce: sync.Once{},
		namespace: opt.Namespace,
		nodeName:  opt.NodeName,
		scanner:   scanner,
	}
}

func (m *Monitor) Start(ctx context.Context) {
	logrus.Info("Starting mount monitor")

	// Get the mounts file path using the same logic as openProcMounts
	mountsFile := m.getMountsFilePath()
	if mountsFile == "" {
		logrus.Error("Failed to get mounts file path")
		return
	}

	logrus.Infof("Monitoring mounts file: %s", mountsFile)

	// Start monitoring in a goroutine
	go m.monitor(ctx, mountsFile)
}

func (m *Monitor) getMountsFilePath() string {
	// Use the same logic as openProcMounts in block_device.go
	// Check if host proc is mounted, if yes, use the host namespace path
	isMounted, err := utils.IsHostProcMounted()
	if err != nil {
		logrus.Warnf("Failed to check if host proc is mounted: %v, using default /proc/mounts", err)
		return "/proc/mounts"
	}

	if isMounted {
		// Use the same logic as openProcMounts with PathOverrides
		ns := ndmutils.GetHostNamespacePath(ndmutils.HostProcPath)
		file := strings.TrimSuffix(ns, "ns/") + "mounts"
		return file
	}

	return "/proc/mounts"
}

func (m *Monitor) monitor(ctx context.Context, mountsFile string) {
	// Get initial MD5 hash
	var lastMD5 string
	currentMD5, err := m.getFileMD5(mountsFile)
	if err != nil {
		logrus.Errorf("Failed to get initial MD5 for %s: %v", mountsFile, err)
		return
	}
	lastMD5 = currentMD5

	logrus.Infof("Started MD5 polling monitoring for %s", mountsFile)

	// Monitor loop with 1 minute interval
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logrus.Info("Mount monitor stopped")
			return
		case <-ticker.C:
			currentMD5, err := m.getFileMD5(mountsFile)
			if err != nil {
				logrus.Errorf("Failed to get MD5 for %s: %v", mountsFile, err)
				continue
			}

			if currentMD5 != lastMD5 {
				logrus.Debugf("Detected mount file content change, MD5 changed from %s to %s", lastMD5, currentMD5)
				lastMD5 = currentMD5
				m.wakeUpScanner()
			}
		}
	}
}

func (m *Monitor) getFileMD5(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}

func (m *Monitor) wakeUpScanner() {
	if m.scanner == nil {
		logrus.Error("Scanner is nil, cannot wake up scanner")
		return
	}

	// Wake up the scanner using the same pattern as in udev monitor
	utils.CallerWithCondLock(m.scanner.Cond, func() any {
		logrus.Debug("Signaling scanner from mount monitor")
		m.scanner.Cond.Signal()
		return nil
	})
}
