// +build linux

package workercmd

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	"code.cloudfoundry.org/garden/server"
	"code.cloudfoundry.org/lager"
	containerd "github.com/concourse/concourse/worker/backend"
	"github.com/concourse/concourse/worker/backend/libcontainerd"
	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/ifrit/grouper"
	"github.com/tedsuo/ifrit/restart"
)

func containerdGardenServerRunner(
	logger lager.Logger,
	bindAddr,
	containerdAddr string,
	requestTimeout time.Duration,
	dnsServers []string,
	networkPool string,
) (ifrit.Runner, error) {
	const (
		graceTime = 0
		namespace = "concourse"
	)

	backendOpts := []containerd.BackendOpt{}
	networkOpts := []containerd.CNINetworkOpt{}

	if len(dnsServers) > 0 {
		networkOpts = append(networkOpts, containerd.WithNameServers(dnsServers))
	}

	if networkPool != "" {
		networkOpts = append(networkOpts, containerd.WithCNINetworkConfig(
			containerd.CNINetworkConfig{
				BridgeName:  "concourse0",
				NetworkName: "concourse",
				Subnet:      networkPool,
			}))
	}

	cniNetwork, err := containerd.NewCNINetwork(networkOpts...)
	if err != nil {
		return nil, fmt.Errorf("new cni network: %w", err)
	}

	backendOpts = append(backendOpts, containerd.WithNetwork(cniNetwork))

	backend, err := containerd.New(
		libcontainerd.New(containerdAddr, namespace, requestTimeout),
		backendOpts...,
	)
	if err != nil {
		return nil, fmt.Errorf("containerd containerd init: %w", err)
	}

	server := server.New("tcp", bindAddr,
		graceTime,
		&backend,
		logger,
	)

	runner := gardenServerRunner{logger, server}

	return restart.Restarter{
		Runner: runner,
		Load: func(prevRunner ifrit.Runner, prevErr error) ifrit.Runner {
			return runner
		},
	}, nil
}

// writeDefaultContainerdConfig writes a default containerd configuration file
// to a destination.
//
func writeDefaultContainerdConfig(dest string) error {
	// disable plugins we don't use:
	//
	// - CRI: we're not supposed to be targetted by a kubelet, so there's no
	//        need to bring up kubernete's container runtime interface plugin.
	//
	// - AUFS/BTRFS/ZFS: since linux 3.18, `overlayfs` is in upstream, which
	//                   most distros should include, so by keeping a focus
	//                   on a single snapshotter implementation we can better
	//                   reason about potential problems down the road.
	//
	const config = `disabled_plugins = ["cri", "aufs", "btrfs", "zfs"]`

	err := ioutil.WriteFile(dest, []byte(config), 0755)
	if err != nil {
		return fmt.Errorf("write file %s: %w", dest, err)
	}

	return nil
}

func (cmd *WorkerCommand) containerdRunner(logger lager.Logger) (ifrit.Runner, error) {
	const sock = "/run/containerd/containerd.sock"

	var (
		config = filepath.Join(cmd.WorkDir.Path(), "containerd.toml")
		root   = filepath.Join(cmd.WorkDir.Path(), "containerd")
		bin    = "containerd"
	)

	err := os.MkdirAll(root, 0755)
	if err != nil {
		return nil, err
	}

	if cmd.Garden.Config.Path() != "" {
		config = cmd.Garden.Config.Path()
	} else {
		err := writeDefaultContainerdConfig(config)
		if err != nil {
			return nil, fmt.Errorf("write default containerd config: %w", err)
		}
	}

	if cmd.Garden.Bin != "" {
		bin = cmd.Garden.Bin
	}

	command := exec.Command(bin,
		"--address="+sock,
		"--root="+root,
		"--config="+config,
	)

	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	command.SysProcAttr = &syscall.SysProcAttr{
		Pdeathsig: syscall.SIGKILL,
	}

	backendRunner, err := containerdGardenServerRunner(
		logger,
		cmd.bindAddr(),
		sock,
		cmd.Garden.RequestTimeout,
		cmd.Garden.DNSServers,
		cmd.ContainerNetworkPool,
	)
	if err != nil {
		return nil, fmt.Errorf("containerd garden server runner: %w", err)
	}

	return grouper.NewParallel(os.Interrupt, grouper.Members{
		{
			Name:   "containerd",
			Runner: CmdRunner{command},
		},
		{
			Name:   "containerd-backend",
			Runner: backendRunner,
		},
	}), nil
}
