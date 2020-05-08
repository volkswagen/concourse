package worker

import (
	"context"
	"fmt"
	"github.com/concourse/concourse/atc/compression"
	"io"
	"time"

	"code.cloudfoundry.org/lager"
	"github.com/hashicorp/go-multierror"

	"github.com/concourse/concourse/atc/db"
	"github.com/concourse/concourse/atc/db/lock"
	"github.com/concourse/concourse/atc/metric"
)

func NewClientWithConfig(pool Pool,
	provider WorkerProvider,
	compression compression.Compression,
	workerPollingInterval, WorkerStatusPublishInterval time.Duration) *client {
	return &client{
		pool:                        pool,
		provider:                    provider,
		compression:                 compression,
		workerPollingInterval:       workerPollingInterval,
		workerStatusPublishInterval: WorkerStatusPublishInterval,
	}
}

func (client *client) chooseTaskWorkerNew(
	ctx context.Context,
	logger lager.Logger,
	strategy ContainerPlacementStrategy,
	lockFactory lock.LockFactory,
	owner db.ContainerOwner,
	containerSpec ContainerSpec,
	workerSpec WorkerSpec,
	outputWriter io.Writer,
) (Worker, error) {
	var (
		chosenWorker    Worker
		activeTasksLock lock.Lock
		elapsed         time.Duration
		err             error
	)

	started := time.Now()
	waitForWorkerTicker := time.NewTicker(client.workerPollingInterval)
	defer waitForWorkerTicker.Stop()
	workerStatusTicker := time.NewTicker(client.workerStatusPublishInterval)
	defer workerStatusTicker.Stop()

	for {
		if chosenWorker, err = client.pool.FindOrChooseWorkerForContainer(
			ctx,
			logger,
			owner,
			containerSpec,
			workerSpec,
			strategy,
		); err != nil {
			return nil, err
		}

		if !strategy.ModifiesActiveTasks() {
			return chosenWorker, nil
		}

		var acquired bool
		if activeTasksLock, acquired, err = lockFactory.Acquire(logger, lock.NewActiveTasksLockID()); err != nil {
			return nil, err
		}

		if !acquired {
			time.Sleep(time.Second)
			continue
		}

		select {
		case <-ctx.Done():
			logger.Info("aborted-waiting-worker")
			e := multierror.Append(err, activeTasksLock.Release(), ctx.Err())
			return nil, e
		default:
		}

		if chosenWorker != nil {
			err = increaseActiveTasks(logger,
				client.pool,
				chosenWorker,
				activeTasksLock,
				owner,
				containerSpec,
				workerSpec)

			if elapsed > 0 {
				message := fmt.Sprintf("Found a free worker after waiting %s.\n", elapsed.Round(1*time.Second))
				writeOutputMessage(logger, outputWriter, message)
			}

			return chosenWorker, err
		}

		err := activeTasksLock.Release()
		if err != nil {
			return nil, err
		}

		if elapsed == 0 {
			metric.TasksWaiting.Inc()
			defer metric.TasksWaiting.Dec()
		}

		elapsed = waitForWorker(logger, waitForWorkerTicker, workerStatusTicker, outputWriter, started)
	}
}

func waitForWorker(
	logger lager.Logger,
	waitForWorkerTicker, workerStatusTicker *time.Ticker,
	outputWriter io.Writer,
	started time.Time) (elapsed time.Duration) {

	select {
	case <-waitForWorkerTicker.C:
		elapsed = time.Since(started)

	case <-workerStatusTicker.C:
		message := "All workers are busy at the moment, please stand-by.\n"
		writeOutputMessage(logger, outputWriter, message)
		elapsed = time.Since(started)
	}

	return elapsed
}

func writeOutputMessage(logger lager.Logger, outputWriter io.Writer, message string) {
	_, err := outputWriter.Write([]byte(message))
	if err != nil {
		logger.Error("failed-to-report-status", err)
	}
}

func increaseActiveTasks(
	logger lager.Logger,
	pool Pool,
	chosenWorker Worker,
	activeTasksLock lock.Lock,
	owner db.ContainerOwner,
	containerSpec ContainerSpec,
	workerSpec WorkerSpec) (err error) {

	var existingContainer bool
	defer release(activeTasksLock, err)

	existingContainer, err = pool.ContainerInWorker(logger, owner, containerSpec, workerSpec)
	if err != nil {
		return err
	}

	if !existingContainer {
		if err = chosenWorker.IncreaseActiveTasks(); err != nil {
			logger.Error("failed-to-increase-active-tasks", err)
		}
	}

	return err
}

func release(activeTasksLock lock.Lock, err error) {
	releaseErr := activeTasksLock.Release()
	if releaseErr != nil {
		err = multierror.Append(err, releaseErr)
	}
}
