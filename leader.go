package recipes

import (
	"sync"
	"time"

	"github.com/coreos/go-etcd/etcd"
	"gopkg.in/errgo.v1"
)

const (
	// ELECTED : the node just got leadership.
	ELECTED = iota
	// OUSTED : the node just lost leadership.
	OUSTED
)

// LeaderElection wraps all metadata for leader election.
type LeaderElection struct {
	Name     string
	Change   chan int
	isLeader bool
	client   *etcd.Client
	key      string
	timeout  uint64
	lock     *Lock
	stop     chan bool
	wg       sync.WaitGroup
}

// NewLeaderElection return a new candidate for leadership on the provided key.
func NewLeaderElection(name string, client *etcd.Client, key string, timeout uint64) *LeaderElection {
	return &LeaderElection{name, make(chan int, 10), false, client, key, timeout, nil, nil, sync.WaitGroup{}}
}

// Start participating in the leader election process.
func (l *LeaderElection) Start() error {
	if l.lock != nil {
		return errgo.New("leader election is already started")
	}

	l.stop = make(chan bool)
	l.wg.Add(1)

	go func() {
		defer l.wg.Done()

		for {
			select {
			case <-l.stop:
				return
			default:
				break
			}

			lock, err := acquire(l.Name, l.client, l.key, LockOptions{l.timeout, true, true, l.stop})

			if err != nil {
				time.Sleep(time.Duration(l.timeout) * time.Second)
				continue
			}

			l.lock = lock
			l.Change <- ELECTED
			l.isLeader = true

			select {
			case <-l.lock.Expired:
				l.Change <- OUSTED
				l.isLeader = false
			case <-l.stop:
				return
			}
		}
	}()

	return nil
}

// Stop participating in the leader election process.
func (l *LeaderElection) Stop() error {
	if l.lock != nil {
		close(l.stop)
		l.wg.Wait()

		if l.isLeader {
			l.Change <- OUSTED
			l.isLeader = false
		}

		err := l.lock.Release()
		l.lock = nil
		return err
	}

	return nil
}

// HasLeadership returns whether we have the leadership or not.
func (l *LeaderElection) HasLeadership() (bool, error) {
	if l.lock == nil {
		return false, nil
	}

	return l.lock.IsAcquired()
}

// GetLeader returns the name of the current leader.
func (l *LeaderElection) GetLeader() (string, error) {
	res, err := l.client.Get(l.key, true, false)
	if err != nil {
		if _err, ok := err.(*etcd.EtcdError); ok {
			if _err.ErrorCode == 100 {
				return "", nil
			}
		}

		return "", errgo.Mask(err)
	}

	if len(res.Node.Nodes) == 0 {
		return "", nil
	}

	return res.Node.Nodes[0].Value, nil
}
