package recipes

import (
	"os"
	"time"

	"github.com/coreos/go-etcd/etcd"
	"gopkg.in/errgo.v1"
)

// Lock wraps all the metadata for a lock.
type Lock struct {
	Expired chan bool
	Options LockOptions
	client  *etcd.Client
	key     string
	node    *etcd.Node
}

// LockOptions specifies the options set for a lock.
type LockOptions struct {
	Timeout   uint64
	KeepAlive bool
	Wait      bool
	Cancel    chan bool
}

// NewLock returns a new lock on the provided key.
func NewLock(key string, client *etcd.Client, options LockOptions) *Lock {
	return &Lock{make(chan bool), options, client, key, nil}
}

func addChildDir(value string, client *etcd.Client, key string, timeout uint64) (*etcd.Node, error) {
	if value == "" {
		var err error
		value, err = os.Hostname()

		if err != nil {
			return nil, errgo.Notef(err, "failed to get hostname")
		}
	}

	// Enqueue a *request* for the lock.
	res, err := client.AddChild(key, value, timeout)

	if err != nil {
		return nil, err
	}

	return res.Node, nil
}

func waitForExpire(lock *Lock, node *etcd.Node, timeout uint64, cancel chan bool) (bool, error) {
	receiver := make(chan *etcd.Response)
	stop := make(chan bool)
	cancelled := false

	defer func() {
		close(stop)
	}()

	go lock.client.Watch(node.Key, node.ModifiedIndex, false, receiver, stop)

	for !cancelled {
		select {
		case <-time.After(time.Duration(timeout) * time.Second / 2):
			break
		case res, more := <-receiver:
			if !more {
				return false, nil
			}

			if res.Action == "delete" || res.Action == "expire" {
				return false, nil
			}
		case <-cancel:
			cancelled = true
		}

		if _, err := lock.client.Update(lock.node.Key, lock.node.Value, timeout); err != nil {
			return cancelled, err
		}
	}

	return cancelled, nil
}

// Acquire will attempt to acquire the lock for the given amount of time.
func Acquire(client *etcd.Client, key string, options LockOptions) (*Lock, error) {
	return acquire("", client, key, options)
}

func acquire(name string, client *etcd.Client, key string, options LockOptions) (*Lock, error) {
	key = addPrefix(key)
	hasLock := false
	keepWaiting := options.Wait

	client.SyncCluster()

	childNode, err := addChildDir(name, client, key, options.Timeout)
	var owner *etcd.Node

	if err != nil {
		return nil, errgo.Mask(err)
	}

	lock := &Lock{make(chan bool), options, client, key, childNode}

	for {
		owner = nil

		res, err := client.Get(key, true, true)

		if err != nil {
			return nil, errgo.Mask(err)
		}

		if len(res.Node.Nodes) > 0 {
			owner = res.Node.Nodes[0]
			if owner.CreatedIndex == childNode.CreatedIndex {
				hasLock = true
			} else if keepWaiting {
				// Find the *previous* node before us and wait for it to expire.
				var prev *etcd.Node
				for _, node := range res.Node.Nodes {
					if node.CreatedIndex == childNode.CreatedIndex {
						break
					}
					prev = node
				}

				cancelled, err := waitForExpire(lock, prev, options.Timeout, options.Cancel)
				if err != nil {
					return nil, errgo.Mask(err)
				}

				keepWaiting = !cancelled
			}
		}

		if !keepWaiting {
			break
		}

		if hasLock {
			break
		} else {
			// Update the ttl again.
			if _, err = client.Update(childNode.Key, childNode.Value, options.Timeout); err != nil {
				return nil, errgo.Mask(err)
			}
		}
	}

	if !hasLock {
		_, err := client.Delete(childNode.Key, false)

		if err != nil || owner == nil {
			err = errgo.New("timed out while trying to acquire lock")
		} else {
			err = errgo.New("lock is owned by " + owner.Value)
		}
		return nil, err
	}

	// If we get the lock, reset the timeout and return the lock.
	if _, err = client.Update(childNode.Key, childNode.Value, options.Timeout); err != nil {
		return nil, errgo.Mask(err)
	}

	// Start a goroutine to watch the lock key and unblock the expired channel when
	// it disappears.
	go func() {
		receiver := make(chan *etcd.Response)
		stop := make(chan bool)

		defer func() {
			close(stop)
		}()

		go client.Watch(lock.node.Key, lock.node.ModifiedIndex, false, receiver, stop)

		for {
			if res, more := <-receiver; more {
				if res.Action == "delete" || res.Action == "expire" {
					close(lock.Expired)
					return
				}
			} else {
				return
			}
		}
	}()

	// Start a goroutine for periodically refreshing the lock key so it doesn't
	// expire because of the timeout.
	if options.KeepAlive {
		go func() {
			errCount := 0

			// Exit the keep alive goroutine if we see 25 errors. A legitimate error
			// is only the one where error code = 100.
			for errCount < 25 {
				if _, err = client.Update(childNode.Key, childNode.Value, options.Timeout); err != nil {
					if _err, ok := err.(*etcd.EtcdError); ok {
						// Key missing? Lock has expired, can't keep it alive anymore because
						// someone else could have sneaked in and acquired it.
						if _err.ErrorCode == 100 {
							return
						}
					}

					errCount++
				}

				time.Sleep(time.Duration(options.Timeout) * time.Second / 2)
			}
		}()
	}

	return lock, nil
}

// IsAcquired returns whether the lock is acquired or not.
func (l *Lock) IsAcquired() (bool, error) {
	if l == nil {
		return false, nil
	}

	res, err := l.client.Get(l.key, true, false)

	if err != nil {
		if _err, ok := err.(*etcd.EtcdError); ok {
			if _err.ErrorCode == 100 {
				return false, nil
			}
		}

		return false, errgo.Mask(err)
	}

	// We must be the smallest child node for us to be the owner of the lock.
	if len(res.Node.Nodes) > 0 {
		owner := res.Node.Nodes[0]
		return owner.CreatedIndex == l.node.CreatedIndex, nil
	}

	return false, nil
}

// Release releases the lock.
func (l *Lock) Release() error {
	if l == nil {
		return nil
	}

	l.client.SyncCluster()

	if _, err := l.client.Delete(l.node.Key, false); err != nil {
		if _err, ok := err.(*etcd.EtcdError); ok {
			if _err.ErrorCode == 100 {
				return errgo.New("lock has expired or been released already")
			}
		}

		return errgo.Mask(err)
	}

	return nil
}
