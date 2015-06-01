package recipes

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	. "gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type LockSuite struct{}

var _ = Suite(&LockSuite{})

func (s *LockSuite) TestBasic(c *C) {
	options := LockOptions{1, false, false, nil}

	lock, _ := Acquire(testClient, "basic", options)
	c.Assert(lock, NotNil)

	_, err := Acquire(testClient, "basic", options)
	c.Assert(err, NotNil)

	c.Assert(lock.Release(), IsNil)

	lock, _ = Acquire(testClient, "basic", options)
	c.Assert(lock, NotNil)

	c.Assert(lock.Release(), IsNil)
}

func (s *LockSuite) TestExpire(c *C) {
	options := LockOptions{1, false, false, nil}

	lock, _ := Acquire(testClient, "expire", options)
	time.Sleep(2 * time.Second)

	isAcquired, _ := lock.IsAcquired()
	c.Assert(isAcquired, Equals, false)
	c.Assert(lock.Release(), NotNil)

	lock, _ = Acquire(testClient, "expire", options)
	start := time.Now()
	<-lock.Expired
	c.Assert(time.Since(start) >= time.Second, Equals, true)

	isAcquired, _ = lock.IsAcquired()
	c.Assert(isAcquired, Equals, false)
	c.Assert(lock.Release(), NotNil)
}

func (s *LockSuite) TestKeepAlive(c *C) {
	options := LockOptions{1, true, false, nil}

	lock, _ := Acquire(testClient, "keepalive", options)
	time.Sleep(2 * time.Second)

	isAcquired, _ := lock.IsAcquired()
	c.Assert(isAcquired, Equals, true)

	c.Assert(lock.Release(), IsNil)
}

func (s *LockSuite) TestWait(c *C) {
	options := LockOptions{1, false, true, nil}

	start := time.Now()
	lock, _ := Acquire(testClient, "wait", options)
	c.Assert(time.Since(start) < time.Millisecond*100, Equals, true)

	lock, _ = Acquire(testClient, "wait", options)
	c.Assert(time.Since(start) > time.Second, Equals, true)

	lock.Release()

	start = time.Now()
	locks := make(chan *Lock)
	for i := 0; i < 5; i++ {
		go func() {
			lock, _ := Acquire(testClient, "wait", options)
			locks <- lock
		}()
	}

	for i := 0; i < 5; i++ {
		lock := <-locks
		<-lock.Expired
	}
	c.Assert(time.Since(start) > 5*time.Second, Equals, true)
}

func (s *LockSuite) TestWaitCancellation(c *C) {
	cancel := make(chan bool)
	options := LockOptions{1, true, true, cancel}

	lock, _ := Acquire(testClient, "cancel", options)
	c.Assert(lock, NotNil)

	go func() {
		lock, _ = Acquire(testClient, "cancel", options)
		cancel <- true
	}()

	time.Sleep(100 * time.Millisecond)

	select {
	case <-cancel:
		c.Fail()
	default:
		break
	}

	cancel <- true
	time.Sleep(100 * time.Millisecond)

	select {
	case <-cancel:
		break
	default:
		c.Fail()
	}
}

func (s *LockSuite) TestMutualExclusion(c *C) {
	options := LockOptions{1, false, true, nil}
	startCh := make(chan time.Time)
	endCh := make(chan time.Time)
	numThreads := 50

	for i := 0; i < numThreads; i++ {
		go func() {
			time.Sleep(time.Duration(rand.Int()%100) * time.Millisecond)
			lock, _ := Acquire(testClient, "mutex", options)
			startCh <- time.Now()
			time.Sleep(time.Duration(rand.Int()%80+20) * time.Millisecond)
			endCh <- time.Now()
			lock.Release()
		}()
	}

	var wg sync.WaitGroup
	var startTimes []time.Time
	var endTimes []time.Time

	wg.Add(2)
	f := func(ch chan time.Time, array *[]time.Time) {
		for i := 0; i < numThreads; i++ {
			*array = append(*array, <-ch)
		}
		wg.Done()
	}
	go f(startCh, &startTimes)
	go f(endCh, &endTimes)
	wg.Wait()

	var prevEnd time.Time
	for i := 0; i < numThreads; i++ {
		c.Assert(endTimes[i].Sub(startTimes[i]) > time.Millisecond, Equals, true)
		c.Assert(startTimes[i].Sub(prevEnd) > time.Millisecond, Equals, true)
		prevEnd = endTimes[i]
	}
}
