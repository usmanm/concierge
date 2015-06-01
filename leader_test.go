package recipes

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	. "gopkg.in/check.v1"
)

type LeaderElectionSuite struct{}

var _ = Suite(&LeaderElectionSuite{})

func (s *LeaderElectionSuite) TestBasic(c *C) {
	leader := NewLeaderElection("leader", testClient, "basic", 1)

	leader.Start()
	c.Assert(<-leader.Change, Equals, ELECTED)
	hasLeadership, _ := leader.HasLeadership()
	c.Assert(hasLeadership, Equals, true)
	leaderName, _ := leader.GetLeader()
	c.Assert(leaderName, Equals, "leader")

	leader.Stop()
	c.Assert(<-leader.Change, Equals, OUSTED)
}

func (s *LeaderElectionSuite) TestConcurrent(c *C) {
	electedCh := make(chan time.Time)
	oustedCh := make(chan time.Time)
	numThreads := 50

	for i := 0; i < numThreads; i++ {
		go func(i int) {
			name := fmt.Sprintf("leader-%d", i)
			leader := NewLeaderElection(name, testClient, "concurrent", 1)
			time.Sleep(time.Duration(rand.Int()%20) * time.Millisecond)
			leader.Start()
			change := <-leader.Change
			c.Assert(change, Equals, ELECTED)
			electedCh <- time.Now()
			hasLeadership, _ := leader.HasLeadership()
			c.Assert(hasLeadership, Equals, true)
			leaderName, _ := leader.GetLeader()
			c.Assert(leaderName, Equals, name)
			time.Sleep(time.Duration(rand.Int()%80+20) * time.Millisecond)
			leader.Stop()
			change = <-leader.Change
			c.Assert(change, Equals, OUSTED)
			oustedCh <- time.Now()
		}(i)
	}

	var wg sync.WaitGroup
	var electedTimes []time.Time
	var oustedTimes []time.Time

	wg.Add(2)
	f := func(ch chan time.Time, array *[]time.Time) {
		for i := 0; i < numThreads; i++ {
			*array = append(*array, <-ch)
		}
		wg.Done()
	}
	go f(electedCh, &electedTimes)
	go f(oustedCh, &oustedTimes)
	wg.Wait()

	var prevOusted time.Time
	for i := 0; i < numThreads; i++ {
		c.Assert(oustedTimes[i].Sub(electedTimes[i]) > time.Millisecond, Equals, true)
		c.Assert(electedTimes[i].Sub(prevOusted) > time.Millisecond, Equals, true)
		prevOusted = oustedTimes[i]
	}
}
