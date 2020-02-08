package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type stepID string

func (s stepID) in(steps []stepID) bool {
	for _, step := range steps {
		if step == s {
			return true
		}
	}
	return false
}

func (s stepID) prev(steps []stepID) stepID {
	for i, step := range steps {
		if s == step {
			if i == 0 {
				return ""
			}
			return steps[i-1]
		}
	}
	return ""
}

func (s stepID) next(steps []stepID) (stepID, bool) {
	for i, step := range steps {
		if s == step {
			if len(steps) > i {
				return steps[i+1], true
			}
		}
	}
	return "", false
}

func (s stepID) removeFrom(steps []stepID) []stepID {
	nsteps := make([]stepID, len(steps)-1)
	i := 0
	for _, st := range steps {
		if st != s {
			nsteps[i] = st
			i++
		}
	}
	return nsteps
}

type itemID string

type slots struct {
	steps []stepID

	mux     sync.Mutex
	entries map[itemID]stepID
}

func newSlots(steps []stepID) *slots {
	return &slots{
		steps:   steps,
		entries: make(map[itemID]stepID),
	}
}

func (s *slots) await(entries []itemID) {
	s.mux.Lock()
	defer s.mux.Unlock()

	for _, e := range entries {
		if _, ok := s.entries[e]; ok {
			continue
		}
		s.entries[e] = s.steps[0]
	}
}

func (s *slots) release(step stepID, releaseEntries []itemID) []error {
	s.mux.Lock()
	defer s.mux.Unlock()

	var errs []error
	for _, e := range releaseEntries {
		currentStep, ok := s.entries[e]
		if !ok {
			errs = append(errs, fmt.Errorf("did not expect %s at step %s", e, currentStep))
			continue
		}
		if step != currentStep {
			errs = append(errs, fmt.Errorf("expected %s at step %s, but got %s", e, currentStep, step))
			continue
		}
		nextStep, ok := step.next(s.steps)
		if !ok {
			delete(s.entries, e)
			continue
		}
		s.entries[e] = nextStep
	}
	return errs
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

func randItemID(n int) itemID {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return itemID(b)
}

type generator struct {
	steps []stepID

	mux  sync.Mutex
	pool map[itemID][]stepID
}

func newGenerator(steps []stepID) *generator {
	return &generator{
		steps: steps,
		pool:  make(map[itemID][]stepID),
	}
}

func (g *generator) allSteps() []stepID {
	s := make([]stepID, len(g.steps))
	copy(s, g.steps)
	return s
}

func (g *generator) feed(n int) {
	for i := 0; i < n; i++ {
		item := randItemID(6)
		g.pool[item] = g.allSteps()
	}
}

func (g *generator) take(step stepID, n int) []itemID {
	g.mux.Lock()
	defer g.mux.Unlock()

	found := make([]itemID, 0)

	for {
		for item, steps := range g.pool {
			if !step.in(steps) {
				continue
			}
			if len(steps) == 1 {
				delete(g.pool, item)
				continue
			}
			g.pool[item] = step.removeFrom(steps)
			found = append(found, item)
			if len(found) == n {
				break
			}
		}
		if len(found) == n {
			break
		}
		g.feed(n - len(found))
	}

	//fmt.Printf("INFO: %s: pool size is %d\n", step, len(g.pool))
	return found
}

func (g *generator) randomStep() stepID {
	n := rand.Intn(len(g.steps))
	return g.steps[n]
}

type sequencer struct {
	steps []stepID

	counter  int
	batches  map[int]*batch // int is the batch number
	received map[itemID]map[stepID]struct{}
	items    map[itemID]map[int]struct{}
}

func newSequencer(steps []stepID) *sequencer {
	return &sequencer{
		steps:    steps,
		batches:  make(map[int]*batch),
		received: make(map[itemID]map[stepID]struct{}),
		items:    make(map[itemID]map[int]struct{}),
	}
}

func (s *sequencer) index(batch *batch) {
	if batch.step != s.steps[0] {
		s.batches[s.counter] = batch

		for _, item := range batch.items {
			if _, ok := s.items[item]; !ok {
				s.items[item] = make(map[int]struct{})
			}
			s.items[item][s.counter] = struct{}{}
		}

		s.counter++
	}

	for _, item := range batch.items {
		if _, ok := s.received[item]; !ok {
			s.received[item] = make(map[stepID]struct{})
		}
		s.received[item][batch.step] = struct{}{}
	}
}

func (s *sequencer) ready(step stepID, out chan<- *batch) {
	for id, batch := range s.batches {
		if batch.step != step {
			continue
		}

		var incomplete bool
		for _, item := range batch.items {
			if _, ok := s.received[item]; !ok {
				incomplete = true
				break
			}
			if len(s.received[item]) != len(s.steps) {
				incomplete = true
				break
			}
		}
		if !incomplete {
			out <- batch
			for _, item := range batch.items {
				if _, ok := s.items[item][id]; ok {
					delete(s.items[item], id)
				}
				if len(s.items[item]) == 0 {
					delete(s.items, item)
					delete(s.received, item)
				}
			}
			delete(s.batches, id)
		}
	}
}

func (s *sequencer) run(in <-chan *batch, out chan<- *batch) {
	for batch := range in {
		s.index(batch)

		if batch.step == s.steps[0] {
			out <- batch
		}

		for _, step := range s.steps {
			s.ready(step, out)
		}
	}
}

type batch struct {
	step  stepID
	items []itemID
}

func main() {
	rand.Seed(time.Now().UnixNano())

	steps := []stepID{"step1", "step2", "step3", "step4"}

	slots := newSlots(steps)

	var wg sync.WaitGroup
	wg.Add(3)

	in := make(chan *batch, 0)
	out := make(chan *batch, 0)

	go func() {
		s := newSequencer(steps)
		s.run(in, out)
		close(out)
		wg.Done()
	}()

	go func() {
		gen := newGenerator(steps)

		for i := 0; i < 10000; i++ {
			step := gen.randomStep()
			entries := gen.take(step, 4)
			slots.await(entries)
			in <- &batch{
				step:  step,
				items: entries,
			}
			// fmt.Printf("%s\t%v\n", step, entries)
		}
		close(in)
		wg.Done()
	}()

	go func() {
		for batch := range out {
			if errs := slots.release(batch.step, batch.items); len(errs) > 0 {
				for _, err := range errs {
					fmt.Printf("Error: item invalid: %v\n", err)
				}
			}
		}
		wg.Done()
	}()

	wg.Wait()
}
