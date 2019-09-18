package scheduler

import (
	"sort"
	"sync"
	"time"
)

// Params is the arguments interface array to
// pass data when invoking a job function.
//
// The idea is to store some context pointers
// to work with later on.
type Params []interface{}

// Function is the desired function structure
// for cron jobs to execute.
//
// It will always receive as in parameter the
// caller job and an interfaces array to cast
// depending on all elements set in job Param
type Function func(*Job, ...interface{})

// Job represents an unique time  event  with
// some defined rules for code execution.
//
// Exposed Vars:
//
// - Time,	Time for the next execution (Will
// be set to time.Now plus Lapse if Zero for
// first execution)
//
// - Lapse,	Time for the next exec since the
// last one (Jobs will only trigger once and
// then deleted if Zero)
//
// - Func,	Function to call when event gets
// triggered. Not nullable (Checks will not
// be made)
//
// - Param,	Parameters to pass to the func
// for context data. Nullable
//
// Private Vars:
//
// - kill,	Bool that tells if job is marked
// to be killed or not.
//
// - prnt,	Pointer to Scheduler structure.
type Job struct {
	kill  bool
	prnt  *Scheduler
	Time  time.Time
	Lapse time.Duration
	Func  Function
	Param Params
}

// Scheduler represents the job executor for
// each job inside of it. Ticker will always
// check for jobs at lapse time if active.
//
// Private Vars:
//
// - active, Used to stop job execution from
// outside the routine and must be set by
// the Stop func.
//
// - jobs,	A list of active jobs ordered by
// time.
//
// - mtx,	Mutex for concurrency safe
type Scheduler struct {
	active bool
	jobs   []*Job
	mtx    sync.Mutex
}

// New crate and return a new scheduler class
func New() *Scheduler {
	return &Scheduler{}
}

// Stop cancels all jobs in queue list but do
// not stop currently executing ones on other
// routines.
func (s *Scheduler) Stop() {
	s.active = false
}

// Append includes the new job into the queue
// list and sorts the entire list again.
func (s *Scheduler) Append(job *Job) {
	s.mtx.Lock()
	job.prnt = s
	if job.Lapse > 0 && job.Time.IsZero() {
		job.Time = time.Now().Add(job.Lapse)
	}
	s.jobs = append(s.jobs, job)
	s.sortJobs()
	s.mtx.Unlock()
}

// Attach includes the new job into the queue
// list but unlike append it does not sort.
func (s *Scheduler) Attach(job *Job) {
	s.mtx.Lock()
	job.prnt = s
	if job.Lapse > 0 && job.Time.IsZero() {
		job.Time = time.Now().Add(job.Lapse)
	}
	s.jobs = append(s.jobs, job)
	s.mtx.Unlock()
}

// Scheduler returns the parent scheduler for
// the job.
func (j *Job) Scheduler() *Scheduler {
	return j.prnt
}

// Remove marks the job to delete in the next
// tick when accessed by the queue without
// executing it.
//
// It does not cancel current execution if is
// being made in another routine.
func (j *Job) Remove() {
	j.Scheduler().mtx.Lock()
	j.Lapse = 0
	j.kill = true
	j.Time = time.Now()
	j.Scheduler().mtx.Unlock()
}

// Every executes every job at marked time if
// scheduler active.
// Calls to function lock the current routine
// so remember to use "go" if don't want this
// behaviour.
//
// It accepts as params a check interval that
// can be any time.Duration value from ms up.
//
// For range is used and loop will only break
// if (i) item time is higher than now since
// we take for granted that the slice is sort
func (s *Scheduler) Every(lapse time.Duration) {
	tick := time.NewTicker(lapse)
	s.active = true
	for s.active {
		select {
		case now := <-tick.C:
			s.mtx.Lock()
			for n, job := range s.jobs {
				// End of queued jobs, sort again
				// everything if changed & break
				if job.Time.After(now) {
					if n != 0 {
						s.sortJobs()
					}
					break
				}
				s.execute(job, now)
			}
			s.mtx.Unlock()
		}
	}
	tick.Stop()
}

// sortJobs sorts all the jobs
func (s *Scheduler) sortJobs() {
	sort.SliceStable(s.jobs, func(i, j int) bool {
		return s.jobs[i].Time.Before(s.jobs[j].Time)
	})
}

// execute calls job function with parameters
//
// Each job gets executed in a new routine to
// reduce load and will only be called if the
// kill value is not set.
func (s *Scheduler) execute(job *Job, now time.Time) {
	// Update next instance if lapse is valid
	// or remove if not (meaning that it will
	// not be executed anymore)
	if job.Lapse != 0 {
		job.Time = now.Add(job.Lapse)
	} else {
		s.jobs = s.jobs[1:]
	}
	// Execute job if NOT programmed to kill
	if !job.kill {
		go job.Func(job, job.Param...)
	}
}
