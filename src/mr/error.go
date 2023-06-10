package mr

type NoIdleTaskError string

func (e NoIdleTaskError) Error() string {
	return "no idle task at this time"
}

const ErrNoIdleTask = NoIdleTaskError("no idle task at this time")

type NoMoreTaskError string

func (e NoMoreTaskError) Error() string {
	return "no more task as the whole computation has completed"
}

const ErrNoMoreTask = NoMoreTaskError("no more task as the whole computation has completed")
