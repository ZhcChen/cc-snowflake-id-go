package lease

import (
	"context"
	"time"
)

type fakeClock struct {
	now            int64
	monotonic      int64
	sleeps         []int64
	durationSleeps []time.Duration
	err            error
}

func (c *fakeClock) NowMillis() int64 {
	return c.now
}

func (c *fakeClock) MonotonicMillis() int64 {
	return c.monotonic
}

func (c *fakeClock) SleepUntil(ctx context.Context, millis int64) error {
	c.sleeps = append(c.sleeps, millis)
	if c.err != nil {
		return c.err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.now < millis {
		c.monotonic += millis - c.now
		c.now = millis
	}
	return nil
}

func (c *fakeClock) Sleep(ctx context.Context, duration time.Duration) error {
	c.durationSleeps = append(c.durationSleeps, duration)
	if c.err != nil {
		return c.err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	millis := duration.Milliseconds()
	if millis > 0 {
		c.monotonic += millis
		c.now += millis
	}
	return nil
}
