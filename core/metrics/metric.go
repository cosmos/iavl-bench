package metrics

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
)

var Default = NewMetrics()

type Collectable interface {
	Collect() MetricPoint
	Mutate(op Op)
}

type Metrics struct {
	in      chan Op
	metrics []Collectable
	Series  map[string][]MetricPoint
}

func NewMetrics() *Metrics {
	return &Metrics{
		in:     make(chan Op, 100_000),
		Series: make(map[string][]MetricPoint),
	}
}

type MetricPoint struct {
	time  int64
	value int64
	path  string
}

func (m *Metrics) CollectNow(path string, value int64) {
}

func (m *Metrics) Run(ctx context.Context) []MetricPoint {
	ticker := time.NewTicker(time.Second * 5)
	flush := func() {
		for _, c := range m.metrics {
			pt := c.Collect()
			m.Series[pt.path] = append(m.Series[pt.path], pt)
		}
	}
	for {
		select {
		case <-ctx.Done():
			flush()
			fmt.Println("metrics done")
			fmt.Println(m.Print())
			return nil
		case o := <-m.in:
			o.collectable.Mutate(o)
		case <-ticker.C:
			flush()
		}
	}
}

func (m *Metrics) Print() string {
	builder := strings.Builder{}
	for path, series := range m.Series {
		for _, pt := range series {
			builder.WriteString(fmt.Sprintf("%s %s %d\n", path, humanize.Comma(pt.value), pt.time))
		}
	}
	return builder.String()
}

type Op struct {
	collectable Collectable
	value       int64
}

func (m *Metrics) NewCounter(path string) *Counter {
	c := &Counter{
		metrics: m,
		path:    path,
	}
	m.metrics = append(m.metrics, c)
	return c
}

type Counter struct {
	metrics *Metrics
	path    string
	count   int64
}

func (c *Counter) Mutate(_ Op) {
	c.count++
}

func (c *Counter) Inc() {
	atomic.AddInt64(&c.count, 1)
}

func (c *Counter) Collect() MetricPoint {
	return MetricPoint{
		time:  time.Now().Unix(),
		value: c.count,
		path:  c.path,
	}
}
