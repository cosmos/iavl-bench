package metrics

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
)

var Default = NewMetrics()

type Collectable interface {
	Collect() MetricPoint
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
		t := time.Now().Unix()
		for _, c := range m.metrics {
			pt := c.Collect()
			pt.time = t
			m.Series[pt.path] = append(m.Series[pt.path], pt)
		}
	}
	for {
		select {
		case <-ctx.Done():
			flush()
			return nil
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
		path: path,
	}
	m.metrics = append(m.metrics, c)
	return c
}

type Counter struct {
	path  string
	count int64
}

func (c *Counter) Inc() {
	atomic.AddInt64(&c.count, 1)
}

func (c *Counter) Collect() MetricPoint {
	return MetricPoint{
		value: c.count,
		path:  c.path,
	}
}

type Gauge struct {
	metrics *Metrics
	path    string
	valBits uint64
}

func (g *Gauge) Set(val float64) {
	atomic.StoreUint64(&g.valBits, math.Float64bits(val))
}

func (g *Gauge) SetToCurrentTime() {
	g.Set(float64(time.Now().UnixNano()) / 1e9)
}

func (g *Gauge) Inc() {
	g.Add(1)
}

func (g *Gauge) Dec() {
	g.Add(-1)
}

func (g *Gauge) Add(val float64) {
	for {
		oldBits := atomic.LoadUint64(&g.valBits)
		newBits := math.Float64bits(math.Float64frombits(oldBits) + val)
		if atomic.CompareAndSwapUint64(&g.valBits, oldBits, newBits) {
			return
		}
	}
}

func (g *Gauge) Sub(val float64) {
	g.Add(val * -1)
}

func (g *Gauge) Collect() MetricPoint {
	val := math.Float64frombits(atomic.LoadUint64(&g.valBits))
	return MetricPoint{
		value: int64(val),
		path:  g.path,
	}
}
