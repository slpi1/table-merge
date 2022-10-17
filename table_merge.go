package merger

import (
	"fmt"
	"sync"
)

type Merger interface {
	Discover() []*Range
}

type merger struct {
	data [][]int

	maxX int
	maxY int

	l  sync.Mutex
	wg sync.WaitGroup

	ranges    []*Range
	discoverd map[string]int
}

func NewMerger(d [][]int) Merger {
	return &merger{
		data:      d,
		discoverd: make(map[string]int),
	}
}

func (m *merger) Discover() []*Range {
	m.maxX = len(m.data[0])
	m.maxY = len(m.data)
	start := Point{0, 0}

	m.wg.Add(1)
	go m.discover(start)
	m.wg.Wait()
	return m.ranges
}

func (m *merger) PointValue(p Point) int {
	return m.data[p.Y][p.X]
}

func (m *merger) discover(start Point) {
	defer m.wg.Done()

	// record points which has bin discoverd, escape rediscover
	m.l.Lock()
	key := fmt.Sprintf("%d-%d", start.X, start.Y)
	if _, ok := m.discoverd[key]; ok {
		m.l.Unlock()
		return
	}
	m.discoverd[key] = 1
	m.l.Unlock()

	if m.out(start) {
		return
	}

	r := &Range{
		Table: m,
		Ltop:  start,
	}

	lb, rt := r.Discover()
	m.l.Lock()
	m.ranges = append(m.ranges, r)
	// todo:: if extends, join r.Rbottom.X/r.Rbottom.X into table stop
	m.l.Unlock()

	m.wg.Add(2)
	go m.discover(lb)
	go m.discover(rt)
}

func (m *merger) AtStopX(p Point) bool {
	if p.X >= m.maxX {
		return true
	}
	return false
}
func (m *merger) AtStopY(p Point) bool {
	if p.Y >= m.maxY {
		return true
	}
	return false
}
func (m *merger) out(p Point) bool {
	if p.X >= m.maxX || p.Y >= m.maxY {
		return true
	}
	return false
}

type Range struct {
	Table   *merger
	Ltop    Point
	Rbottom Point

	cur           Point
	rightBoundary int
}

func (r *Range) Discover() (lb, rt Point) {
	r.cur = Point{
		X: r.Ltop.X,
		Y: r.Ltop.Y,
	}

	r.findRightBoundary()
	lb.X = r.Ltop.X
	lb.Y = r.cur.Y + 1
	rt.X = r.cur.X + 1
	rt.Y = r.Ltop.Y
	return lb, rt
}

func (r *Range) findRightBoundary() {
	v := r.Table.PointValue(r.cur)
	rightPoint := r.cur.Right()
	if r.stopX(rightPoint) {
		r.nextLine()
		return
	}

	rv := r.Table.PointValue(rightPoint)
	if v == rv {
		r.cur = r.cur.Right()
		r.findRightBoundary()
		return
	}

	r.rightBoundary = r.cur.X
	r.nextLine()
	return
}

func (r *Range) nextLine() {
	nextLineStart := Point{
		X: r.Ltop.X,
		Y: r.cur.Y + 1,
	}
	// at table's bottom or stop line
	if r.Table.AtStopY(nextLineStart) {
		r.finish()
		return
	}

	v := r.Table.PointValue(r.cur)
	nv := r.Table.PointValue(nextLineStart)
	if nv != v {
		r.finish()
		return
	}

	// move cur point to next line start
	r.cur = nextLineStart
	r.findRightBoundary()
}

func (r *Range) stopX(p Point) bool {
	if r.rightBoundary > 0 && p.X > r.rightBoundary {
		return true
	}

	// at table's right or stop line
	if r.Table.AtStopX(p) {
		return true
	}

	return false
}

func (r *Range) finish() {
	r.Rbottom = r.cur
}

type Point struct {
	X int
	Y int
}

func (p Point) Right() Point {
	return Point{p.X + 1, p.Y}
}
