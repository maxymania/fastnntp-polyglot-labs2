/*
Copyright (c) 2018 Simon Schmidt

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/


package healthmap

import "sync"

func cloneb(b []byte) []byte{
	s := make([]byte,len(b))
	copy(s,b)
	return s
}
type Health struct{
	Name     []byte
	Writable bool
}

type HealthReceiver interface{
	IssueHealth(h Health)
}

var healthRecvsLock sync.Mutex
var healthRecvs []HealthReceiver
func AddHealthReceiver(hr HealthReceiver) {
	healthRecvsLock.Lock(); defer healthRecvsLock.Unlock()
	healthRecvs = append(healthRecvs,hr)
}
func RemoveHealthReceiver(hr HealthReceiver) {
	healthRecvsLock.Lock(); defer healthRecvsLock.Unlock()
	l := len(healthRecvs)-1
	for i,ohr := range healthRecvs {
		if ohr!=hr { continue }
		if i!=l {
			healthRecvs[i] = healthRecvs[l]
		}
		healthRecvs = healthRecvs[:l]
	}
}
func IssueHealth(h Health) {
	for _,hr := range healthRecvs { if hr!=nil { hr.IssueHealth(h) } }
}


type HealthMap struct {
	hmap map[string]*Health
	access sync.RWMutex
}
func (n *HealthMap) Init() {
	n.hmap = make(map[string]*Health)
}
func (n *HealthMap) SetWritable(name []byte,writable bool) {
	n.access.Lock(); defer n.access.Unlock()
	h := n.hmap[string(name)]
	if h!=nil {
		h.Writable = writable
	} else {
		n.hmap[string(name)] = &Health{cloneb(name),writable}
	}
}
func (n *HealthMap) GetWritable(name []byte,def bool) bool {
	n.access.RLock(); defer n.access.RUnlock()
	h := n.hmap[string(name)]
	if h!=nil { return h.Writable }
	return def
}
func (n *HealthMap) GetAll() (hs []Health) {
	n.access.RLock(); defer n.access.RUnlock()
	hs = make([]Health,0,len(n.hmap))
	for _,h := range n.hmap {
		if h==nil { continue }
		hs = append(hs,*h)
	}
	return
}
