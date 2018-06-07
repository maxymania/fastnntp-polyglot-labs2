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


package mlst

import "github.com/hashicorp/memberlist" 
import "github.com/maxymania/fastnntp-polyglot-labs2/utils/cluster"
import "net"
import "errors"

var EBadMerge = errors.New("EBadMerge")

func Convert(m *memberlist.Node) *cluster.Node {
	return &cluster.Node{
		Name: m.Name,
		Addr: append(net.IP(nil),m.Addr...),
		Meta: append([]byte(nil),m.Meta...),
	}
}
func Convert2(m *memberlist.Node) cluster.Node {
	return cluster.Node{
		Name: m.Name,
		Addr: append(net.IP(nil),m.Addr...),
		Meta: append([]byte(nil),m.Meta...),
	}
}

type Delegate struct{
	cluster.StateHandler
	H cluster.Handler
}
func (d *Delegate) NodeMeta(limit int) []byte {
	return d.H.Metadata(limit)
}
func (d *Delegate) NotifyMsg([]byte) {}
func (d *Delegate) GetBroadcasts(overhead, limit int) [][]byte { return nil }

var _ memberlist.Delegate = (*Delegate)(nil)

func (d *Delegate) NotifyJoin  (n *memberlist.Node) { d.H.Create(Convert(n)) }
func (d *Delegate) NotifyLeave (n *memberlist.Node) { d.H.Update(Convert(n)) }
func (d *Delegate) NotifyUpdate(n *memberlist.Node) { d.H.Delete(Convert(n)) }

var _ memberlist.EventDelegate = (*Delegate)(nil)

func (d *Delegate) NotifyAlive(peer *memberlist.Node) error {
	if !d.H.ValidateAll([]cluster.Node{Convert2(peer)}) { return EBadMerge }
	return nil
}
func (d *Delegate) NotifyMerge(peers []*memberlist.Node) error {
	c := make([]cluster.Node,len(peers))
	for i,peer := range peers { c[i] = Convert2(peer) }
	if !d.H.ValidateAll(c) { return EBadMerge }
	return nil
}

func Configure(cfg *memberlist.Config,h cluster.Handler,sh cluster.StateHandler) {
	d := &Delegate{sh,h}
	cfg.Delegate = d
	cfg.Events = d
	cfg.Alive = d
	cfg.Merge = d
}
