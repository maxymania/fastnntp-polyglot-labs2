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


package cluster

import "fmt"
import "net"
import "bytes"
import "sync"
//import "github.com/maxymania/fastnntp-polyglot-labs2/bucketstore"
import "github.com/maxymania/fastnntp-polyglot-labs2/bucketstore/globmap"
import "github.com/maxymania/fastnntp-polyglot-labs2/bucketstore/bucketmap"
import "github.com/maxymania/fastnntp-polyglot-labs2/bucketstore/netkv"
import "github.com/maxymania/fastnntp-polyglot-labs2/bucketstore/healthmap"
import "github.com/vmihailenco/msgpack"
import "github.com/byte-mug/golibs/msgpackx"
import "github.com/hashicorp/memberlist"
import "sort"

const (
	// &Command{}
	CmdAdd uint = iota
	CmdSub
	
	// &NetKvStore{}
	NetAdd
	NetRem
	
	// &HealthEvent{}
	PutHealth
)

type HealthEvent healthmap.Health
func (c HealthEvent) Invalidates(b memberlist.Broadcast) bool {
	switch o := b.(type) {
	case *HealthEvent: return string(c.Name)==string(o.Name)
	case HealthEvent: return string(c.Name)==string(o.Name)
	}
	return false
}
func (c HealthEvent) Message() []byte {
	b,_ := msgpackx.Marshal(PutHealth,c.Name,c.Writable)
	return b
}
func (c HealthEvent) Finished() {}

type NetKvStore struct{
	// NetAdd,NetRem
	Op     uint
	Prov,Loc string
	Bucket,Meta []byte
}
func (c *NetKvStore) Invalidates(b memberlist.Broadcast) bool {
	switch o := b.(type) {
	case *NetKvStore:
		if string(c.Bucket)!=string(o.Bucket) { return false }
		if c.Loc!=o.Loc { return false }
		return c.Op!=c.Op
	}
	return false
}
func (c *NetKvStore) Message() []byte {
	b,_ := msgpackx.Marshal(c.Op,c.Loc,c.Prov,c.Bucket,c.Meta)
	return b
}
func (c *NetKvStore) Finished() {}

type Command struct{
	Op     uint
	Node   string
	Bucket []byte
}

func (c *Command) Invalidates(b memberlist.Broadcast) bool {
	switch o := b.(type) {
	case *Command:
		if string(c.Bucket)!=string(o.Bucket) { return false }
		return c.Op!=c.Op
	}
	return false
}
func (c *Command) Message() []byte {
	b,_ := msgpackx.Marshal(c.Op,c.Node,c.Bucket)
	return b
}
func (c *Command) Finished() {}

const Magic uint = 0xcafedead

type DistanceMeter interface{
	Distance(l,r string) int
}
type simplistic struct{}
func (simplistic) Distance(l,r string) int {
	if l==r { return 1 }
	return 0
}

type nkvm map[string]*NetKvStore

type Metadata struct{
	Loc string `msgpack:loc`
	Port int   `msgpack:rpc`
}
func (m *Metadata) String() string {
	if m==nil{ return "{}" }
	return fmt.Sprint("{loc:",m.Loc,",rpc:",m.Port,"}")
}
type NodeMetadata struct{
	Name string
	IP   net.IP
	Metadata
	distance int
}
func (m *NodeMetadata) String() string {
	return fmt.Sprintf("%q/%v%v",m.Name,m.IP,&m.Metadata)
}

func cloneip(i net.IP) net.IP {
	return append(make(net.IP,0,len(i)),i...)
}

type mdmap map[string]*NodeMetadata

type Deleg struct{
	Self string
	Meta *Metadata
	TLQ memberlist.TransmitLimitedQueue
	ML  *memberlist.Memberlist
	NM  globmap.NodeMap
	BM  bucketmap.BucketMap
	HM  healthmap.HealthMap
	NKV netkv.NetKVMap
	
	Dist DistanceMeter
	
	nkvL    sync.RWMutex
	nkvAdd nkvm
	nkvRem nkvm
	
	othersL sync.RWMutex
	othersM mdmap
	
	leaveListener []func(string)
	joinListener []func(string)
}
func (d *Deleg) AddLeaveListener(f func(string)){
	d.leaveListener = append(d.leaveListener,f)
}
func (d *Deleg) AddJoinListener(f func(string)){
	d.joinListener = append(d.joinListener,f)
}
func (d *Deleg) SortNodesDistance(s []*NodeMetadata) {
	dist := d.Dist
	if dist==nil { dist = simplistic{} }
	for i := range s { s[i].distance = dist.Distance(s[i].Loc,d.Meta.Loc) }
	sort.Slice(s,func(i,j int) bool { return s[i].distance < s[j].distance })
}
func (d *Deleg) numNodes() int {
	if ml := d.ML; ml!=nil {
		return ml.NumMembers()
	}
	return 1
}
func (d *Deleg) Init() {
	d.TLQ.NumNodes = d.numNodes
	d.TLQ.RetransmitMult = 1
	d.NM.Init()
	d.BM.Init()
	d.HM.Init()
	d.NKV.Init()
	d.nkvAdd = make(nkvm)
	d.nkvRem = make(nkvm)
	d.othersM = make(mdmap)
}
func (d *Deleg) NodeMeta(limit int) []byte {
	m := d.Meta
	if m==nil { return nil }
	data,_ := msgpackx.Marshal(Magic,m)
	return data
}
func (d *Deleg) loc() string {
	m := d.Meta
	if m==nil { return "" }
	return m.Loc
}
func (d *Deleg) getNetKvStoreEncoded(buf *bytes.Buffer) {
	d.nkvL.RLock(); defer d.nkvL.RUnlock()
	for _,v := range d.nkvAdd { buf.Write(v.Message()) }
	for _,v := range d.nkvRem { buf.Write(v.Message()) }
}
func (d *Deleg) handleNetKvStore(n *NetKvStore) {
	if !(n.Loc=="" || n.Loc==d.loc()) { return } // If out of loc, ignore.
	
	switch n.Op {
	case NetAdd:
		if !d.NKV.Offer(n.Prov,n.Bucket,n.Meta,n.Loc=="") { return }
		if n.Loc!="" { d.TLQ.QueueBroadcast(&Command{CmdAdd,d.Self,n.Bucket}) }
	case NetRem:
		d.NKV.Remove(n.Bucket)
		if n.Loc!="" { d.TLQ.QueueBroadcast(&Command{CmdSub,d.Self,n.Bucket}) }
	}
	
	d.nkvL.Lock(); defer d.nkvL.Unlock()
	switch n.Op {
	case NetAdd:
		delete(d.nkvRem,string(n.Bucket))
		d.nkvAdd[string(n.Bucket)] = n
	case NetRem:
		delete(d.nkvAdd,string(n.Bucket))
		d.nkvRem[string(n.Bucket)] = n
	}
}

func (d *Deleg) NotifyMsg(msg []byte) {
	var op uint
	var node string
	var buck []byte
	dec := msgpack.NewDecoder(bytes.NewReader(msg))
	for{
		if dec.Decode(&op)!=nil { return }
		switch op {
		case CmdAdd,CmdSub:
			if dec.DecodeMulti(&node,&buck)!=nil { return }
			switch op {
			case CmdAdd: d.NM.Set(node,string(buck))
			case CmdSub: d.NM.Drop(node,string(buck))
			}
		case NetAdd,NetRem:
			{
				c := new(NetKvStore)
				c.Op = op
				if dec.DecodeMulti(&c.Loc,&c.Prov,&c.Bucket,&c.Meta)!=nil { return }
				d.handleNetKvStore(c)
			}
		case PutHealth:
			{
				c := new(HealthEvent)
				if dec.DecodeMulti(&c.Name,&c.Writable)!=nil { return }
				d.HM.SetWritable(c.Name,c.Writable)
			}
		}
	}
}
func (d *Deleg) GetBroadcasts(overhead, limit int) [][]byte { return d.TLQ.GetBroadcasts(overhead,limit) }
func (  *Deleg) LocalState(join bool) []byte { return nil }
func (  *Deleg) MergeRemoteState(buf []byte, join bool) { }
func (d *Deleg) onNode(n *memberlist.Node) {
	var u uint
	m := new(NodeMetadata)
	err := msgpackx.Unmarshal(n.Meta,&u,&m.Metadata)
	if err!=nil { return }
	m.Name = n.Name
	m.IP   = cloneip(n.Addr)
	d.othersL.Lock(); defer d.othersL.Unlock()
	d.othersM[n.Name] = m
}
func (d *Deleg) NotifyJoin(n *memberlist.Node) {
	if ml := d.ML; ml!=nil {
		var buf bytes.Buffer
		enc := msgpack.NewEncoder(&buf)
		for _,e := range d.BM.ListupRaw() {
			enc.EncodeMulti(CmdAdd,d.Self,[]byte(e))
		}
		for _,e := range d.NKV.GetLocalBuckets() {
			enc.EncodeMulti(CmdAdd,d.Self,[]byte(e))
		}
		for _,e := range d.HM.GetAll() {
			enc.EncodeMulti(PutHealth,e.Name,e.Writable)
		}
		d.getNetKvStoreEncoded(&buf)
		if buf.Len()>0 {
			go ml.SendReliable(n, buf.Bytes())
		}
	}
	d.onNode(n)
	for _,f := range d.joinListener { f(n.Name) }
}
func (d *Deleg) NotifyLeave(n *memberlist.Node) {
	d.NM.DropNode(n.Name)
	for _,f := range d.leaveListener { f(n.Name) }
}
func (d *Deleg) NotifyUpdate(n *memberlist.Node) {
	d.onNode(n)
}
func (d *Deleg) NotifyAlive(peer *memberlist.Node) error {
	return d.NotifyMerge([]*memberlist.Node{peer})
}
func (d *Deleg) NotifyMerge(peers []*memberlist.Node) error {
	var u uint
	var m Metadata
	for _,peer := range peers {
		err := msgpackx.Unmarshal(peer.Meta,&u,&m)
		if err!=nil { return err }
		if u!=Magic { return fmt.Errorf("wrong magic number: got %x expected %x",u,Magic)}
	}
	return nil
}


// ----
func (d *Deleg) GetAll(nodes []string,appndTo []*NodeMetadata) []*NodeMetadata {
	d.othersL.RLock(); defer d.othersL.RUnlock()
	for _,node := range nodes {
		appndTo = append(appndTo,d.othersM[node])
	}
	return appndTo
}
func (d *Deleg) GetOne(node string) *NodeMetadata {
	d.othersL.RLock(); defer d.othersL.RUnlock()
	return d.othersM[node]
}

// After calling, the 'name' array must no be modified.
func (d *Deleg) AddBucket(name []byte,buck bucketmap.Bucket) {
	d.NM.Set(d.Self,string(name))
	d.BM.Add(name,buck)
	d.TLQ.QueueBroadcast(&Command{CmdAdd,d.Self,name})
}

// After calling, the 'name' array must no be modified.
func (d *Deleg) DeleteBucket(name []byte) {
	d.NM.Drop(d.Self,string(name))
	d.BM.Remove(name)
	d.TLQ.QueueBroadcast(&Command{CmdSub,d.Self,name})
}
// After calling, the *NetKvStore data structure and all buffers used by it must not be used.
func (d *Deleg) OfferNetKvStore(n *NetKvStore) {
	d.handleNetKvStore(n)
	d.TLQ.QueueBroadcast(n)
}
// ----
func (d *Deleg) IssueHealth(h healthmap.Health) {
	d.HM.SetWritable(h.Name,h.Writable)
	d.TLQ.QueueBroadcast(HealthEvent(h))
}
var _ healthmap.HealthReceiver = (*Deleg)(nil)
