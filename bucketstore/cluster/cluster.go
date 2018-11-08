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
import "github.com/vmihailenco/msgpack"
import "github.com/byte-mug/golibs/msgpackx"
import "github.com/hashicorp/memberlist"

const (
	CmdAdd uint = iota
	CmdSub
)

type Command struct{
	Op     uint
	Node   string
	Bucket []byte
}

func (c *Command) Invalidates(b memberlist.Broadcast) bool { return false }
func (c *Command) Message() []byte {
	b,_ := msgpackx.Marshal(c.Op,c.Node,c.Bucket)
	return b
}
func (c *Command) Finished() {}

const Magic uint = 0xcafedead

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
	
	othersL sync.RWMutex
	othersM mdmap
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
	d.othersM = make(mdmap)
}
func (d *Deleg) NodeMeta(limit int) []byte {
	m := d.Meta
	if m==nil { return nil }
	data,_ := msgpackx.Marshal(Magic,m)
	return data
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
		if buf.Len()>0 {
			go ml.SendReliable(n, buf.Bytes())
		}
	}
	d.onNode(n)
}
func (d *Deleg) NotifyLeave(n *memberlist.Node) {
	d.NM.DropNode(n.Name)
}
func (d *Deleg) NotifyUpdate(n *memberlist.Node) {
	d.onNode(n)
}

func (d *Deleg) GetAll(nodes []string,appndTo []*NodeMetadata) []*NodeMetadata {
	d.othersL.RLock(); defer d.othersL.RUnlock()
	for _,node := range nodes {
		appndTo = append(appndTo,d.othersM[node])
	}
	return appndTo
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

