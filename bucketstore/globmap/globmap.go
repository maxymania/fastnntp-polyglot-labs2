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


package globmap

import "github.com/maxymania/fastnntp-polyglot-labs2/bucketstore"
import "sync"

type Bucket struct {
	Reader bucketstore.BucketR
	Writer bucketstore.BucketW
	WriterEx bucketstore.BucketWEx
}

func find(e string,s []string) bool{
	for _,o := range s { if e==o { return true } }
	return false
}
func add(e string,s []string) []string{
	for _,o := range s { if e==o { return s } }
	return append(s,e)
}
func rem(e string,s []string) []string{
	for i,o := range s { if e==o { return append(s[:i],s[i+1:]...) } }
	return s
}

func addi(k string,e string,sm map[string][]string) {
	s := sm[k]
	for _,o := range s { if e==o { return } }
	sm[k] = append(s,e)
}
func remi(k string,e string,sm map[string][]string) {
	s := sm[k]
	for i,o := range s {
		if e==o {
			if len(s)==1 { delete(sm,k) } else { sm[k] = append(s[:i],s[i+1:]...) }
			return
		}
	}
}
func clonesp(s []string) []string {
	if len(s)==0 { return nil } /* Fast path. */
	return append(make([]string,0,len(s)),s...)
}

type nodemap map[string][]string

type NodeMap struct{
	n2b,b2n nodemap
	access sync.RWMutex
}
func (n *NodeMap) Init() {
	n.n2b = make(nodemap)
	n.b2n = make(nodemap)
}
func (n *NodeMap) Buckets(node string) []string {
	n.access.RLock(); defer n.access.RUnlock()
	return clonesp(n.n2b[node])
}
func (n *NodeMap) Nodes(bucket string) []string {
	n.access.RLock(); defer n.access.RUnlock()
	return clonesp(n.b2n[bucket])
}
func (n *NodeMap) Set(node,bucket string) {
	n.access.Lock(); defer n.access.Unlock()
	addi(node,bucket,n.n2b)
	addi(bucket,node,n.b2n)
}
func (n *NodeMap) Drop(node,bucket string) {
	n.access.Lock(); defer n.access.Unlock()
	remi(node,bucket,n.n2b)
	remi(bucket,node,n.b2n)
}
func (n *NodeMap) DropNode(node string) {
	n.access.Lock(); defer n.access.Unlock()
	for _,bucket := range n.n2b[node] {
		remi(bucket,node,n.b2n)
	}
	delete(n.n2b,node)
}
func (n *NodeMap) DropBucket(bucket string) {
	n.access.Lock(); defer n.access.Unlock()
	for _,node := range n.b2n[bucket] {
		remi(node,bucket,n.n2b)
	}
	delete(n.b2n,bucket)
}

