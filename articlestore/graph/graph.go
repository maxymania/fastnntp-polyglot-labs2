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


package graph

import "github.com/maxymania/fastnntp-polyglot-labs2/articlestore"
import "github.com/maxymania/fastnntp-polyglot-labs/bufferex"
import "github.com/maxymania/gonbase/hashring"
import "github.com/maxymania/gonbase/hashring/dhash"

import avl "github.com/emirpasic/gods/trees/avltree"
import "github.com/emirpasic/gods/utils"
import "fmt"

type store struct {
	id,msg []byte
	expire uint64
}
func (s store) Mutate(hash uint64, obj interface{}) bool {
	return obj.(articlestore.StorageW).StoreWriteMessage(s.id,s.msg,s.expire)==nil
}
type loader struct {
	id []byte
	over,head,body bool
	b bufferex.Binary
	e error
}
func (l *loader) Mutate(hash uint64, obj interface{}) bool {
	l.b.Free()
	l.b,l.e = obj.(articlestore.StorageR).StoreReadMessage(l.id,l.over,l.head,l.body)
	return (l.e)==nil
}

type unimpl struct{}
func (unimpl) StoreReadMessage(id []byte, over, head, body bool) (bufferex.Binary, error) { return bufferex.Binary{}, articlestore.EFail{} }
func (unimpl) StoreWriteMessage(id, msg []byte, expire uint64) error { return articlestore.EFail{} }

/* A None-Object. */
var Unimpl articlestore.Storage = unimpl{}

type Storage struct {
	articlestore.Storage
	Class uint
}
func (s *Storage) Set(obj articlestore.Storage,cls uint) {
	if s.Storage==nil || s.Storage==Unimpl || s.Class<cls {
		s.Storage = obj
		s.Class   = cls
	}
}
func (s *Storage) Unset() {
	s.Storage = Unimpl
}


type Ring struct {
	R hashring.IHashRing
}
func (r *Ring) StoreReadMessage(id []byte, over, head, body bool) (bufferex.Binary, error) {
	l := new(loader)
	l.id,l.over,l.head,l.body = id,over,head,body
	r.R.MutateStore(id,l)
	return l.b,l.e
}
func (r *Ring) StoreWriteMessage(id, msg []byte, expire uint64) error {
	if r.R.MutateStore(id,store{id,msg,expire}) { return nil }
	return articlestore.EFail{}
}

func (r *Ring) Configure(cfg *CfgRing, st map[string]*Storage) {
	switch cfg.Type {
	case "JUMP":
		msr := new(hashring.MultiJumpSeedRing64)
		msr.Init()
		msr.Seed = cfg.Seed
		msr.N    = cfg.Num
		r.R = msr
	case "STEP":
		msr := new(hashring.MultiStepSeedRing64)
		msr.Init()
		msr.Seed = cfg.Seed
		msr.N    = cfg.Num
		r.R = msr
	default:
		msr := new(hashring.SeedRing64)
		msr.Init()
		msr.Seed = cfg.Seed
		r.R = msr
	}
	for _,shard := range cfg.Shard {
		str := &Storage{Unimpl,0}
		r.R.AddNode(dhash.Hash64([]byte(shard)),str)
		st[shard] = str
	}
}

type StorageMM map[string]map[string]*Storage

func (l1 StorageMM) Walk(k1,k2 string) (p *Storage) {
	l2,ok := l1[k1]
	if !ok {
		p = new(Storage)
		l1[k1] = map[string]*Storage{k2:p}
		return
	}
	p,ok = l2[k2]
	if !ok {
		p = new(Storage)
		l2[k2] = p
	}
	return
}
func (l1 StorageMM) RWalk(k1,k2 string) (p *Storage) {
	var l2 map[string]*Storage
	if l1!=nil {
		l2 = l1[k1]
	}
	if l2!=nil {
		p  = l2[k2]
	}
	return
}



type RingSet struct {
	Rings []Ring
	Trees *avl.Tree
}
func (r *RingSet) x() {
	r.Trees = avl.NewWith(utils.Float64Comparator)
}
func (r *RingSet) performRead(key []byte, m hashring.Mutator) bool {
	for l := r.Trees.Left(); l!=nil ; l = l.Next() {
		if l.Value.(Ring).R.MutateStore(key,m) { return true }
	}
	return false
}
func (r *RingSet) performWrite(key []byte, m hashring.Mutator) bool {
	for _,ring := range r.Rings {
		if ring.R.MutateStore(key,m) { return true }
	}
	return false
}
func (r *RingSet) Configure(cfg *CfgConfig, stt StorageMM) {
	var ring Ring
	r.Trees = avl.NewWith(utils.Float64Comparator)
	for _,cring := range cfg.Rings {
		st := make(map[string]*Storage)
		ring.Configure(&cring,st)
		r.Rings = append(r.Rings,ring)
		r.Trees.Put(cring.Prio,ring)
		stt[cring.Ring]=st
	}
}
func (r *RingSet) StoreReadMessage(id []byte, over, head, body bool) (bufferex.Binary, error) {
	l := new(loader)
	l.id,l.over,l.head,l.body = id,over,head,body
	r.performRead(id,l)
	return l.b,l.e
}
func (r *RingSet) StoreWriteMessage(id, msg []byte, expire uint64) error {
	if r.performWrite(id,store{id,msg,expire}) { return nil }
	return articlestore.EFail{}
}
func (r *RingSet) String() string { return fmt.Sprintf("{%v\n%v\n}",r.Rings,r.Trees) }


