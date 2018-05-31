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


package overdb

import "github.com/maxymania/fastnntp-polyglot-labs2/groupover"
import bolt "github.com/coreos/bbolt"
import "sync/atomic"
import "sync"
import "os"
import "io"
import "fmt"


import (
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/syndtr/goleveldb/leveldb/cache"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/syndtr/goleveldb/leveldb/table"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type snapshot struct {
	c uint64
	r *rostore
	w *wrstore
}
func (s *snapshot) free() {
	if atomic.AddUint64(&s.c,^uint64(0))!=0 { return }
	if s.r!=nil { s.r.tr.Release() }
}
func (s *snapshot) GroupOverviewList(targ func(group []byte, statusAndDescr []byte)) bool {
	if s.r!=nil { return s.r.GroupOverviewList(targ) }
	if s.w!=nil { return s.w.GroupOverviewList(targ) }
	return false
}

func (s *snapshot) GroupOverviewGet(group []byte, buffer []byte) (statusAndDescr []byte) {
	if s.r!=nil { return s.r.GroupOverviewGet(group, buffer) }
	if s.w!=nil { return s.w.GroupOverviewGet(group, buffer) }
	return
}

var (
	gTable = []byte("table")
	gPrefs = []byte("prefs")
/* ------------------------------------------------------------ */
	pState = []byte("state")
	pSSTab = []byte("sstable")
)

type DB struct{
	
	PPF string
	Opts *opt.Options
	db *bolt.DB
	
	mu sync.RWMutex
	sn *snapshot
	
	ch *cache.Cache
	bp *util.BufferPool
	
	updater sync.RWMutex
}

func (db *DB) Init() (e error) {
	db.db,e = bolt.Open(db.PPF+".live.db",0600,nil)
	if e!=nil { return }
	db.ch = cache.NewCache(cache.NewLRU(1024))
	db.bp = util.NewBufferPool(1<<14)
	db.loadCurrent()
	return
}

func (db *DB) openSST(u uint64) (*snapshot,error) {
	f,err := os.Open(db.PPF+fmt.Sprintf(".%d.sst",u))
	if err!=nil { return nil,err }
	s,err := f.Stat()
	if err!=nil { f.Close(); return nil,err }
	tr,err := table.NewReader(f, s.Size(), storage.FileDesc{}, &cache.NamespaceGetter{db.ch,u}, db.bp, db.Opts)
	if err!=nil { f.Close(); return nil,err }
	return &snapshot{c:1,r:&rostore{tr}},nil
}

func (db *DB) loadCurrent() {
	dok := false
	db.db.View(func(t *bolt.Tx) error {
		bkt := t.Bucket(gPrefs)
		if bkt==nil { return nil }
		u := decode(bkt.Get(pSSTab))
		st := bkt.Get(pState)
		switch string(st) {
		case "imported":
			dok = true
			fallthrough
		case "exported":
			sn,_ := db.openSST(u)
			db.sn = sn
		}
		return nil
	})
	if db.sn==nil {
		if dok {
			db.db.Update(func(t *bolt.Tx) error {
				bkt := t.Bucket(gPrefs)
				bkt.Put(pState,[]byte("internal"))
				return nil
			})
		}
		db.sn = &snapshot{c:1,w:&wrstore{db.db,gTable}}
	}
}

func (db *DB) current() *snapshot {
	db.mu.RLock()
	defer db.mu.RUnlock()
	atomic.AddUint64(&db.sn.c,1)
	return db.sn
}

func (db *DB) update() (sync.Locker,*snapshot) {
restart:
	db.updater.RLock()
	sn := db.current()
	if sn.w!=nil { return db.updater.RLocker(),sn }
	sn.free()
	db.updater.RUnlock()
	db.updater.Lock()
	
	sn = db.sn
	if sn.w!=nil {
		db.updater.Unlock()
		goto restart
	}
	
	/* Lemma: sn.w==nil && sn.r!=nil */
	
	db.db.Update(func(t *bolt.Tx) error {
		bkt := t.Bucket(gPrefs)
		st := bkt.Get(pState)
		
		if string(st)=="imported" {
			p := t.Bucket(iPairs)
			sn.GroupOverviewList(func(group []byte, statusAndDescr []byte){
				p.Put(group,statusAndDescr)
			})
		}
		
		bkt.Put(pState,[]byte("internal"))
		return nil
	})
	
	rsn := &snapshot{c:1,w:&wrstore{db.db,gTable}}
	db.mu.Lock()
	db.sn = rsn
	defer db.mu.Unlock()
	sn.free()
	return &db.updater,rsn
}

func (db *DB) GroupOverviewList(targ func(group []byte, statusAndDescr []byte)) bool {
	s := db.current()
	defer s.free()
	return s.GroupOverviewList(targ)
}

func (db *DB) GroupOverviewGet(group []byte, buffer []byte) (statusAndDescr []byte) {
	s := db.current()
	defer s.free()
	return s.GroupOverviewGet(group,buffer)
}
var _ groupover.GroupOverview = (*DB)(nil)

func (db *DB) GroupOverviewSetStatus(group []byte, status byte) error {
	l,s := db.update()
	defer l.Unlock()
	defer s.free()
	return s.w.GroupOverviewSetStatus(group,status)
}
func (db *DB) GroupOverviewSetDescr(group []byte, descr []byte) error {
	l,s := db.update()
	defer l.Unlock()
	defer s.free()
	return s.w.GroupOverviewSetDescr(group,descr)
}

var _ groupover.GroupOverviewUpdater = (*DB)(nil)

func (db *DB) GroupOverviewBatchUpdate(targ func(groupover.GroupOverviewUpdater)error) error {
	l,s := db.update()
	defer l.Unlock()
	defer s.free()
	return s.w.GroupOverviewBatchUpdate(targ)
}
var _ groupover.GroupOverviewBatchUpdater = (*DB)(nil)

func (db *DB) Export() (string,error) {
	db.updater.Lock()
	defer db.updater.Unlock()
	
	sn := db.sn
	
	var u uint64
	exported := false
	db.db.View(func(t *bolt.Tx) error {
		bkt := t.Bucket(gPrefs)
		if bkt==nil { return nil }
		u = decode(bkt.Get(pSSTab))
		st := bkt.Get(pState)
		switch string(st) {
		case "imported","exported":
			exported = true
		}
		return nil
	})
	
	if exported { return db.PPF+fmt.Sprintf(".%d.sst",u),nil }
	
	err := db.db.Update(func(t *bolt.Tx) error {
		bkt,err := t.CreateBucketIfNotExists(gPrefs)
		if err!=nil { return err }
		s,err := bkt.NextSequence()
		if err!=nil { return err }
		if u==s {
			u,err = bkt.NextSequence()
			if err!=nil { return err }
		} else {
			u = s
		}
		return bkt.Put(pSSTab,encode(u))
	})
	
	if err!=nil { return "",err }
	
	path := db.PPF+fmt.Sprintf(".%d.sst",u)
	f,err := os.Create(path)
	if err!=nil { return "",err }
	w := table.NewWriter(f,db.Opts)
	
	sn.GroupOverviewList(func(group []byte, statusAndDescr []byte) {
		if err!=nil { return }
		err = w.Append(group, statusAndDescr)
	})
	w.Close()
	f.Close()
	if err!=nil {
		os.Remove(path)
		return "",err
	}
	
	rsn,err := db.openSST(u)
	if err!=nil {
		os.Remove(path)
		return "",err
	}
	
	err = db.db.Update(func(t *bolt.Tx) error {
		bkt := t.Bucket(gPrefs)
		
		bkt.Put(pState,[]byte("exported"))
		return nil
	})
	
	if err!=nil {
		rsn.free()
		os.Remove(path)
		return "",err
	}
	
	db.mu.Lock()
	db.sn = rsn
	defer db.mu.Unlock()
	sn.free()
	
	return path,err
}

func (db *DB) Import(r io.Reader) error {
	db.updater.Lock()
	defer db.updater.Unlock()
	
	ipath := db.PPF+".import.sst"
	{
		f,err := os.Create(ipath)
		if err!=nil { return err }
		_,err = io.Copy(f,r)
		f.Close()
		if err!=nil { return err }
	}
	
	var rsn *snapshot
	err := db.db.Update(func(t *bolt.Tx) error {
		bkt,err := t.CreateBucketIfNotExists(gPrefs)
		if err!=nil { return err }
		u := decode(bkt.Get(pSSTab))
		s,err := bkt.NextSequence()
		if err!=nil { return err }
		if u==s {
			s,err = bkt.NextSequence()
			if err!=nil { return err }
		}
		
		path := db.PPF+fmt.Sprintf(".%d.sst",s)
		err = os.Rename(ipath,path)
		
		if err!=nil { os.Remove(ipath) ; return err }
		
		rsn,err = db.openSST(s)
		
		if err!=nil { os.Remove(path) ; return err }
		
		bkt.Put(pState,[]byte("imported"))
		return bkt.Put(pSSTab,encode(s))
	})
	if err!=nil { return err }
	
	sn := db.sn
	db.mu.Lock()
	db.sn = rsn
	defer db.mu.Unlock()
	sn.free()
	
	return nil
}


