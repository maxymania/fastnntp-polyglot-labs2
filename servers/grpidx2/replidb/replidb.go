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

package grpidx

import . "github.com/maxymania/fastnntp-polyglot-labs2/servers/grpidx2"
import "github.com/maxymania/fastnntp-polyglot-labs2/groupidx"

import bolt "github.com/coreos/bbolt" 
import "github.com/maxymania/gonbase/replidb"
import "github.com/maxymania/fastnntp-polyglot-labs2/grouphead/mockup"
import "github.com/maxymania/fastnntp-polyglot-labs2/grouphead/repliseq"

import "path/filepath"
import "os"
import "strings"

func openLoggedSequence(s string,sto *Storage) (*replidb.LoggedSequence,error) {
	db,err := bolt.Open(filepath.Join(s,"Sequence"),0600,nil)
	if err!=nil { return nil,err }
	
	isq := &replidb.Sequence{
		DB: db,
		Table: []byte("Data"), 
	}
	
	isq.Init()
	
	sq := &replidb.LoggedSequence{
		Seq: isq,
		Path: s,
		MaxLogBeforeMerge: sto.LogSizeFlush.Int64(),
	}
	
	err = sq.Startup()
	if err!=nil { return nil,err }
	return sq,nil
	
}

var osPS = string([]rune{os.PathSeparator})

func storageGroupdb (s *Storage) (groupidx.GroupIndex,error) {
	seq,err := openLoggedSequence(strings.Replace(s.Location,"/",osPS,-1),s)
	if err!=nil { return nil,err }
	swrk := &replidb.SequenceWorker{LS: seq}
	swrk.Start()
	repli := &repliseq.RepliDB{swrk}
	return mockup.Wrapper{repli},nil
}

func init() {
	Register("replidb",storageGroupdb)
}

