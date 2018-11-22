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


package chybrid

import "github.com/gocql/gocql"
import "github.com/maxymania/fastnntp-polyglot/buffer"
import "github.com/maxymania/fastnntp-polyglot-labs2/articlestore"
import "github.com/maxymania/fastnntp-polyglot-labs2/bucketstore/netmodel"
import "github.com/maxymania/fastnntp-polyglot-labs2/bucketstore/selerr"

import "time"

type BucketSched interface{
	NextBucket() ([]byte,bool)
}
type FastLookup interface{
	FastLookup(bucket []byte) (srv netmodel.Server, rok bool)
}

type StoreWriter struct {
	Sched BucketSched
	Flook FastLookup
	Session *gocql.Session
	UseFastOver bool
}

func (s *StoreWriter) StoreWriteMessage(id, msg []byte, expire uint64) (err error) {
	bkt,ok := s.Sched.NextBucket(); if !ok { return articlestore.VEFail }
	srv,ok := s.Flook.FastLookup(bkt); if !ok { return articlestore.VEFail }
	xover,head,body := articlestore.UnpackMessage(msg)
	var nxover []byte
	
	idb,bts := extend(id)
	defer idb.Free()
	
	rid := gocql.TimeUUID()
	
	var ide []byte
	{
		buf := buffer.Get(len(id)+1)
		defer buffer.Put(buf)
		ide = (*buf)[:len(id)+1]
	}
	copy(ide,id)
	
	if s.UseFastOver { nxover,xover = xover,nxover }
	
	if srv.WriterEx!=nil {
		/*
		If the bucket sits in a remote node, srv.WriterEx is set with an
		implementation of bucketstore.BucketWEx (which is a *kvrpc.Client),
		which's BucketPutExpire() method will always fail. We catch that
		error case by checking the returned error for being a
		selerr.TNotImplemented instance.
		
		We write the header first, since it is non-optional like xover
		and we use the first performed write for our check, so it must
		not be optional.
		*/
		*bts='h'
		err = srv.WriterEx.BucketPutExpire(bkt,idb.Bytes(),head,expire)
		if _,ok := err.(selerr.TNotImplemented); ok { goto onWriter }
		if err!=nil { return }
		
		*bts='x'
		if len(xover)>0 { err = srv.WriterEx.BucketPutExpire(bkt,idb.Bytes(),xover,expire) }
		if err!=nil { return }
		
		*bts='b'
		err = srv.WriterEx.BucketPutExpire(bkt,idb.Bytes(),body,expire)
		if err!=nil { return }
		
		secs := int64(time.Until(time.Unix(int64(expire),0))/time.Second)+1
		
		err = s.Session.Query(`
		INSERT INTO article_locs
		 (messageid,recid,avail,xover ,bucket,keep,exp   ) VALUES
		 (?        ,?    ,true ,?     ,?     ,true,?     ) USING TTL ?
		`,id       ,rid        ,nxover,bkt        ,expire           ,secs).Exec()
		return
	}
	onWriter:
	if srv.Writer!=nil {
		err = s.Session.Query(`
		INSERT INTO article_locs
		 (messageid,recid,bucket,exp   ) VALUES
		 (?        ,?    ,?     ,?     )
		`,id       ,rid  ,bkt   ,expire).Exec()
		if err!=nil { return }
		
		*bts='x'
		if len(xover)>0 { err = srv.Writer.BucketPut(bkt,idb.Bytes(),xover) }
		if err!=nil { return }
		
		*bts='h'
		err = srv.Writer.BucketPut(bkt,idb.Bytes(),head)
		if err!=nil { return }
		
		*bts='b'
		err = srv.Writer.BucketPut(bkt,idb.Bytes(),body)
		if err!=nil { return }
		
		secs := int64(time.Until(time.Unix(int64(expire),0))/time.Second)+1
		
		err = s.Session.Query(`
		INSERT INTO article_locs
		 (messageid,recid,xover ,avail,keep) VALUES
		 (?        ,?    ,?     ,true ,true) USING TTL ?
		`,id       ,rid  ,nxover                      ,secs).Exec()
		return
	}
	return articlestore.VEFail
}
var _ articlestore.StorageW = (*StoreWriter)(nil)

