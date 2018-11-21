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
//import "github.com/maxymania/fastnntp-polyglot-labs/bufferex"

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

func (s *StoreWriter) StoreWriteMessage(id, msg []byte, expire uint64) error {
	bkt,ok := s.Sched.NextBucket(); if !ok { return articlestore.VEFail }
	srv,ok := s.Flook.FastLookup(bkt); if !ok { return articlestore.VEFail }
	xover,head,body := articlestore.UnpackMessage(msg)
	
	rid := gocql.TimeUUID()
	secs := int64(time.Until(time.Unix(int64(expire),0))/time.Second) + 1
	var err error
	
	var ide []byte
	{
		buf := buffer.Get(len(id)+1)
		defer buffer.Put(buf)
		ide = (*buf)[:len(id)+1]
	}
	copy(ide,id)
	
	switch {
	case srv.WriterEx!=nil && s.UseFastOver:
		ide[len(id)]='h'
		err = srv.WriterEx.BucketPutExpire(bkt,ide,head,expire)
		if err!=nil { return err }
		ide[len(id)]='b'
		err = srv.WriterEx.BucketPutExpire(bkt,ide,body,expire)
		if err!=nil { return err }
		err = s.Session.Query(`
		INSERT INTO article_locs
		 (messageid,recid,tmbmb,xover,hbkt,bbkt,hbkp,bbkp) VALUES
		 (?        ,?    ,true ,?    ,?   ,?   ,true,true) USING TTL ?
		`,id       ,rid        ,xover,bkt  ,bkt                      ,secs).Exec()
		if err!=nil { return err }
	case srv.WriterEx!=nil:
		ide[len(id)]='x'
		err = srv.WriterEx.BucketPutExpire(bkt,ide,xover,expire)
		if err!=nil { return err }
		
		ide[len(id)]='h'
		err = srv.WriterEx.BucketPutExpire(bkt,ide,head,expire)
		if err!=nil { return err }
		
		ide[len(id)]='b'
		err = srv.WriterEx.BucketPutExpire(bkt,ide,body,expire)
		if err!=nil { return err }
		
		err = s.Session.Query(`
		INSERT INTO article_locs
		 (messageid,recid,tmbmb,obkt,hbkt,bbkt,obkp,hbkp,bbkp) VALUES
		 (?        ,?    ,true ,?   ,?   ,?   ,true,true,true) USING TTL ?
		`,id       ,rid        ,bkt ,bkt ,bkt                           ,secs).Exec()
		if err!=nil { return err }
	case srv.Writer!=nil  && s.UseFastOver:
		err = s.Session.Query(`
		INSERT INTO article_locs
		 (messageid,recid,hbkt,bbkt) VALUES
		 (?        ,?    ,?   ,?)
		`,id       ,rid  ,bkt ,bkt).Exec()
		if err!=nil { return err }
		
		ide[len(id)]='h'
		err = srv.Writer.BucketPut(bkt,ide,head)
		if err!=nil { return err }
		
		ide[len(id)]='b'
		err = srv.Writer.BucketPut(bkt,ide,body)
		if err!=nil { return err }
		
		err = s.Session.Query(`
		INSERT INTO article_locs
		 (messageid,recid,tmbmb,xover,hbkp,bbkp) VALUES
		 (?        ,?    ,true ,?    ,true,true) USING TTL ?
		`,id       ,rid        ,xover                      ,secs).Exec()
		if err!=nil { return err }
	case srv.Writer!=nil:
		err = s.Session.Query(`
		INSERT INTO article_locs
		 (messageid,recid,obkt,hbkt,bbkt) VALUES
		 (?        ,?    ,?   ,?   ,?)
		`,id       ,rid  ,bkt ,bkt ,bkt).Exec()
		if err!=nil { return err }
		
		ide[len(id)]='x'
		err = srv.Writer.BucketPut(bkt,ide,xover)
		if err!=nil { return err }
		
		ide[len(id)]='h'
		err = srv.Writer.BucketPut(bkt,ide,head)
		if err!=nil { return err }
		
		ide[len(id)]='b'
		err = srv.Writer.BucketPut(bkt,ide,body)
		if err!=nil { return err }
		
		err = s.Session.Query(`
		INSERT INTO article_locs
		 (messageid,recid,tmbmb,obkp,hbkp,bbkp) VALUES
		 (?        ,?    ,true ,true,true,true) USING TTL ?
		`,id       ,rid                                  ,secs).Exec()
		if err!=nil { return err }
	default:return articlestore.VEFail
	}
	return nil
}
var _ articlestore.StorageW = (*StoreWriter)(nil)

