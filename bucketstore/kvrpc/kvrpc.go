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


package kvrpc

import "time"
import "errors"
import "github.com/maxymania/fastnntp-polyglot-labs/bufferex"
import "github.com/maxymania/fastnntp-polyglot-labs2/bucketstore"
import "github.com/valyala/fastrpc"

type GBucket interface{
	bucketstore.BucketR
	bucketstore.BucketW
	bucketstore.BucketWEx
}

func NewHandler() fastrpc.HandlerCtx { return new(reqCtx) }
func nresp() fastrpc.ResponseReader { return new(resp) }

func e2s(e error) string {
	if e==nil { return "" }
	return e.Error()
}
func s2e(e string) error {
	switch e {
	case "": return nil
	case "EFail": return bucketstore.EFail
	case "ENotFound": return bucketstore.ENotFound
	}
	return errors.New(e)
}

func doerr (r *reqCtx) {
	r.err = "EFail"
}

func Makehandler(b GBucket) func(ctx fastrpc.HandlerCtx) fastrpc.HandlerCtx {
	return func(ctx fastrpc.HandlerCtx) fastrpc.HandlerCtx {
		rr := ctx.(*reqCtx)
		switch rr.op {
		case uGet:
			var e error
			rr.bin,e = b.BucketGet(rr.bucket,rr.key)
			rr.err = e2s(e)
		case uPut: rr.err = e2s(b.BucketPut(rr.bucket,rr.key,rr.value))
		case uDelete: rr.err = e2s(b.BucketDelete(rr.bucket,rr.key))
		case uPutExpire: rr.err = e2s(b.BucketPutExpire(rr.bucket,rr.key,rr.value,rr.expiresAt))
		default: doerr(rr)
		}
		return ctx
	}
}
func NewServer(b GBucket) *fastrpc.Server {
	return &fastrpc.Server{
		NewHandlerCtx: NewHandler,
		Handler: Makehandler(b),
	}
}


type Client struct{
	Cli fastrpc.Client
}
func (c *Client) Init() {
	c.Cli.NewResponse = nresp
}
func (c *Client) BucketGet(bucket, key []byte) (bufferex.Binary, error) {
	o := reqs.Get().(*req)
	defer o.free()
	i := resps.Get().(*resp)
	defer i.free()
	
	o.op = uGet
	o.bucket = append(o.bucket,bucket...)
	o.key = append(o.key,key...)
	o.value = o.value[:0]
	o.expiresAt = 0
	
	err := c.Cli.DoDeadline(o,i,time.Now().Add(time.Second*5))
	if err!=nil { return bufferex.Binary{},err }
	
	return i.pullBin(),s2e(i.err)
}
func (c *Client) BucketPut(bucket, key, value []byte) error {
	o := reqs.Get().(*req)
	defer o.free()
	i := resps.Get().(*resp)
	defer i.free()
	
	o.op = uPut
	o.bucket = append(o.bucket,bucket...)
	o.key = append(o.key,key...)
	o.value = append(o.value,value...)
	o.expiresAt = 0
	
	err := c.Cli.DoDeadline(o,i,time.Now().Add(time.Second*5))
	if err!=nil { return err }
	
	return s2e(i.err)
}
func (c *Client) BucketDelete(bucket, key []byte) error {
	o := reqs.Get().(*req)
	defer o.free()
	i := resps.Get().(*resp)
	defer i.free()
	
	o.op = uDelete
	o.bucket = append(o.bucket,bucket...)
	o.key = append(o.key,key...)
	o.value = o.value[:0]
	o.expiresAt = 0
	
	err := c.Cli.DoDeadline(o,i,time.Now().Add(time.Second*5))
	if err!=nil { return err }
	
	return s2e(i.err)
}
func (c *Client) BucketPutExpire(bucket, key, value []byte, expiresAt uint64) error {
	o := reqs.Get().(*req)
	defer o.free()
	i := resps.Get().(*resp)
	defer i.free()
	
	o.op = uPut
	o.bucket = append(o.bucket,bucket...)
	o.key = append(o.key,key...)
	o.value = append(o.value,value...)
	o.expiresAt = expiresAt
	
	err := c.Cli.DoDeadline(o,i,time.Now().Add(time.Second*5))
	if err!=nil { return err }
	
	return s2e(i.err)
}

var _ GBucket = (*Client)(nil)

