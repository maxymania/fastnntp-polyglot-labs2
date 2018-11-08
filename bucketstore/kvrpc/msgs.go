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

import "io"
import "net"
import "bufio"
import "github.com/vmihailenco/msgpack"
import "github.com/valyala/fasthttp"
import "github.com/maxymania/fastnntp-polyglot-labs/bufferex"
import "sync"


type writer interface {
	io.Writer
	WriteByte(byte) error
	WriteString(string) (int, error)
}
type cwriter struct{
	writer
}

const (
	uGet uint = iota
	uPut
	uDelete
	uPutExpire
)

type encer struct{
	wri cwriter
	enc *msgpack.Encoder
}
func (e *encer) write(bw *bufio.Writer) *msgpack.Encoder {
	e.wri.writer = bw
	if e.enc==nil {
		e.enc = msgpack.NewEncoder(&e.wri)
	}
	return e.enc
}

type decer struct{
	dec *msgpack.Decoder
}
func (d *decer) read(br *bufio.Reader) *msgpack.Decoder {
	if d.dec==nil {
		d.dec = msgpack.NewDecoder(br)
	} else {
		d.dec.Reset(br)
	}
	return d.dec
}

type req struct{
	op uint
	bucket, key, value []byte
	expiresAt uint64
	enc encer
}
func (r *req) WriteRequest(bw *bufio.Writer) error {
	return r.enc.write(bw).EncodeMulti(r.op,r.bucket,r.key,r.value,r.expiresAt)
}

type reqCtx struct{
	// req
	op uint
	bucket, key, value []byte
	expiresAt uint64
	
	// resp
	err string
	bin bufferex.Binary
	
	dec decer
	enc encer
}

func (r *reqCtx) ConcurrencyLimitError(concurrency int) { r.err = "EFail" }
func (r *reqCtx) Init(conn net.Conn, logger fasthttp.Logger) {}
func (r *reqCtx) ReadRequest(br *bufio.Reader) error {
	return r.dec.read(br).DecodeMulti(&r.op,&r.bucket,&r.key,&r.value,&r.expiresAt)
}
func (r *reqCtx) WriteResponse(bw *bufio.Writer) error {
	arr := r.bin.Bytes()
	err := r.enc.write(bw).EncodeMulti(r.err,uint(len(arr)))
	if err!=nil { return err }
	_,err = bw.Write(arr)
	r.bin.Free()
	r.bin = bufferex.Binary{}
	return err
}

type resp struct{
	err string
	bin bufferex.Binary
	dec decer
}
func (r *resp) ReadResponse(br *bufio.Reader) error {
	var ui uint
	err := r.dec.read(br).DecodeMulti(&r.err,&ui)
	if err!=nil { return err }
	r.bin = bufferex.AllocBinary(int(ui))
	_,err = io.ReadFull(br,r.bin.Bytes())
	return err
}

var reqs = sync.Pool{ New:func() interface{} { return new(req) } }
var resps = sync.Pool{ New:func() interface{} { return new(resp) } }
func (r *req) free() {
	reqs.Put(r)
}
func (r *resp) pullBin() (bin bufferex.Binary) {
	bin = r.bin
	r.bin = bufferex.Binary{}
	return
}
func (r *resp) free() {
	r.bin.Free()
	r.bin = bufferex.Binary{}
	resps.Put(r)
}

