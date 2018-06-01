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


package netwire

import "github.com/valyala/fastrpc"
import "github.com/valyala/fasthttp"
import "github.com/maxymania/fastnntp-polyglot-labs/bufferex"
import "github.com/vmihailenco/msgpack"
import "net"
import "bufio"
import "bytes"
import "fmt"
import "sync"

var empty_ = new(bytes.Buffer)

var ENoResponse = fmt.Errorf("NoResponse")

type iRequest struct{
	Type      []byte
	MessageId []byte
	Payload   []byte
	Expire    uint64
	
	ok        bool
	reply     []byte
}
/* ---------------------------------------------------------- */
func (r *iRequest) Respond(b []byte,e error) {
	if e!=nil {
		r.ok = false
		r.reply = append(r.reply[:0],e.Error()...)
	} else {
		r.ok = true
		r.reply = append(r.reply[:0],b...)
	}
}
func (r *iRequest) RespondB(b bufferex.Binary,e error) {
	defer b.Free()
	if e!=nil {
		r.ok = false
		r.reply = append(r.reply[:0],e.Error()...)
	} else {
		r.ok = true
		r.reply = append(r.reply[:0],b.Bytes()...)
	}
}
/* ---------------------------------------------------------- */
func (r *iRequest) DecodeReq(dec *msgpack.Decoder) error {
	return dec.Decode(&r.Type,&r.MessageId,&r.Payload,&r.Expire)
}
func (r *iRequest) EncodeReq(enc *msgpack.Encoder) error {
	return enc.Encode(r.Type,r.MessageId,r.Payload,r.Expire)
}
func (r *iRequest) DecodeResp(dec *msgpack.Decoder) error {
	return dec.Decode(&r.ok,&r.reply)
}
func (r *iRequest) EncodeResp(enc *msgpack.Encoder) error {
	return enc.Encode(r.ok,r.reply)
}
/* ---------------------------------------------------------- */
func (r *iRequest) GetError() error {
	if r.ok { return nil }
	return fmt.Errorf("%s",r.reply)
}
func (r *iRequest) GetBinary() (b bufferex.Binary) {
	if r.ok { b = bufferex.NewBinary(r.reply) }
	return
}
/* ---------------------------------------------------------- */

type handlerctx struct{
	inner iRequest
	encs mpwp
	enc *msgpack.Encoder
	dec *msgpack.Decoder
}
func (h *handlerctx) wreset(m mpwi) {
	h.encs.mpwi = m
}

func (h *handlerctx) ConcurrencyLimitError(concurrency int) {
	h.inner.Respond(nil,fmt.Errorf("ConcurrencyLimitError(%d)",concurrency))
}
func (h *handlerctx) Init(conn net.Conn, logger fasthttp.Logger) {
	h.inner.Respond(nil,ENoResponse)
}
func (h *handlerctx) ReadRequest(br *bufio.Reader) error {
	h.dec.Reset(br)
	defer h.dec.Reset(empty_)
	return h.inner.DecodeReq(h.dec)
}
func (h *handlerctx) WriteResponse(bw *bufio.Writer) error {
	h.wreset(bw)
	defer h.wreset(empty_)
	return h.inner.EncodeResp(h.enc)
}

var _ fastrpc.HandlerCtx = (*handlerctx)(nil)

func newHandler() fastrpc.HandlerCtx{
	h := new(handlerctx)
	h.wreset(empty_)
	h.enc = msgpack.NewEncoder(&h.encs)
	h.dec = msgpack.NewDecoder(empty_)
	return h
}

type request struct{
	inner iRequest
	encs mpwp
	enc *msgpack.Encoder
}
func (h *request) wreset(m mpwi) {
	h.encs.mpwi = m
}
func (h *request) WriteRequest(bw *bufio.Writer) error {
	h.wreset(bw)
	defer h.wreset(empty_)
	if h.enc==nil { h.enc = msgpack.NewEncoder(&h.encs) }
	return h.inner.EncodeReq(h.enc)
}

var _ fastrpc.RequestWriter = (*request)(nil)

type response struct{
	inner iRequest
	dec *msgpack.Decoder
}
func (r *response) ReadResponse(br *bufio.Reader) error {
	if r.dec==nil {
		r.dec = msgpack.NewDecoder(br)
	} else {
		r.dec.Reset(br)
	}
	defer r.dec.Reset(empty_)
	return r.inner.DecodeResp(r.dec)
}

var _ fastrpc.ResponseReader = (*response)(nil)

var reqPool = sync.Pool{ New:func() interface{} { return new(request) } }
var respPool = sync.Pool{ New:func() interface{} { return new(response) } }

func newResponse() fastrpc.ResponseReader { return respPool.Get().(fastrpc.ResponseReader) }

