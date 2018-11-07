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


package wire2

import "github.com/valyala/fastrpc"
import "github.com/valyala/fasthttp"
//import "github.com/maxymania/fastnntp-polyglot-labs2/groupidx"
import "github.com/vmihailenco/msgpack"
import "net"
import "bufio"
import "bytes"

var empty_ = new(bytes.Buffer)


type iRequest struct{
	Type      []byte
	WantReply bool
	Payload   []byte
	
	ok        bool
	reply     []byte
}
func (r *iRequest) Reply(ok bool, payload []byte) {
	r.ok = ok
	r.reply = append(r.reply[:0],payload...)
}
func (r *iRequest) DecodeReq(dec *msgpack.Decoder) error {
	return dec.DecodeMulti(&r.Type,&r.WantReply,&r.Payload)
}
func (r *iRequest) EncodeReq(enc *msgpack.Encoder) error {
	return enc.EncodeMulti(r.Type,r.WantReply,r.Payload)
}
func (r *iRequest) DecodeResp(dec *msgpack.Decoder) error {
	return dec.DecodeMulti(&r.ok,&r.reply)
}
func (r *iRequest) EncodeResp(enc *msgpack.Encoder) error {
	return enc.EncodeMulti(r.ok,r.reply)
}

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
	//h.inner.Reply(false,nil)
}
func (h *handlerctx) Init(conn net.Conn, logger fasthttp.Logger) {
	h.inner.Reply(false,nil)
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

func newHandler() fastrpc.HandlerCtx{
	h := new(handlerctx)
	h.wreset(empty_)
	h.enc = msgpack.NewEncoder(&h.encs)
	h.dec = msgpack.NewDecoder(empty_)
	return h
}

