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


package wire1

import "io"
import "bufio"
import "sync"

type ewriter struct{}
func (ewriter) Write([]byte) (int,error) { return 0,nil }
var ewriter_ io.Writer = ewriter{}

type writerPool struct {
	sync.Pool
	Size int
}
type writer struct {
	*bufio.Writer
	h *writerPool
}

func (p *writerPool) Create(w io.Writer) *writer {
	o := p.Get()
	if o==nil { return &writer{bufio.NewWriterSize(w,p.Size),p} }
	x := o.(*writer)
	x.Reset(w)
	return x
}

func (w *writer) Free() {
	w.h.Put(w)
	w.Reset(ewriter_)
}


var largeWriters = writerPool{Size: 1<<16}
var smallWriters = writerPool{Size: 1<<16}

/*------------------------------------------------*/

