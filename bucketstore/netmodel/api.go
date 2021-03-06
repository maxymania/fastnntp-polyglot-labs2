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

package netmodel

import "github.com/maxymania/fastnntp-polyglot-labs2/bucketstore"
import "fmt"

type BucketType uint
const (
	// If the bucket is not served from any node.
	// Every bucket in this state is effectively non-existing.
	None BucketType = iota
	
	// If the bucket is served from one node.
	One
	
	// If the bucket is served from more than one node.
	Some
	
	// If the bucket is s
	All
)
func (b BucketType) String() string {
	switch b{
	case None: return "None"
	case One: return "One"
	case Some: return "Some"
	case All: return "All"
	}
	return fmt.Sprint("BucketType(",uint(b),")")
}

type Server struct {
	Reader bucketstore.BucketR
	Writer bucketstore.BucketW
	WriterEx bucketstore.BucketWEx
}

type Holder interface {
	GetHolder(bucket []byte) (srv Server,t BucketType)
}

