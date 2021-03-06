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


package articlechs

import "github.com/maxymania/fastnntp-polyglot-labs2/bucketstore/cluster"
import "github.com/maxymania/fastnntp-polyglot-labs2/bucketstore/bucketmap"
import "github.com/maxymania/fastnntp-polyglot-labs2/bucketstore/dkv"
import "github.com/maxymania/fastnntp-polyglot-labs2/bucketstore/dkv/health"
import "github.com/maxymania/fastnntp-polyglot-labs/guido"
import "os"
//import "errors"

//var EPathMismatch = errors.New("EPathMismatch")

func openBucket(path string,d *cluster.Deleg) {
	s,err := os.Stat(path)
	if err!=nil { return }
	if !s.IsDir() { return }
	u,err := guido.GetUID(path)
	if err!=nil { return }
	db,err := dkv.OpenQuick(path)
	if err!=nil { return }
	bkt := []byte(u.String())
	d.AddBucket(bkt,bucketmap.Bucket{Reader:db,Writer:db,WriterEx:db})
	(&health.HealthChecker{Path:path,Bucket:bkt}).Start()
}

