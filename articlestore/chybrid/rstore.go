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
import "github.com/maxymania/fastnntp-polyglot-labs/bufferex"
import "github.com/maxymania/fastnntp-polyglot-labs2/articlestore"
import "github.com/maxymania/fastnntp-polyglot-labs2/bucketstore"

func extend(id []byte) (bufferex.Binary,*byte) {
	buf := bufferex.AllocBinary(len(id)+1)
	copy(buf.Bytes(),id)
	return buf,&buf.Bytes()[len(id)]
}

type StoreReader struct {
	Bucket bucketstore.BucketR
	Session *gocql.Session
}

func (s *StoreReader) StoreReadMessage(id []byte, over, head, body bool) (result bufferex.Binary, err error) {
	idb,bts := extend(id)
	defer idb.Free()
	var bkt []byte
	var overb,headb,bodyb bufferex.Binary
	q := s.Session.Query(`
	SELECT
		xover,
		bucket
	FROM article_locs
	WHERE messageid = ? AND avail = true
	LIMIT 1 ALLOW FILTERING
	`,id)
	defer q.Release()
	
	{
		var xov []byte
		err = q.Scan(&xov,&bkt)
		if err!=nil { return }
		overb = bufferex.NewBinaryInplace(xov)
	}
	
	if head {
		*bts = 'h'
		headb,err = s.Bucket.BucketGet(bkt,idb.Bytes())
		if err!=nil { return }
		defer headb.Free()
	}
	if !over {
		overb = bufferex.Binary{}
	} else if len(overb.Bytes())==0 {
		*bts = 'x'
		overb,err = s.Bucket.BucketGet(bkt,idb.Bytes())
		if err!=nil { return }
		defer overb.Free()
	}
	if body {
		*bts = 'b'
		bodyb,err = s.Bucket.BucketGet(bkt,idb.Bytes())
		if err!=nil { return }
		defer bodyb.Free()
	}
	result,err = articlestore.PackMessage(overb.Bytes(),headb.Bytes(),bodyb.Bytes())
	return
}
var _ articlestore.StorageR = (*StoreReader)(nil)

