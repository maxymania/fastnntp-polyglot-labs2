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

package overdbimp

//import "github.com/syndtr/goleveldb/leveldb"
import "github.com/syndtr/goleveldb/leveldb/table"
import "github.com/syndtr/goleveldb/leveldb/opt"
import "github.com/syndtr/goleveldb/leveldb/memdb"
import "github.com/syndtr/goleveldb/leveldb/comparer"
import "io"
import "bufio"
import "bytes"
import "regexp"
//import "fmt"

var active = regexp.MustCompile(`^(\S+)\s+\S+\s+\S+\s+(.)`)
var newsgroups = regexp.MustCompile(`^(\S+)\s+([^\r\n]+)`)

func Import(a,ng io.Reader,i io.Writer, o *opt.Options) error {
	db := memdb.New(comparer.DefaultComparer,1<<20)
	A := bufio.NewReader(a)
	NG := bufio.NewReader(ng)
	var buf []byte
	for {
		line,err := A.ReadSlice('\n')
		if err!=nil { break }
		m := active.FindSubmatch(line)
		if len(m)==0 { continue }
		db.Put(m[1],m[2])
	}
	for {
		line,err := NG.ReadSlice('\n')
		if err!=nil { break }
		m := newsgroups.FindSubmatch(line)
		if len(m)==0 { continue }
		k,v,err := db.Find(m[1])
		if err!=nil { break }
		if !bytes.Equal(k,m[1]) { continue }
		buf = append(append(buf[:0],v[0]),m[2]...)
		db.Put(m[1],buf)
	}
	t := table.NewWriter(i, o)
	defer t.Close()
	cursor := db.NewIterator(nil)
	for ok := cursor.First(); ok ; ok = cursor.Next() {
		err := t.Append(cursor.Key(),cursor.Value())
		if err!=nil { return err }
		//fmt.Printf("%q => %q\n",cursor.Key(),cursor.Value())
	}
	return nil
}

