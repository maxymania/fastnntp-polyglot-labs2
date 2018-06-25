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

package grpidx

import . "github.com/maxymania/fastnntp-polyglot-labs2/servers/grpidx2"
import "github.com/maxymania/fastnntp-polyglot-labs2/groupidx"
import "github.com/maxymania/fastnntp-polyglot-labs2/groupidx/groupdb3"
import "os"
import "strings"

var osPS = string([]rune{os.PathSeparator})

func storageGroupdb (s *Storage) (groupidx.GroupIndex,error) {
	db,err := groupdb3.OpenLogDB(strings.Replace(s.Location,"/",osPS,-1))
	if err!=nil { return nil,err }
	go db.Worker(s.LogSizeFlush.Int64())
	return db,nil
}

func init() {
	Register("groupdb3",storageGroupdb)
}
