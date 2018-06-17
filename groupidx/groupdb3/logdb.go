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


package groupdb3

import bolt "github.com/coreos/bbolt"
import "github.com/vmihailenco/msgpack"
import "os"
import "path/filepath"
import "io/ioutil"
import "strings"
import "sort"
import "fmt"
import "time"

type LogDB struct{
	DB
	path string
	signal chan int
}
func openLogDB(path string) (*LogDB,error) {
	j := filepath.Join(path,"master.db")
	bdb,err := bolt.Open(j,0600,nil)
	if err!=nil { return nil,err }
	return &LogDB{
		DB{bdb,[]byte("groups"),new(slogs)},
		path,
		make(chan int,1),
	},nil
}
func (l *LogDB) getLogs() []string {
	fis,_ := ioutil.ReadDir(l.path)
	s := make([]string,0,len(fis))
	for _,fi := range fis {
		if fi.IsDir() { continue }
		fn := fi.Name()
		if !strings.HasSuffix(fn,".log") { continue }
		s = append(s,fn)
	}
	sort.Strings(s)
	return s
}
func (l *LogDB) startup() error {
	for _,s := range l.getLogs() {
		j := filepath.Join(l.path,s)
		f,err := os.Open(j)
		if err!=nil { return err }
		dec := msgpack.NewDecoder(f)
		slg := mkslog2()
		for {
			row := new(TableRow)
			if dec.Decode(row)!=nil { break }
			slg.dedupeString(&row.Group)
			slg.insertMem(row)
		}
		f.Close()
		err = l.batch(func(t *bolt.Bucket) error { return Tx{t}.insertAllRecords(slg) })
		if err!=nil { return err }
		err = os.Remove(j)
		if err!=nil { return err }
	}
	return nil
}
func (l *LogDB) allocSlog() error {
	j := filepath.Join(l.path,fmt.Sprintf("%016x.log",time.Now().UnixNano()))
	f,err := os.OpenFile(j,os.O_CREATE|os.O_EXCL|os.O_RDWR,0644)
	if err!=nil { return err }
	l.slogs.push(mkslog(f,j))
	return nil
}
func OpenLogDB(path string) (*LogDB,error) {
	l,e := openLogDB(path)
	if e!=nil { return nil,e }
	e = l.startup()
	if e!=nil { l.inner.Close(); return nil,e }
	e = l.allocSlog()
	if e!=nil { l.inner.Close(); return nil,e }
	return l,nil
}

func (l *LogDB) FlushLog() error {
	l.slogs.WB.Lock(); defer l.slogs.WB.Unlock()
	err := l.allocSlog()
	if err!=nil { return err }
	s := l.slogs.Stack[0]
	s.WB.Lock()
		err = l.batch(func(t *bolt.Bucket) error { return Tx{t}.insertAllRecords(s) })
	s.WB.Unlock()
	if err!=nil { return err }
	s.deleteIt()
	l.slogs.shift()
	return nil
}
func (l *LogDB) getSize0() (int64,error) {
	l.slogs.Lock.RLock(); defer l.slogs.Lock.RUnlock()
	if len(l.slogs.Stack)==0 { return 0,fmt.Errorf("No slogs") }
	s := l.slogs.Stack[0]
	s.Lock.RLock(); defer s.Lock.RUnlock()
	fi,err := s.File.Stat()
	if err!=nil { return 0,err }
	return fi.Size(),nil
}
func (l *LogDB) Worker(treshold int64) {
	for {
		<- l.signal
		s,err := l.getSize0()
		if err!=nil {
			fmt.Println(err)
			err = l.FlushLog()
		} else if s>=treshold {
			err = l.FlushLog()
		}
		if err!=nil {
			fmt.Println(err)
		}
	}
}
func (l *LogDB) wakeup() {
	select {
	case l.signal <- 1:
	default:
	}
}
func (l *LogDB) AssignArticleToGroup(group []byte, num, exp uint64, id []byte) error {
	defer l.wakeup()
	id = append([]byte(nil),id...)
	row := &TableRow{TableKey:TableKey{Group:group,Number:num},Expires:exp,MessageId:id}
	return l.slogs.insert(row)
}
func (l *LogDB) AssignArticleToGroups(groups [][]byte, nums []int64, exp uint64, id []byte) error {
	defer l.wakeup()
	id = append([]byte(nil),id...)
	for i := range groups {
		row := &TableRow{TableKey:TableKey{Group: groups[i],Number: uint64(nums[i])},Expires: exp,MessageId: id}
		err := l.slogs.insert(row)
		if err!=nil && i==0 { return err }
	}
	return nil
}


