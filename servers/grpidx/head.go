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

import "github.com/byte-mug/goconfig"
import "github.com/maxymania/fastnntp-polyglot-labs2/groupidx"
import "fmt"


type f_storage func(s *storage) (groupidx.GroupIndex,error)
var m_storage = make(map[string]f_storage)

func create_storage_head(c *storage) (groupidx.GroupIndex,error) {
	f := m_storage[c.Type]
	if f==nil { return nil,fmt.Errorf("No such storage method %q",c.Type) }
	return f(c)
}

/*
Parses a config file (or blob), loads the storage and runs.
	storage groupdb {
		initial-mmap-size: 1<<20
		page-size: 1<<14
		table: groupdb
		location: 'F:/data/'
	}
	network {
		net: tcp
		addr: ':9999'
	}
*/
func Serve(cfg []byte) error {
	obj := new(config)
	err := goconfig.Parse(cfg,goconfig.CreateReflectHandler(obj))
	if err!=nil { return err }
	fmt.Println(obj)
	ginr,err := create_storage_head(&obj.Storage)
	if err!=nil { return err }
	return serve(&obj.Network,ginr)
}

