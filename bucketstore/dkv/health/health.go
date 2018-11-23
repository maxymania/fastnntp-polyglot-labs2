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


package health

import "github.com/ricochet2200/go-disk-usage/du"
import "github.com/maxymania/fastnntp-polyglot-labs2/bucketstore/healthmap"
import "time"

// Minimum is 256 MB
const minimumAvailable = 1<<28

type HealthChecker struct {
	Path string
	Bucket []byte
	Stop bool
}
func (h *HealthChecker) perform() {
	t := time.Tick(time.Second*30)
	for {
		<-t
		usage := du.NewDiskUsage(h.Path)
		minsiz := usage.Size()>>10
		bad := usage.Available()<minimumAvailable || usage.Available()<minsiz
		
		healthmap.IssueHealth(healthmap.Health{h.Bucket,!bad})
	}
}
func (h *HealthChecker) Start() {
	go h.perform()
}

