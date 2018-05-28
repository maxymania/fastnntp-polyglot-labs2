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

import "golang.org/x/crypto/ssh"

type Option interface{
	applyClient(cc *ssh.ClientConfig)
}

type fastciphers struct{}

func (fastciphers) applyClient(cc *ssh.ClientConfig) {
	/* We are prefering fast ciphers. */
	cc.Ciphers = []string{
		/* Fast and secure: */
		"chacha20-poly1305@openssh.com",
		
		/* Fast and insecure: */
		"arcfour","arcfour128","arcfour256",
	}
}

func FastCiphersOption() Option { return fastciphers{} }

type fastkex struct{}

func (fastkex) applyClient(cc *ssh.ClientConfig) {
	/* We are prefering fast KEXes. */
	cc.KeyExchanges = []string{
		/* Fastest and most secure one. */
		"curve25519-sha256@libssh.org",
		/* Compromitted but fast. */
		"ecdh-sha2-nistp256",
		"ecdh-sha2-nistp384",
		"ecdh-sha2-nistp521",
	}
}

func FastKexOption() Option { return fastkex{} }


