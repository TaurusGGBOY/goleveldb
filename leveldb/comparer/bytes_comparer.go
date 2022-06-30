// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package comparer

import "bytes"

type bytesComparer struct{}

func (bytesComparer) Compare(a, b []byte) int {
	return bytes.Compare(a, b)
}

func (bytesComparer) Name() string {
	return "leveldb.BytewiseComparator"
}

// 懂了……和前一个比较 只保存不同的地方 减少key的大小的
func (bytesComparer) Separator(dst, a, b []byte) []byte {
	i, n := 0, len(a)
	// n是ab最小大小
	if n > len(b) {
		n = len(b)
	}
	// 一直遍历到两者最小的大小处
	for ; i < n && a[i] == b[i]; i++ {
	}
	if i >= n {
		// 如果i==n了 就其中一个是前缀 返回空
		// Do not shorten if one string is a prefix of the other
		// 否则如果a小 就把a加入dst 并截断最后一个byte
		// 意义不明……
	} else if c := a[i]; c < 0xff && c+1 < b[i] {
		dst = append(dst, a[:i+1]...)
		dst[len(dst)-1]++
		return dst
	}
	return nil
}

// 然后这个是给个前缀和key用来恢复的
func (bytesComparer) Successor(dst, b []byte) []byte {
	for i, c := range b {
		if c != 0xff {
			dst = append(dst, b[:i+1]...)
			dst[len(dst)-1]++
			return dst
		}
	}
	return nil
}

// DefaultComparer are default implementation of the Comparer interface.
// It uses the natural ordering, consistent with bytes.Compare.
var DefaultComparer = bytesComparer{}
