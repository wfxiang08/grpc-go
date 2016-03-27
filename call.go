/*
 *
 * Copyright 2014, Google Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *     * Neither the name of Google Inc. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package grpc

import (
	"bytes"
	"io"
	"time"

	"golang.org/x/net/context"
	"golang.org/x/net/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/transport"
)

// recvResponse receives and parses an RPC response.
// On error, it returns the error and indicates whether the call should be retried.
//
// TODO(zhaoq): Check whether the received message sequence is valid.
func recvResponse(dopts dialOptions, t transport.ClientTransport, c *callInfo, stream *transport.Stream, reply interface{}) error {
	// Try to acquire header metadata from the server if there is any.

	// 如何接受请求呢?
	// 1. 和处理的Http请求类似, 首先是: 处理Header
	var err error
	c.headerMD, err = stream.Header()
	if err != nil {
		return err
	}


	// 2. 然后处理stream
	p := &parser{r: stream}
	for {
		// stream --> (parser) --> msg --> decompress --> unmarshal --> reply
		// Iter 1: 读取到Reply
		// Iter 2: 读取失败，碰到EOF
		if err = recv(p, dopts.codec, stream, dopts.dc, reply); err != nil {
			// 似乎需要等待stream的EOF
			if err == io.EOF {
				break
			}
			return err
		}
	}

	// 3. 获取Trailer?
	c.trailerMD = stream.Trailer()
	return nil
}

// sendRequest writes out various information of an RPC such as Context and Message.
func sendRequest(ctx context.Context, codec Codec, compressor Compressor, callHdr *transport.CallHdr, t transport.ClientTransport, args interface{}, opts *transport.Options) (_ *transport.Stream, err error) {
	stream, err := t.NewStream(ctx, callHdr)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			if _, ok := err.(transport.ConnectionError); !ok {
				t.CloseStream(stream, err)
			}
		}
	}()

	// 1. cbuf必须为干净的，没有上次的状态
	var cbuf *bytes.Buffer
	if compressor != nil {
		cbuf = new(bytes.Buffer)
	}

	// 2. 数据的编码
	outBuf, err := encode(codec, args, compressor, cbuf)

	// 2.1 如果编码出现错误，则直接报错
	if err != nil {
		return nil, transport.StreamErrorf(codes.Internal, "grpc: %v", err)
	}

	// 2.2 输出数据
	//     注意概念: stream vs. transport
	err = t.Write(stream, outBuf, opts)
	if err != nil {
		return nil, err
	}

	// Sent successfully.
	return stream, nil
}

// Invoke is called by the generated code. It sends the RPC request on the
// wire and returns after response is received.
func Invoke(ctx context.Context, method string, args, reply interface{}, cc *ClientConn, opts ...CallOption) (err error) {
	var c callInfo

	// before/after
    // 1. 首先调用所有的opts的before(作用的对象是callInfo)
	for _, o := range opts {
		if err := o.before(&c); err != nil {
			return toRPCErr(err)
		}
	}
	defer func() {
		for _, o := range opts {
			o.after(&c)
		}
	}()

	// 暂不考虑: Tracing
	if EnableTracing {
		c.traceInfo.tr = trace.New("grpc.Sent."+methodFamily(method), method)
		defer c.traceInfo.tr.Finish()
		c.traceInfo.firstLine.client = true
		if deadline, ok := ctx.Deadline(); ok {
			c.traceInfo.firstLine.deadline = deadline.Sub(time.Now())
		}
		c.traceInfo.tr.LazyLog(&c.traceInfo.firstLine, false)
		// TODO(dsymonds): Arrange for c.traceInfo.firstLine.remoteAddr to be set.
		defer func() {
			if err != nil {
				c.traceInfo.tr.LazyLog(&fmtStringer{"%v", []interface{}{err}}, true)
				c.traceInfo.tr.SetError()
			}
		}()
	}

	// 函数调用: 最后一个消息，马上执行
	topts := &transport.Options{
		Last:  true,
		Delay: false,
	}

	var (
		lastErr error // record the error that happened
	)

	// 在某个Invoke过程中不停地for，到底是要做什么呢?
	for {
		var (
			err    error
			t      transport.ClientTransport
			stream *transport.Stream
		)


		// TODO(zhaoq): Need a formal spec of retry strategy for non-failfast rpcs.
		// 1. failFast的处理
		if lastErr != nil && c.failFast {
			return toRPCErr(lastErr)
		}

		// 2. callHdr如何处理呢?
		callHdr := &transport.CallHdr{
			Host:   cc.authority,
			Method: method,
		}

		// 3. 设置发送方的压缩算法(在接收方需要检测这个)
		if cc.dopts.cp != nil {
			callHdr.SendCompress = cc.dopts.cp.Type()
		}

		// 4. 挑选一个Transport
		//    这个是如何实现的呢?
		t, err = cc.dopts.picker.Pick(ctx)


		// 4.1 如果是Pick出现问题，则没有必要尝试；可能是底层的网路出现问题
		if err != nil {
			// 如果retry失败，则直接报失败
			if lastErr != nil {
				// This was a retry; return the error from the last attempt.
				return toRPCErr(lastErr)
			}
			return toRPCErr(err)
		}

		// 暂不考虑: tr
		if c.traceInfo.tr != nil {
			c.traceInfo.tr.LazyLog(&payload{sent: true, msg: args}, true)
		}

		// 5. 如何发送请求呢?
		// 核心参数: args
		stream, err = sendRequest(ctx, cc.dopts.codec, cc.dopts.cp, callHdr, t, args, topts)

		// 如何处理Err呢?
		if err != nil {
			// 5.1 如果是连接出现错误，则继续尝试
			if _, ok := err.(transport.ConnectionError); ok {
				lastErr = err
				continue
			}

			// 5.2 其他错误，则直接终止调用
			if lastErr != nil {
				return toRPCErr(lastErr)
			}
			return toRPCErr(err)
		}


		// 6. Receive the response
		lastErr = recvResponse(cc.dopts, t, &c, stream, reply)

		// 6.1 如果是连接错误，则可以继续尝试
		if _, ok := lastErr.(transport.ConnectionError); ok {
			continue
		}
		if c.traceInfo.tr != nil {
			c.traceInfo.tr.LazyLog(&payload{sent: false, msg: reply}, true)
		}

		// 7. 关闭stream
		t.CloseStream(stream, lastErr)
		if lastErr != nil {
			return toRPCErr(lastErr)
		}

		// 8. 汇报错误状态
		return Errorf(stream.StatusCode(), "%s", stream.StatusDesc())
	}
}
