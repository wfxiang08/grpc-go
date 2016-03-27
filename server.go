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
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/context"
	"golang.org/x/net/http2"
	"golang.org/x/net/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/internal"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/transport"
)

type methodHandler func(srv interface{}, ctx context.Context, dec func(interface{}) error) (interface{}, error)

// 1. 基本元素:
//            方法描述
//            流描述
//            服务描述
//
// MethodDesc represents an RPC service's method specification.
type MethodDesc struct {
	MethodName string
	Handler    methodHandler
}

// ServiceDesc represents an RPC service's specification.
type ServiceDesc struct {
	ServiceName string
	// The pointer to the service interface. Used to check whether the user
	// provided implementation satisfies the interface requirements.
	HandlerType interface{}
	Methods     []MethodDesc
	Streams     []StreamDesc
}

// service consists of the information of the server serving this service and
// the methods in this service.
// 2. 服务: service
type service struct {
	server interface{} // the server for service methods
	md     map[string]*MethodDesc
	sd     map[string]*StreamDesc
}

// Server is a gRPC server to serve RPC requests.
// 3. Server 将多个服务聚合在一起，对外提供服务?
type Server struct {
	opts   options

							   // 注意: golang的结构体的设计，变量的布局，注释...
	mu     sync.Mutex          // guards following
	lis    map[net.Listener]bool
	conns  map[io.Closer]bool
	m      map[string]*service // service name -> service info
	events trace.EventLog
}

type options struct {
	creds                credentials.Credentials
	codec                Codec
	cp                   Compressor
	dc                   Decompressor
	maxConcurrentStreams uint32
	useHandlerImpl       bool // use http.Handler-based server
}

// 4. 通过ServerOption来设置 Server Side的options
//----------------------------------------------------------------------------------------------------------------------
// A ServerOption sets options.
type ServerOption func(*options)

// CustomCodec returns a ServerOption that sets a codec for message marshaling and unmarshaling.
func CustomCodec(codec Codec) ServerOption {
	return func(o *options) {
		o.codec = codec
	}
}

func RPCCompressor(cp Compressor) ServerOption {
	return func(o *options) {
		o.cp = cp
	}
}

func RPCDecompressor(dc Decompressor) ServerOption {
	return func(o *options) {
		o.dc = dc
	}
}

// MaxConcurrentStreams returns a ServerOption that will apply a limit on the number
// of concurrent streams to each ServerTransport.
func MaxConcurrentStreams(n uint32) ServerOption {
	return func(o *options) {
		o.maxConcurrentStreams = n
	}
}

// Creds returns a ServerOption that sets credentials for server connections.
func Creds(c credentials.Credentials) ServerOption {
	return func(o *options) {
		o.creds = c
	}
}
//----------------------------------------------------------------------------------------------------------------------

// NewServer creates a gRPC server which has no service registered and has not
// started to accept requests yet.
func NewServer(opt ...ServerOption) *Server {
	// 1. 调用各种 options
	var opts options
	for _, o := range opt {
		o(&opts)
	}

	// 2. 设置codec, 默认为protobuf
	if opts.codec == nil {
		// Set the default codec.
		opts.codec = protoCodec{}
	}

	// 3. 创建一个Server, 默认情况下: lis 为空, conns为空
	s := &Server{
		lis:   make(map[net.Listener]bool),
		opts:  opts,
		conns: make(map[io.Closer]bool),
		m:     make(map[string]*service),
	}

	// 3.1. 如何Tracing, runtime?
	if EnableTracing {
		_, file, line, _ := runtime.Caller(1)
		s.events = trace.NewEventLog("grpc.Server", fmt.Sprintf("%s:%d", file, line))
	}
	return s
}

// printf records an event in s's event log, unless s has been stopped.
// REQUIRES s.mu is held.
func (s *Server) printf(format string, a ...interface{}) {
	if s.events != nil {
		s.events.Printf(format, a...)
	}
}

// errorf records an error in s's event log, unless s has been stopped.
// REQUIRES s.mu is held.
func (s *Server) errorf(format string, a ...interface{}) {
	if s.events != nil {
		s.events.Errorf(format, a...)
	}
}

// RegisterService register a service and its implementation to the gRPC
// server. Called from the IDL generated code. This must be called before
// invoking Serve.
// 例如:
// func RegisterGreeterServer(s *grpc.Server, srv GreeterServer) {
// 	s.RegisterService(&_Greeter_serviceDesc, srv)
// }
// 通过pb生成的代码，会将: RegisterService 进行额外的包装, 将ServiceDesc封装起来
//
func (s *Server) RegisterService(sd *ServiceDesc, ss interface{}) {

	ht := reflect.TypeOf(sd.HandlerType).Elem()
	st := reflect.TypeOf(ss)
	if !st.Implements(ht) {
		grpclog.Fatalf("grpc: Server.RegisterService found the handler of type %v that does not satisfy %v", st, ht)
	}
	s.register(sd, ss)
}

//
// 将服务注册到当前的Server上
//
func (s *Server) register(sd *ServiceDesc, ss interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.printf("RegisterService(%q)", sd.ServiceName)
	if _, ok := s.m[sd.ServiceName]; ok {
		grpclog.Fatalf("grpc: Server.RegisterService found duplicate service registration for %q", sd.ServiceName)
	}

	// 生成Server内部的service的描述
	// server: 具体的Handler
	// md: Method Description
	// sd: Stream description
	// 1. 创建一个service
	srv := &service{
		server: ss,
		md:     make(map[string]*MethodDesc),
		sd:     make(map[string]*StreamDesc),
	}
	for i := range sd.Methods {
		d := &sd.Methods[i]
		srv.md[d.MethodName] = d
	}
	for i := range sd.Streams {
		d := &sd.Streams[i]
		srv.sd[d.StreamName] = d
	}

	// 2. 注册服务
	s.m[sd.ServiceName] = srv
}

var (
// ErrServerStopped indicates that the operation is now illegal because of
// the server being stopped.
	ErrServerStopped = errors.New("grpc: the server has been stopped")
)

func (s *Server) useTransportAuthenticator(rawConn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	// 1. 获取creds
	creds, ok := s.opts.creds.(credentials.TransportAuthenticator)
	if !ok {
		return rawConn, nil, nil
	}

	// 2. TLS Handshake
	return creds.ServerHandshake(rawConn)
}

// Serve accepts incoming connections on the listener lis, creating a new
// ServerTransport and service goroutine for each. The service goroutines
// read gRPC requests and then call the registered handlers to reply to them.
// Service returns when lis.Accept fails.
func (s *Server) Serve(lis net.Listener) error {
	// 如何对外提供服务呢?

	// 利用当前的listener对外提供服务

	// 1. 服务的状态: lis == nil 停止服务
	//               lis == map 正常服务
	s.mu.Lock()
	s.printf("serving")
	if s.lis == nil {
		s.mu.Unlock()
		return ErrServerStopped
	}
	s.lis[lis] = true
	s.mu.Unlock()


	// 2. 关闭当前的 lis
	defer func() {
		lis.Close()  // 关闭当前的listener

		s.mu.Lock()
		delete(s.lis, lis)  // 删除当前的listener
		s.mu.Unlock()
	}()


	// 3.
	for {
		rawConn, err := lis.Accept()

		// 如果Accept出现错误，表示不再接受请求
		if err != nil {
			s.mu.Lock()
			s.printf("done serving; Accept = %v", err)
			s.mu.Unlock()
			return err
		}

		// Start a new goroutine to deal with rawConn
		// so we don't stall this Accept loop goroutine.
		go s.handleRawConn(rawConn)
	}
}

// handleRawConn is run in its own goroutine and handles a just-accepted
// connection that has not had any I/O performed on it yet.
func (s *Server) handleRawConn(rawConn net.Conn) {
	// Server在每一个Connection上都都做啥了
	// 1. 获取经过 TLS 封装的 Conn/AuthInfo等
	conn, authInfo, err := s.useTransportAuthenticator(rawConn)

	// HandShake Failed
	if err != nil {
		s.mu.Lock()
		s.errorf("ServerHandshake(%q) failed: %v", rawConn.RemoteAddr(), err)
		s.mu.Unlock()
		grpclog.Printf("grpc: Server.Serve failed to complete security handshake from %q: %v", rawConn.RemoteAddr(), err)
		rawConn.Close()
		return
	}

	// 服务关闭
	s.mu.Lock()
	if s.conns == nil {
		s.mu.Unlock()
		conn.Close()
		return
	}
	s.mu.Unlock()


	// 2. 对外提供服务
	if s.opts.useHandlerImpl {
		s.serveUsingHandler(conn)
	} else {
		s.serveNewHTTP2Transport(conn, authInfo)
	}
}

// serveNewHTTP2Transport sets up a new http/2 transport (using the
// gRPC http2 server transport in transport/http2_server.go) and
// serves streams on it.
// This is run in its own goroutine (it does network I/O in
// transport.NewServerTransport).
func (s *Server) serveNewHTTP2Transport(c net.Conn, authInfo credentials.AuthInfo) {

	// 1. 在TCP等conn的基础上，实现了基于http2的Transport
	//    transport.ServerTransport 的具体实现
	st, err := transport.NewServerTransport("http2", c, s.opts.maxConcurrentStreams, authInfo)

	if err != nil {
		s.mu.Lock()
		s.errorf("NewServerTransport(%q) failed: %v", c.RemoteAddr(), err)
		s.mu.Unlock()
		c.Close()
		grpclog.Println("grpc: Server.Serve failed to create ServerTransport: ", err)
		return
	}

	// 2. 添加transport, 如果Server被关闭，则返回false
	if !s.addConn(st) {
		st.Close()
		return
	}

	// 3. 在conn上 Server Streams(处理连续不断的请求)
	s.serveStreams(st)
}

func (s *Server) serveStreams(st transport.ServerTransport) {
	defer s.removeConn(st)
	defer st.Close()

	var wg sync.WaitGroup
	// 可以认为: 在Transport上维持一个长连接，然后接下来就处理一个个Request/Stream
	st.HandleStreams(func(stream *transport.Stream) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// stream是不是一个个被分割的请求
			// 将st的Stream交给s进行处理
			s.handleStream(st, stream, s.traceInfo(st, stream))
		}()
	})
	wg.Wait()
}

var _ http.Handler = (*Server)(nil)

// serveUsingHandler is called from handleRawConn when s is configured
// to handle requests via the http.Handler interface. It sets up a
// net/http.Server to handle the just-accepted conn. The http.Server
// is configured to route all incoming requests (all HTTP/2 streams)
// to ServeHTTP, which creates a new ServerTransport for each stream.
// serveUsingHandler blocks until conn closes.
//
// This codepath is only used when Server.TestingUseHandlerImpl has
// been configured. This lets the end2end tests exercise the ServeHTTP
// method as one of the environment types.
//
// conn is the *tls.Conn that's already been authenticated.
func (s *Server) serveUsingHandler(conn net.Conn) {
	if !s.addConn(conn) {
		conn.Close()
		return
	}
	defer s.removeConn(conn)

	// 1. 启动一个http2服务
	h2s := &http2.Server{
		MaxConcurrentStreams: s.opts.maxConcurrentStreams,
	}

	// 2. 通过Handler的形式来对外提供服务
	//    func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request)
	h2s.ServeConn(conn, &http2.ServeConnOpts{
		Handler: s,
	})
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	// 从: Request, Writer 获取Conn(Transport), 然后再它的基础上提供: Streams 服务
	st, err := transport.NewServerHandlerTransport(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !s.addConn(st) {
		st.Close()
		return
	}
	defer s.removeConn(st)
	s.serveStreams(st)
}

// traceInfo returns a traceInfo and associates it with stream, if tracing is enabled.
// If tracing is not enabled, it returns nil.
func (s *Server) traceInfo(st transport.ServerTransport, stream *transport.Stream) (trInfo *traceInfo) {
	if !EnableTracing {
		return nil
	}
	trInfo = &traceInfo{
		tr: trace.New("grpc.Recv." + methodFamily(stream.Method()), stream.Method()),
	}
	trInfo.firstLine.client = false
	trInfo.firstLine.remoteAddr = st.RemoteAddr()
	stream.TraceContext(trInfo.tr)
	if dl, ok := stream.Context().Deadline(); ok {
		trInfo.firstLine.deadline = dl.Sub(time.Now())
	}
	return trInfo
}

// 主要是检查: 是否出现 conns 为 nil的情况，也就是服务被关闭的情况
func (s *Server) addConn(c io.Closer) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conns == nil {
		return false
	}
	s.conns[c] = true
	return true
}

func (s *Server) removeConn(c io.Closer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conns != nil {
		delete(s.conns, c)
	}
}

func (s *Server) sendResponse(t transport.ServerTransport, stream *transport.Stream, msg interface{}, cp Compressor, opts *transport.Options) error {
	var cbuf *bytes.Buffer
	if cp != nil {
		cbuf = new(bytes.Buffer)
	}
	p, err := encode(s.opts.codec, msg, cp, cbuf)
	if err != nil {
		// This typically indicates a fatal issue (e.g., memory
		// corruption or hardware faults) the application program
		// cannot handle.
		//
		// TODO(zhaoq): There exist other options also such as only closing the
		// faulty stream locally and remotely (Other streams can keep going). Find
		// the optimal option.
		grpclog.Fatalf("grpc: Server failed to encode response %v", err)
	}
	return t.Write(stream, p, opts)
}


//
// service存在, MethodDesc也存在
//
func (s *Server) processUnaryRPC(t transport.ServerTransport, stream *transport.Stream, srv *service, md *MethodDesc, trInfo *traceInfo) (err error) {
	if trInfo != nil {
		defer trInfo.tr.Finish()
		trInfo.firstLine.client = false
		trInfo.tr.LazyLog(&trInfo.firstLine, false)
		defer func() {
			if err != nil && err != io.EOF {
				trInfo.tr.LazyLog(&fmtStringer{"%v", []interface{}{err}}, true)
				trInfo.tr.SetError()
			}
		}()
	}
	p := &parser{r: stream}
	for {
		// PayloadFormat, Request Data, Err
		pf, req, err := p.recvMsg()
		if err == io.EOF {
			// The entire stream is done (for unary RPC only).
			return err
		}
		if err == io.ErrUnexpectedEOF {
			err = transport.StreamError{Code: codes.Internal, Desc: "io.ErrUnexpectedEOF"}
		}
		if err != nil {
			switch err := err.(type) {
			case transport.ConnectionError:
			// Nothing to do here.
			// 连接断开了，则直接返回
			case transport.StreamError:
				if err := t.WriteStatus(stream, err.Code, err.Desc); err != nil {
					grpclog.Printf("grpc: Server.processUnaryRPC failed to write status %v", err)
				}
			default:
				panic(fmt.Sprintf("grpc: Unexpected error (%T) from recvMsg: %v", err, err))
			}
			return err
		}

		if err := checkRecvPayload(pf, stream.RecvCompress(), s.opts.dc); err != nil {
			switch err := err.(type) {
			case transport.StreamError:
				if err := t.WriteStatus(stream, err.Code, err.Desc); err != nil {
					grpclog.Printf("grpc: Server.processUnaryRPC failed to write status %v", err)
				}
			default:
				if err := t.WriteStatus(stream, codes.Internal, err.Error()); err != nil {
					grpclog.Printf("grpc: Server.processUnaryRPC failed to write status %v", err)
				}

			}
			return err
		}

		// 处理正常的Method Invocation
		statusCode := codes.OK
		statusDesc := ""

		// Decode Input
		df := func(v interface{}) error {
			if pf == compressionMade {
				// 如果数据有压缩，则通过opts中配置的Compressor/Decompressor进行处理
				var err error
				req, err = s.opts.dc.Do(bytes.NewReader(req))
				if err != nil {
					if err := t.WriteStatus(stream, codes.Internal, err.Error()); err != nil {
						grpclog.Printf("grpc: Server.processUnaryRPC failed to write status %v", err)
					}
					return err
				}
			}

			// 将req中的数据反序列化到: v 中，例如:
			//	in := new(HelloRequest)
			//	if err := dec(in); err != nil {
			//		return nil, err
			//	}
			if err := s.opts.codec.Unmarshal(req, v); err != nil {
				return err
			}

			// 打印trace info
			if trInfo != nil {
				trInfo.tr.LazyLog(&payload{sent: false, msg: v}, true)
			}
			return nil
		}


		// 参考: func _Greeter_SayHello_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error) (interface{}, error) {
		//var _Greeter_serviceDesc = grpc.ServiceDesc{
		//	ServiceName: "helloworld.Greeter",
		//	HandlerType: (*GreeterServer)(nil),
		//	Methods: []grpc.MethodDesc{
		//		{
		//			// 方法的描述
		//			MethodName: "SayHello",
		//			Handler:    _Greeter_SayHello_Handler,
		//		},
		//	},
		//
		//	// Streams如何处理呢?
		//	Streams: []grpc.StreamDesc{},
		//}
		// Handler是直接可调用的函数
		// 其中: df 接受输入参数的类型，进行数据的反序列化
		//
		reply, appErr := md.Handler(srv.server, stream.Context(), df)

		// 如果处理应用返回来的Error
		if appErr != nil {
			if err, ok := appErr.(rpcError); ok {
				statusCode = err.code
				statusDesc = err.desc
			} else {
				statusCode = convertCode(appErr)
				statusDesc = appErr.Error()
			}
			if trInfo != nil && statusCode != codes.OK {
				trInfo.tr.LazyLog(stringer(statusDesc), true)
				trInfo.tr.SetError()
			}
			if err := t.WriteStatus(stream, statusCode, statusDesc); err != nil {
				grpclog.Printf("grpc: Server.processUnaryRPC failed to write status: %v", err)
				return err
			}
			return nil
		}

		// 处理完毕之后，正常返回数据
		if trInfo != nil {
			trInfo.tr.LazyLog(stringer("OK"), false)
		}
		opts := &transport.Options{
			Last:  true,
			Delay: false,
		}
		if s.opts.cp != nil {
			stream.SetSendCompress(s.opts.cp.Type())
		}

		// 返回数据
		if err := s.sendResponse(t, stream, reply, s.opts.cp, opts); err != nil {
			switch err := err.(type) {
			case transport.ConnectionError:
			// Nothing to do here.
			case transport.StreamError:
				statusCode = err.Code
				statusDesc = err.Desc
			default:
				statusCode = codes.Unknown
				statusDesc = err.Error()
			}
			return err
		}
		if trInfo != nil {
			trInfo.tr.LazyLog(&payload{sent: true, msg: reply}, true)
		}
		return t.WriteStatus(stream, statusCode, statusDesc)
	}
}

func (s *Server) processStreamingRPC(t transport.ServerTransport, stream *transport.Stream, srv *service, sd *StreamDesc, trInfo *traceInfo) (err error) {
	if s.opts.cp != nil {
		stream.SetSendCompress(s.opts.cp.Type())
	}
	ss := &serverStream{
		t:      t,
		s:      stream,
		p:      &parser{r: stream},
		codec:  s.opts.codec,
		cp:     s.opts.cp,
		dc:     s.opts.dc,
		trInfo: trInfo,
	}
	if ss.cp != nil {
		ss.cbuf = new(bytes.Buffer)
	}
	if trInfo != nil {
		trInfo.tr.LazyLog(&trInfo.firstLine, false)
		defer func() {
			ss.mu.Lock()
			if err != nil && err != io.EOF {
				ss.trInfo.tr.LazyLog(&fmtStringer{"%v", []interface{}{err}}, true)
				ss.trInfo.tr.SetError()
			}
			ss.trInfo.tr.Finish()
			ss.trInfo.tr = nil
			ss.mu.Unlock()
		}()
	}
	if appErr := sd.Handler(srv.server, ss); appErr != nil {
		if err, ok := appErr.(rpcError); ok {
			ss.statusCode = err.code
			ss.statusDesc = err.desc
		} else if err, ok := appErr.(transport.StreamError); ok {
			ss.statusCode = err.Code
			ss.statusDesc = err.Desc
		} else {
			ss.statusCode = convertCode(appErr)
			ss.statusDesc = appErr.Error()
		}
	}
	if trInfo != nil {
		ss.mu.Lock()
		if ss.statusCode != codes.OK {
			ss.trInfo.tr.LazyLog(stringer(ss.statusDesc), true)
			ss.trInfo.tr.SetError()
		} else {
			ss.trInfo.tr.LazyLog(stringer("OK"), false)
		}
		ss.mu.Unlock()
	}
	return t.WriteStatus(ss.s, ss.statusCode, ss.statusDesc)

}


//
// 如何处理Stream呢?
//
func (s *Server) handleStream(t transport.ServerTransport, stream *transport.Stream, trInfo *traceInfo) {
	sm := stream.Method()
	if sm != "" && sm[0] == '/' {
		sm = sm[1:]
	}
	pos := strings.LastIndex(sm, "/")
	if pos == -1 {
		if trInfo != nil {
			trInfo.tr.LazyLog(&fmtStringer{"Malformed method name %q", []interface{}{sm}}, true)
			trInfo.tr.SetError()
		}
		if err := t.WriteStatus(stream, codes.InvalidArgument, fmt.Sprintf("malformed method name: %q", stream.Method())); err != nil {
			if trInfo != nil {
				trInfo.tr.LazyLog(&fmtStringer{"%v", []interface{}{err}}, true)
				trInfo.tr.SetError()
			}
			grpclog.Printf("grpc: Server.handleStream failed to write status: %v", err)
		}
		if trInfo != nil {
			trInfo.tr.Finish()
		}
		return
	}


	// 例如: helloworld.Greeter/method
	service := sm[:pos]
	method := sm[pos + 1:]

	// 1. 获取具体的Service
	srv, ok := s.m[service]
	// 1.1 如果Service不存在，则通过: WriteStatus 返回错误状态
	if !ok {
		if trInfo != nil {
			trInfo.tr.LazyLog(&fmtStringer{"Unknown service %v", []interface{}{service}}, true)
			trInfo.tr.SetError()
		}

		// 处理异常
		if err := t.WriteStatus(stream, codes.Unimplemented, fmt.Sprintf("unknown service %v", service)); err != nil {
			if trInfo != nil {
				trInfo.tr.LazyLog(&fmtStringer{"%v", []interface{}{err}}, true)
				trInfo.tr.SetError()
			}
			grpclog.Printf("grpc: Server.handleStream failed to write status: %v", err)
		}
		if trInfo != nil {
			trInfo.tr.Finish()
		}
		return
	}
	// Unary RPC or Streaming RPC?

	// 2. 处理Method
	if md, ok := srv.md[method]; ok {
		s.processUnaryRPC(t, stream, srv, md, trInfo)
		return
	}

	// 3. 处理Stream
	if sd, ok := srv.sd[method]; ok {
		s.processStreamingRPC(t, stream, srv, sd, trInfo)
		return
	}

	// 2,3.1 处理未实现的Method
	if trInfo != nil {
		trInfo.tr.LazyLog(&fmtStringer{"Unknown method %v", []interface{}{method}}, true)
		trInfo.tr.SetError()
	}
	if err := t.WriteStatus(stream, codes.Unimplemented, fmt.Sprintf("unknown method %v", method)); err != nil {
		if trInfo != nil {
			trInfo.tr.LazyLog(&fmtStringer{"%v", []interface{}{err}}, true)
			trInfo.tr.SetError()
		}
		grpclog.Printf("grpc: Server.handleStream failed to write status: %v", err)
	}
	if trInfo != nil {
		trInfo.tr.Finish()
	}
}

// Stop stops the gRPC server. It immediately closes all open
// connections and listeners.
// It cancels all active RPCs on the server side and the corresponding
// pending RPCs on the client side will get notified by connection
// errors.
func (s *Server) Stop() {
	s.mu.Lock()
	listeners := s.lis
	s.lis = nil
	cs := s.conns
	s.conns = nil
	s.mu.Unlock()

	for lis := range listeners {
		lis.Close()
	}
	for c := range cs {
		c.Close()
	}

	s.mu.Lock()
	if s.events != nil {
		s.events.Finish()
		s.events = nil
	}
	s.mu.Unlock()
}

func init() {
	internal.TestingCloseConns = func(arg interface{}) {
		arg.(*Server).testingCloseConns()
	}
	internal.TestingUseHandlerImpl = func(arg interface{}) {
		arg.(*Server).opts.useHandlerImpl = true
	}
}

// testingCloseConns closes all existing transports but keeps s.lis
// accepting new connections.
func (s *Server) testingCloseConns() {
	s.mu.Lock()
	for c := range s.conns {
		c.Close()
		delete(s.conns, c)
	}
	s.mu.Unlock()
}

// SendHeader sends header metadata. It may be called at most once from a unary
// RPC handler. The ctx is the RPC handler's Context or one derived from it.
func SendHeader(ctx context.Context, md metadata.MD) error {
	if md.Len() == 0 {
		return nil
	}
	stream, ok := transport.StreamFromContext(ctx)
	if !ok {
		return fmt.Errorf("grpc: failed to fetch the stream from the context %v", ctx)
	}
	t := stream.ServerTransport()
	if t == nil {
		grpclog.Fatalf("grpc: SendHeader: %v has no ServerTransport to send header metadata.", stream)
	}
	return t.WriteHeader(stream, md)
}

// SetTrailer sets the trailer metadata that will be sent when an RPC returns.
// It may be called at most once from a unary RPC handler. The ctx is the RPC
// handler's Context or one derived from it.
func SetTrailer(ctx context.Context, md metadata.MD) error {
	if md.Len() == 0 {
		return nil
	}
	stream, ok := transport.StreamFromContext(ctx)
	if !ok {
		return fmt.Errorf("grpc: failed to fetch the stream from the context %v", ctx)
	}
	return stream.SetTrailer(md)
}
