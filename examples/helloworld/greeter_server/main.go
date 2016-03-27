package main

import (
	"log"
	"net"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	pb "google.golang.org/grpc/examples/helloworld/helloworld"
)

const (
	port = ":50051"
)

// server is used to implement helloworld.GreeterServer.
type server struct{}

// SayHello implements helloworld.GreeterServer
// 这里的Context有什么作用呢?
//
func (s *server) SayHello(ctx context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	return &pb.HelloReply{Message: "Hello " + in.Name}, nil
}

func main() {
	// 原生的listener
	// 至于如何利用每个connection确实也不是listener的事情
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// 1. 启动一个Server, 关联: listener和handler
	s := grpc.NewServer()
	pb.RegisterGreeterServer(s, &server{})

	// 2. 提供服务
	s.Serve(lis)
}
