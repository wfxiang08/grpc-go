package main

import (
	"flag"
	"io"
	"math/rand"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	pb "google.golang.org/grpc/examples/route_guide/routeguide"
	"google.golang.org/grpc/grpclog"
)

var (
	tls                = flag.Bool("tls", false, "Connection uses TLS if true, else plain TCP")
	caFile             = flag.String("ca_file", "testdata/ca.pem", "The file containning the CA root cert file")
	serverAddr         = flag.String("server_addr", "127.0.0.1:10000", "The server address in the format of host:port")
	serverHostOverride = flag.String("server_host_override", "x.test.youtube.com", "The server name use to verify the hostname returned by TLS handshake")
)

// printFeature gets the feature for the given point.
func printFeature(client pb.RouteGuideClient, point *pb.Point) {
	grpclog.Printf("Getting feature for point (%d, %d)", point.Latitude, point.Longitude)

	feature, err := client.GetFeature(context.Background(), point)
	if err != nil {
		grpclog.Fatalf("%v.GetFeatures(_) = _, %v: ", client, err)
	}
	grpclog.Println(feature)
}

// printFeatures lists all the features within the given bounding Rectangle.
func printFeatures(client pb.RouteGuideClient, rect *pb.Rectangle) {
	grpclog.Printf("Looking for features within %v", rect)
	stream, err := client.ListFeatures(context.Background(), rect)
	if err != nil {
		grpclog.Fatalf("%v.ListFeatures(_) = _, %v", client, err)
	}
	for {
		feature, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			grpclog.Fatalf("%v.ListFeatures(_) = _, %v", client, err)
		}
		grpclog.Println(feature)
	}
}

// runRecordRoute sends a sequence of points to server and expects to get a RouteSummary from server.
func runRecordRoute(client pb.RouteGuideClient) {
	// Create a random number of random points
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	pointCount := int(r.Int31n(100)) + 2 // Traverse at least two points
	var points []*pb.Point
	for i := 0; i < pointCount; i++ {
		points = append(points, randomPoint(r))
	}
	grpclog.Printf("Traversing %d points.", len(points))

	// 通过Stream的方式将数据发送到服务器
	stream, err := client.RecordRoute(context.Background())
	if err != nil {
		grpclog.Fatalf("%v.RecordRoute(_) = _, %v", client, err)
	}
	for _, point := range points {
		if err := stream.Send(point); err != nil {
			grpclog.Fatalf("%v.Send(%v) = %v", stream, point, err)
		}
	}

	// 一口气发送完毕
	reply, err := stream.CloseAndRecv()
	if err != nil {
		grpclog.Fatalf("%v.CloseAndRecv() got error %v, want %v", stream, err, nil)
	}
	grpclog.Printf("Route summary: %v", reply)
}

// runRouteChat receives a sequence of route notes, while sending notes for various locations.
func runRouteChat(client pb.RouteGuideClient) {
	notes := []*pb.RouteNote{
		{&pb.Point{0, 1}, "First message"},
		{&pb.Point{0, 2}, "Second message"},
		{&pb.Point{0, 3}, "Third message"},
		{&pb.Point{0, 1}, "Fourth message"},
		{&pb.Point{0, 2}, "Fifth message"},
		{&pb.Point{0, 3}, "Sixth message"},
	}
	stream, err := client.RouteChat(context.Background())
	if err != nil {
		grpclog.Fatalf("%v.RouteChat(_) = _, %v", client, err)
	}
	waitc := make(chan struct{})
	go func() {
		for {
			in, err := stream.Recv()
			if err == io.EOF {
				// read done.
				close(waitc)
				return
			}
			if err != nil {
				grpclog.Fatalf("Failed to receive a note : %v", err)
			}
			grpclog.Printf("Got message %s at point(%d, %d)", in.Message, in.Location.Latitude, in.Location.Longitude)
		}
	}()

	// 发送数据&关闭连接
	for _, note := range notes {
		if err := stream.Send(note); err != nil {
			grpclog.Fatalf("Failed to send a note: %v", err)
		}
	}
	stream.CloseSend()

	// 等待: stream的Close(数据读取完毕
	<-waitc
}

func randomPoint(r *rand.Rand) *pb.Point {
	lat := (r.Int31n(180) - 90) * 1e7
	long := (r.Int31n(360) - 180) * 1e7
	return &pb.Point{lat, long}
}

func main() {
	flag.Parse()
	var opts []grpc.DialOption

	// 如何使用: TLS呢?
	if *tls {
		var sn string
		if *serverHostOverride != "" {
			sn = *serverHostOverride
		}
		var creds credentials.TransportAuthenticator
		if *caFile != "" {
			var err error
			creds, err = credentials.NewClientTLSFromFile(*caFile, sn)
			if err != nil {
				grpclog.Fatalf("Failed to create TLS credentials %v", err)
			}
		} else {
			creds = credentials.NewClientTLSFromCert(nil, sn)
		}
		opts = append(opts, grpc.WithTransportCredentials(creds))
	} else {
		opts = append(opts, grpc.WithInsecure())
	}

	// 2. 连接服务器
	conn, err := grpc.Dial(*serverAddr, opts...)
	if err != nil {
		grpclog.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()
	client := pb.NewRouteGuideClient(conn)

	// Looking for a valid feature
	printFeature(client, &pb.Point{409146138, -746188906})

	// Feature missing.
	printFeature(client, &pb.Point{0, 0})

	// Looking for features between 40, -75 and 42, -73.
	printFeatures(client, &pb.Rectangle{&pb.Point{400000000, -750000000}, &pb.Point{420000000, -730000000}})

	// RecordRoute
	runRecordRoute(client)

	// RouteChat
	runRouteChat(client)
}
