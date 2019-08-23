package mqtt_server

import (
	"crypto/tls"
	"net"
	"sync"
	"time"

	"github.com/allegro/bigcache"
	"go.uber.org/zap"
)

type TCPServer struct {
	Addr       string
	Tls        bool //是否支持tls
	CertFile   string
	KeyFile    string
	MaxConnNum int // 最多允许的链接客户端
	// NewAgent   func(*TCPConn) Agent
	ConnectionCache *ConnectionCache // 所有链接的存储
	TopicMap map[string]Topic
	Log           *LogStruct
	ln            net.Listener
	wgLn    sync.WaitGroup
	wgConns sync.WaitGroup
}

func (server *TCPServer) Start() {
	server.init()
	server.Log.Info("TCP Listen : %v",  server.Addr))
	go server.run()
}

func (server *TCPServer) init()error {
	ln, err := net.Listen("tcp", server.Addr)
	if err != nil {
		server.Log.Warning("%v", err)
			return err
	}

	if server.NewAgent == nil {
		server.Log.Warning("NewAgent must not be nil")
	}
	if server.Tls {
		tlsConf := new(tls.Config)
		tlsConf.Certificates = make([]tls.Certificate, 1)
		tlsConf.Certificates[0], err = tls.LoadX509KeyPair(server.CertFile, server.KeyFile)
		if err == nil {
			ln = tls.NewListener(ln, tlsConf)
			server.Log.Info("TCP Listen TLS load success")
		} else {
			server.Log.Warning("tcp_server tls :%v", err)
		}
	}
	connectionCache,err:= InitCache()
	if err != nil{
		server.Log.Err("init connection err", err)
		return err
	}
	servver.ConnectionCache := connectionCache
	server.TopicMap = InitTopic()
	server.ln = ln
}
func (server *TCPServer) run() {
	server.wgLn.Add(1)
	defer server.wgLn.Done()

	var tempDelay time.Duration
	for {
		conn, err := server.ln.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}
				if max := 1 * time.Second; tempDelay > max {
					tempDelay = max
				}
				server.Log.Info("accept error: %v; retrying in %v", err, tempDelay)
				time.Sleep(tempDelay)
				continue
			}
			return
		}
		tempDelay = 0
		tcpConn := newTCPConn(conn)
		agent := server.NewAgent(tcpConn)

		go server.AddConnection(conn)
	}
}


// 添加一个新的连接点
func (server *TCPServer) AddConnection(conn net.Conn){
	server.ConnectionCache.AddConn(conn)
}

func (server *TCPServer) Close() {
	server.ln.Close()
	server.wgLn.Wait()
	server.wgConns.Wait()
}
