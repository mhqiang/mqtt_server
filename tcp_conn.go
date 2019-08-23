package mqtt_server

import (
	"bytes"
	"io"
	"net"
	"sync"
	"time"
)

type ConnSet map[net.Conn]struct{}

type TCPConn struct {
	io.Reader //Read(p []byte) (n int, err error)
	io.Writer //Write(p []byte) (n int, err error)
	sync.Mutex
	buf_lock  chan bool //当有写入一次数据设置一次
	buffer    bytes.Buffer
	conn      net.Conn
	closeFlag bool
}

func newTCPConn(conn net.Conn) *TCPConn {
	tcpConn := new(TCPConn)
	tcpConn.conn = conn
	conn.RemoteAddr()

	return tcpConn
}

func (tcpConn *TCPConn) doDestroy() {
	tcpConn.conn.(*net.TCPConn).SetLinger(0)
	tcpConn.conn.Close()

	if !tcpConn.closeFlag {
		tcpConn.closeFlag = true
	}
}

func (tcpConn *TCPConn) Destroy() {
	tcpConn.Lock()
	defer tcpConn.Unlock()

	tcpConn.doDestroy()
}

func (tcpConn *TCPConn) Close() error {
	tcpConn.Lock()
	defer tcpConn.Unlock()
	if tcpConn.closeFlag {
		return nil
	}

	tcpConn.closeFlag = true
	return tcpConn.conn.Close()
}

// b must not be modified by the others goroutines
func (tcpConn *TCPConn) Write(b []byte) (n int, err error) {
	tcpConn.Lock()
	defer tcpConn.Unlock()
	if tcpConn.closeFlag || b == nil {
		return
	}

	return tcpConn.conn.Write(b)
}

func (tcpConn *TCPConn) Read(b []byte) (int, error) {
	return tcpConn.conn.Read(b)
}

func (tcpConn *TCPConn) LocalAddr() net.Addr {
	return tcpConn.conn.LocalAddr()
}

func (tcpConn *TCPConn) RemoteAddr() net.Addr {
	return tcpConn.conn.RemoteAddr()
}

// A zero value for t means I/O operations will not time out.
func (tcpConn *TCPConn) SetDeadline(t time.Time) error {
	return tcpConn.conn.SetDeadline(t)
}

// SetReadDeadline sets the deadline for future Read calls.
// A zero value for t means Read will not time out.
func (tcpConn *TCPConn) SetReadDeadline(t time.Time) error {
	return tcpConn.conn.SetReadDeadline(t)
}

// SetWriteDeadline sets the deadline for future Write calls.
// Even if write times out, it may return n > 0, indicating that
// some of the data was successfully written.
// A zero value for t means Write will not time out.
func (tcpConn *TCPConn) SetWriteDeadline(t time.Time) error {
	return tcpConn.conn.SetWriteDeadline(t)
}
