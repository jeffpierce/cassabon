package listener

import (
	"github.com/jeffpierce/cassabon/config"
)

// StubbornTCPConn wraps a TCP client connection to persistently retry dropped connections.
type StubbornTCPConn struct {
	hostPort string // Host:port of the remote server
}

// Open sets up the parameters used by the connection retrying code.
func (sc *StubbornTCPConn) Open(hostPort string) {
	sc.hostPort = hostPort
	config.G.Log.System.LogInfo("StubbornTCPConn::Open() %s", sc.hostPort)
}

// Close ensures that the underlying TCP connection is in the closed state.
func (sc *StubbornTCPConn) Close() {
	config.G.Log.System.LogInfo("StubbornTCPConn::Close() %s", sc.hostPort)
}

// Send attempts to send data, retrying as necessary.
func (sc *StubbornTCPConn) Send(line string) {
	config.G.Log.System.LogInfo("StubbornTCPConn::Send() %s %s", sc.hostPort, line)
}
