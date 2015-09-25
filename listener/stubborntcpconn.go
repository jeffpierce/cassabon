package listener

import (
	"fmt"
	"net"

	"github.com/jeffpierce/cassabon/config"
)

// StubbornTCPConn wraps a TCP client connection to persistently retry dropped connections.
type StubbornTCPConn struct {
	hostPort   string       // Host:port of the remote server
	isOpen     bool         // True when the underlying TCP connection has been successfully opened
	openFailed bool         // True after an open fails, to throttle subsequent messages
	addr       *net.TCPAddr // Native Go version of the peer TCP address
	conn       *net.TCPConn // The underlying TCP connection
}

// Open sets up the parameters used by the connection retrying code.
func (sc *StubbornTCPConn) Open(hostPort string) {
	sc.hostPort = hostPort
	sc.addr, _ = net.ResolveTCPAddr("tcp4", sc.hostPort)
	config.G.Log.System.LogInfo("Opening peer connection to %s", sc.hostPort)
	sc.internalOpen()
}

// Close ensures that the underlying TCP connection is in the closed state.
func (sc *StubbornTCPConn) Close() {
	config.G.Log.System.LogInfo("Closing peer connection to %s", sc.hostPort)
	if sc.isOpen {
		sc.conn.Close()
	}
	sc.isOpen = false
}

// Send attempts to send data, retrying as necessary.
func (sc *StubbornTCPConn) Send(line string) {

	// If the write fails, try a second time after re-opening the connection.
	retriesRemaining := 2
	for retriesRemaining > 0 {

		// If not open, a retry is indicated.
		if !sc.isOpen {
			if err := sc.internalOpen(); err == nil {
				config.G.Log.System.LogInfo("Peer connection to %s resumed", sc.hostPort)
			}
		}

		// If already open or now open, send the current line.
		if sc.isOpen {
			if _, err := fmt.Fprintf(sc.conn, "%s\n", line); err != nil {
				config.G.Log.System.LogWarn("Peer connection to %s failed: %v", sc.hostPort, err)
				sc.Close()
			} else {
				// The write succeeded, ensure we don't double-write.
				retriesRemaining--
			}
		}
		retriesRemaining--
	}
}

func (sc *StubbornTCPConn) internalOpen() error {
	var err error
	if sc.conn, err = net.DialTCP("tcp4", nil, sc.addr); err == nil {
		sc.isOpen = true
		sc.openFailed = false
	} else {
		if !sc.openFailed {
			// Only report this once, otherwise it gets really noisy.
			config.G.Log.System.LogWarn("Unable to make peer connection to %s: %v", sc.hostPort, err)
			sc.openFailed = true
		}
	}
	return err
}
