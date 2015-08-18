package listener

import (
	"fmt"
	"net"
	"testing"
	"time"
)

func TestCarbonSocket(t *testing.T) {
	fmt.Println("Testing TCP socket connection...")
	go CarbonTCP("127.0.0.1", "2003")

	//	fmt.Println("Testing UDP socket connection...")
	//	go CarbonUDP("127.0.0.1", 2003)

	time.Sleep(10)

	fmt.Println("Sleeping while the connections are opened.")

	for i := 0; i < 10; i++ {
		fmt.Println("Sending good metric to TCP...")
		tcpconn, _ := net.Dial("tcp", "127.0.0.1:2003")
		GoodMetric(tcpconn)
		tcpconn.Close()
	}

	fmt.Println("Sending bad metric to TCP...")
	tcpconnbad, _ := net.Dial("tcp", "127.0.0.1:2003")
	BadMetric(tcpconnbad)

	/*	fmt.Println("Sending good metric to UDP...")
		udpconn, err := net.Dial("udp", "127.0.0.1:2003")
		GoodMetric(udpconn)

		fmt.Println("Sending bad metric to UDP...")
		udpconnbad, err := net.Dial("udp", "127.0.0.1:2003")
		BadMetric(udpconnbad) */

	time.Sleep(100 * time.Millisecond)
}

func GoodMetric(conn net.Conn) {
	testMetric := fmt.Sprintf("carbon.test 1 %d", time.Now().Unix())
	fmt.Println("Sending metric:", testMetric)
	fmt.Fprintf(conn, testMetric+"\n")
}

func BadMetric(conn net.Conn) {
	testMetric := "carbon.terrible 9 Qsplork"
	fmt.Println("Sending bad metric:", testMetric)
	fmt.Fprintf(conn, testMetric+"\n")
	conn.Close()
}
