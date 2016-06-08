package listener

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/jeffpierce/cassabon/config"
	"github.com/jeffpierce/cassabon/logging"
)

func TestCarbonSocket(t *testing.T) {

	config.G.Log.System = logging.NewLogger("system")
	config.G.Log.System.Open("", logging.Info)
	logging.Statsd.Open("", "", "cassabon")

	fmt.Println("Testing TCP socket connection...")
	cpl := new(CarbonPlaintextListener)
	go cpl.carbonTCP("127.0.0.1:2003")

	fmt.Println("Testing UDP socket connection...")
	go cpl.carbonUDP("127.0.0.1:2003")

	time.Sleep(10)

	fmt.Println("Sleeping while the connections are opened.")

	for i := 0; i < 10; i++ {
		fmt.Println("Sending good metric to TCP...")
		if tcpconn, err := net.Dial("tcp", "127.0.0.1:2003"); err == nil {
			GoodMetric(tcpconn)
			tcpconn.Close()
		} else {
			fmt.Printf("Unable to open TCP connection: %s\n", err.Error())
		}
	}

	fmt.Println("Sending bad metric to TCP...")
	if tcpconnbad, err := net.Dial("tcp", "127.0.0.1:2003"); err == nil {
		BadMetric(tcpconnbad)
		tcpconnbad.Close()
	} else {
		fmt.Printf("Unable to open TCP connection: %s\n", err.Error())
	}

	fmt.Println("Sending good metric to UDP...")
	if udpconn, err := net.Dial("udp", "127.0.0.1:2003"); err == nil {
		GoodMetric(udpconn)
	} else {
		fmt.Printf("Unable to open UDP connection: %s\n", err.Error())
	}

	fmt.Println("Sending bad metric to UDP...")
	if udpconnbad, err := net.Dial("udp", "127.0.0.1:2003"); err == nil {
		BadMetric(udpconnbad)
	} else {
		fmt.Printf("Unable to open UDP connection: %s\n", err.Error())
	}

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
