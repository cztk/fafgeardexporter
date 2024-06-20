package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/log"
	"net"
	"net/http"
	"strconv"
	"strings"
)

const namespace = "fafgearclient"

var (
	// prom config
	listenAddress = flag.String("web.listen-address", ":9101",
		"Address to listen on for telemetry")
	metricsPath = flag.String("web.telemetry-path", "/metrics",
		"Path under which to expose metrics")
	gearAddress = flag.String("metric.fetch-address", "127.0.0.1:1370",
		"Address from where to collect statistics")
	// metrics
	metric_up = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "fafgeard_up"),
		"Was the last status request successful.",
		nil, nil,
	)
	metric_query_queue_size = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "server", "query_queue_size"),
		"How many queries are waiting to be processed.",
		nil, nil,
	)
	metric_database_connections_max = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "server", "database_connections_max"),
		"How many database connections are allowed in total.",
		nil, nil,
	)
	metric_database_connections_active = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "server", "database_connections_active"),
		"How many database connections are active at the moment.",
		nil, nil,
	)

	metric_threadpool_input_count = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "server", "threadpool_input_count"),
		"threadpool_input_count.",
		nil, nil,
	)
	metric_threadpool_input_running = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "server", "threadpool_input_running"),
		"threadpool_input_running.",
		nil, nil,
	)
	metric_threadpool_input_queued = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "server", "threadpool_input_queued"),
		"threadpool_input_queued.",
		nil, nil,
	)
	metric_threadpool_input_total = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "server", "threadpool_input_total"),
		"threadpool_input_total.",
		nil, nil,
	)

	metric_threadpool_database_count = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "server", "threadpool_database_count"),
		"threadpool_database_count.",
		nil, nil,
	)
	metric_threadpool_database_running = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "server", "threadpool_database_running"),
		"threadpool_database_running.",
		nil, nil,
	)
	metric_threadpool_database_queued = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "server", "threadpool_database_queued"),
		"threadpool_database_queued.",
		nil, nil,
	)
	metric_threadpool_database_total = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "server", "threadpool_database_total"),
		"threadpool_database_total.",
		nil, nil,
	)
)

func socketSend(conn net.Conn, bytes []byte) bool {
	write, err := conn.Write(bytes)
	if nil != err {
		return false
	}
	if 0 == write {
		return false
	}
	return true
}

func socketRead(conn net.Conn, length int) (bool, string) {
	readbuf := make([]byte, length)
	read, err := conn.Read(readbuf)
	if nil != err {
		return false, ""
	}
	readstr := string(readbuf[:])
	if read != length {
		return false, readstr
	}
	return true, readstr
}

func sendProtocol(conn net.Conn, protocol int8) bool {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, protocol)
	if nil != err {
		return false
	}

	if socketSend(conn, buf.Bytes()) {
		suc, _ := socketRead(conn, 1)
		if suc {
			//fmt.Printf("sending proto version worked appearently\n")
			//fmt.Printf("response was: %s\n", str)
			return true
		} else {
			//fmt.Printf("sending proto version did not work\n")
			//fmt.Printf("response was: %s\n", str)
		}
	}
	return false
}

func sendStatusRequest(conn net.Conn) (bool, int8) {
	buf := new(bytes.Buffer)

	var packettype int8 = 1
	var datalen int32 = 0

	err := binary.Write(buf, binary.LittleEndian, packettype)
	if nil != err {
		return false, 0
	}
	err = binary.Write(buf, binary.LittleEndian, datalen)
	if nil != err {
		return false, 0
	}

	if socketSend(conn, buf.Bytes()) {
		suc, str := socketRead(conn, 1)
		if suc {
			i := int8(str[0])
			//fmt.Printf("sending status request worked appearently\n")
			//fmt.Printf("response was: %s\n", str)
			return true, i
		} else {
			//fmt.Printf("sending status request did not work\n")
			//fmt.Printf("response was: %s\n", str)
		}
	}
	return false, 0
}

func readStatusCsv(conn net.Conn, len int) (bool, string) {
	suc, str := socketRead(conn, len)
	if suc {
		//fmt.Printf("response ok: %s\n", str)
		return true, str
	} else {
		//fmt.Printf("response error: %s\n", str)
		return false, str
	}
}

func getStatus(address string) (bool, string) {
	conn, err := net.Dial("tcp", address)
	if nil != err {
		return false, ""
	}

	var protocol int8 = 1

	if false == sendProtocol(conn, protocol) {
		return false, ""
	}
	suc, statuslen := sendStatusRequest(conn)
	if false == suc {
		return false, ""
	}
	suc, statuscsv := readStatusCsv(conn, int(statuslen))

	err = conn.Close()
	if err != nil {
		return suc, statuscsv
	}

	return suc, statuscsv
}

type Exporter struct {
}

func NewExporter() *Exporter {
	return &Exporter{}
}

func (e *Exporter) Collect(ch chan<- prometheus.Metric) {

	value_query_queue_size := 0
	value_database_connections_max := 0
	value_database_connections_active := 0
	value_threadpool_input_count := 0
	value_threadpool_input_running := 0
	value_threadpool_input_queued := 0
	value_threadpool_input_total := 0
	value_threadpool_database_count := 0
	value_threadpool_database_running := 0
	value_threadpool_database_queued := 0
	value_threadpool_database_total := 0

	suc, csv := getStatus(*gearAddress)
	if suc {
		ch <- prometheus.MustNewConstMetric(metric_up, prometheus.GaugeValue, 1)
		//TODO check size of s
		s := strings.Split(csv, ";")
		i := 0
		value_query_queue_size, _ = strconv.Atoi(s[i])
		i++
		value_database_connections_max, _ = strconv.Atoi(s[i])
		i++
		value_database_connections_active, _ = strconv.Atoi(s[i])
		i++
		value_threadpool_input_count, _ = strconv.Atoi(s[i])
		i++
		value_threadpool_input_running, _ = strconv.Atoi(s[i])
		i++
		value_threadpool_input_queued, _ = strconv.Atoi(s[i])
		i++
		value_threadpool_input_total, _ = strconv.Atoi(s[i])
		i++
		value_threadpool_database_count, _ = strconv.Atoi(s[i])
		i++
		value_threadpool_database_running, _ = strconv.Atoi(s[i])
		i++
		value_threadpool_database_queued, _ = strconv.Atoi(s[i])
		i++
		value_threadpool_database_total, _ = strconv.Atoi(s[i])
	} else {
		ch <- prometheus.MustNewConstMetric(metric_up, prometheus.GaugeValue, 0)
	}

	ch <- prometheus.MustNewConstMetric(metric_query_queue_size, prometheus.GaugeValue, float64(value_query_queue_size))
	ch <- prometheus.MustNewConstMetric(metric_database_connections_max, prometheus.GaugeValue, float64(value_database_connections_max))
	ch <- prometheus.MustNewConstMetric(metric_database_connections_active, prometheus.GaugeValue, float64(value_database_connections_active))
	ch <- prometheus.MustNewConstMetric(metric_threadpool_input_count, prometheus.GaugeValue, float64(value_threadpool_input_count))
	ch <- prometheus.MustNewConstMetric(metric_threadpool_input_running, prometheus.GaugeValue, float64(value_threadpool_input_running))
	ch <- prometheus.MustNewConstMetric(metric_threadpool_input_queued, prometheus.GaugeValue, float64(value_threadpool_input_queued))
	ch <- prometheus.MustNewConstMetric(metric_threadpool_input_total, prometheus.GaugeValue, float64(value_threadpool_input_total))
	ch <- prometheus.MustNewConstMetric(metric_threadpool_database_count, prometheus.GaugeValue, float64(value_threadpool_database_count))
	ch <- prometheus.MustNewConstMetric(metric_threadpool_database_running, prometheus.GaugeValue, float64(value_threadpool_database_running))
	ch <- prometheus.MustNewConstMetric(metric_threadpool_database_queued, prometheus.GaugeValue, float64(value_threadpool_database_queued))
	ch <- prometheus.MustNewConstMetric(metric_threadpool_database_total, prometheus.GaugeValue, float64(value_threadpool_database_total))
}

func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	ch <- metric_query_queue_size
	ch <- metric_database_connections_max
	ch <- metric_database_connections_active
	ch <- metric_threadpool_input_count
	ch <- metric_threadpool_input_running
	ch <- metric_threadpool_input_queued
	ch <- metric_threadpool_input_total
	ch <- metric_threadpool_database_count
	ch <- metric_threadpool_database_running
	ch <- metric_threadpool_database_queued
	ch <- metric_threadpool_database_total
}

func main() {
	flag.Parse()

	exporter := NewExporter()
	prometheus.MustRegister(exporter)

	http.Handle(*metricsPath, promhttp.Handler())
	log.Fatal(http.ListenAndServe(*listenAddress, nil))
}
