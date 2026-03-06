// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package libp2p

import (
	m "github.com/ethersphere/bee/v2/pkg/metrics"
	"github.com/ethersphere/bee/v2/pkg/p2p"
	libp2pmetrics "github.com/libp2p/go-libp2p/core/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

type metrics struct {
	// all metrics fields must be exported
	// to be able to return them by Metrics()
	// using reflection
	CreatedConnectionCount     prometheus.Counter
	HandledConnectionCount     prometheus.Counter
	CreatedStreamCount         prometheus.Counter
	ClosedStreamCount          prometheus.Counter
	StreamResetCount           prometheus.Counter
	HandledStreamCount         prometheus.Counter
	BlocklistedPeerCount       prometheus.Counter
	BlocklistedPeerErrCount    prometheus.Counter
	DisconnectCount            prometheus.Counter
	ConnectBreakerCount        prometheus.Counter
	UnexpectedProtocolReqCount prometheus.Counter
	KickedOutPeersCount        prometheus.Counter
	StreamHandlerErrResetCount prometheus.Counter
	HeadersExchangeDuration    prometheus.Histogram
}

func newMetrics() metrics {
	subsystem := "libp2p"

	return metrics{
		CreatedConnectionCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "created_connection_count",
			Help:      "Number of initiated outgoing libp2p connections.",
		}),
		HandledConnectionCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "handled_connection_count",
			Help:      "Number of handled incoming libp2p connections.",
		}),
		CreatedStreamCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "created_stream_count",
			Help:      "Number of initiated outgoing libp2p streams.",
		}),
		ClosedStreamCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "closed_stream_count",
			Help:      "Number of closed outgoing libp2p streams.",
		}),
		StreamResetCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "stream_reset_count",
			Help:      "Number of outgoing libp2p streams resets.",
		}),
		HandledStreamCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "handled_stream_count",
			Help:      "Number of handled incoming libp2p streams.",
		}),
		BlocklistedPeerCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "blocklisted_peer_count",
			Help:      "Number of peers we've blocklisted.",
		}),
		BlocklistedPeerErrCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "blocklisted_peer_err_count",
			Help:      "Number of peers we've been unable to blocklist.",
		}),
		DisconnectCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "disconnect_count",
			Help:      "Number of peers we've disconnected from (initiated locally).",
		}),
		ConnectBreakerCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "connect_breaker_count",
			Help:      "Number of times we got a closed breaker while connecting to another peer.",
		}),
		UnexpectedProtocolReqCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "unexpected_protocol_request_count",
			Help:      "Number of requests the peer is not expecting.",
		}),
		KickedOutPeersCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "kickedout_peers_count",
			Help:      "Number of total kicked-out peers.",
		}),
		StreamHandlerErrResetCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "stream_handler_error_reset_count",
			Help:      "Number of total stream handler error resets.",
		}),
		HeadersExchangeDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "headers_exchange_duration",
			Help:      "The duration spent exchanging the headers.",
		}),
	}
}

func (s *Service) Metrics() []prometheus.Collector {
	return append(
		m.PrometheusCollectorsFromFields(s.metrics),
		append(s.handshakeService.Metrics(), newBandwidthCollector(s.bandwidthCounter))...,
	)
}

// StatusMetrics exposes metrics that are exposed on the status protocol.
func (s *Service) StatusMetrics() []prometheus.Collector {
	return []prometheus.Collector{
		s.metrics.HeadersExchangeDuration,
	}
}

// bandwidthCollector is a prometheus.Collector that exposes libp2p bandwidth
// statistics (total bytes in/out and current rates in/out) sampled on each scrape.
type bandwidthCollector struct {
	bwc      *libp2pmetrics.BandwidthCounter
	totalIn  *prometheus.Desc
	totalOut *prometheus.Desc
	rateIn   *prometheus.Desc
	rateOut  *prometheus.Desc
}

func newBandwidthCollector(bwc *libp2pmetrics.BandwidthCounter) *bandwidthCollector {
	labels := []string{}
	return &bandwidthCollector{
		bwc: bwc,
		totalIn: prometheus.NewDesc(
			prometheus.BuildFQName(m.Namespace, "libp2p", "bandwidth_total_in_bytes"),
			"Total bytes received over all libp2p connections.",
			labels, nil,
		),
		totalOut: prometheus.NewDesc(
			prometheus.BuildFQName(m.Namespace, "libp2p", "bandwidth_total_out_bytes"),
			"Total bytes sent over all libp2p connections.",
			labels, nil,
		),
		rateIn: prometheus.NewDesc(
			prometheus.BuildFQName(m.Namespace, "libp2p", "bandwidth_rate_in_bytes_per_second"),
			"Current inbound bandwidth rate in bytes per second.",
			labels, nil,
		),
		rateOut: prometheus.NewDesc(
			prometheus.BuildFQName(m.Namespace, "libp2p", "bandwidth_rate_out_bytes_per_second"),
			"Current outbound bandwidth rate in bytes per second.",
			labels, nil,
		),
	}
}

func (b *bandwidthCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- b.totalIn
	ch <- b.totalOut
	ch <- b.rateIn
	ch <- b.rateOut
}

func (b *bandwidthCollector) Collect(ch chan<- prometheus.Metric) {
	stats := b.bwc.GetBandwidthTotals()
	ch <- prometheus.MustNewConstMetric(b.totalIn, prometheus.CounterValue, float64(stats.TotalIn))
	ch <- prometheus.MustNewConstMetric(b.totalOut, prometheus.CounterValue, float64(stats.TotalOut))
	ch <- prometheus.MustNewConstMetric(b.rateIn, prometheus.GaugeValue, stats.RateIn)
	ch <- prometheus.MustNewConstMetric(b.rateOut, prometheus.GaugeValue, stats.RateOut)
}

// GetBandwidthStats returns the current bandwidth statistics for all libp2p connections.
// TotalIn/TotalOut are cumulative byte counts; RateIn/RateOut are bytes per second.
func (s *Service) GetBandwidthStats() p2p.BandwidthStats {
	stats := s.bandwidthCounter.GetBandwidthTotals()
	return p2p.BandwidthStats{
		TotalIn:  stats.TotalIn,
		TotalOut: stats.TotalOut,
		RateIn:   stats.RateIn,
		RateOut:  stats.RateOut,
	}
}

// ClearBandwidthStats resets all bandwidth statistics counters.
func (s *Service) ClearBandwidthStats() {
	s.bandwidthCounter.Reset()
}
