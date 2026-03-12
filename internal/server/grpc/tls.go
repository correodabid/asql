package grpc

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

func loadServerTLSConfig(config Config) (*tls.Config, error) {
	if config.TLSCertPath == "" && config.TLSKeyPath == "" && config.TLSClientCAPath == "" {
		return nil, nil
	}

	certificate, err := tls.LoadX509KeyPair(config.TLSCertPath, config.TLSKeyPath)
	if err != nil {
		return nil, fmt.Errorf("load server key pair: %w", err)
	}

	caPEM, err := os.ReadFile(config.TLSClientCAPath)
	if err != nil {
		return nil, fmt.Errorf("read client ca cert: %w", err)
	}

	clientCAPool := x509.NewCertPool()
	if !clientCAPool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("parse client ca cert: invalid PEM")
	}

	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{certificate},
		ClientCAs:    clientCAPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}, nil
}
