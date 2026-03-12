package grpc

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"log/slog"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestConfigValidateRequiresAllMTLSPaths(t *testing.T) {
	config := Config{Address: ":1", DataDirPath: "./test-data", Logger: testLogger(), TLSCertPath: "server.crt"}

	err := config.Validate()
	if err == nil {
		t.Fatal("expected validation error for partial mtls config")
	}
}

func TestLoadServerTLSConfigDisabled(t *testing.T) {
	tlsConfig, err := loadServerTLSConfig(Config{})
	if err != nil {
		t.Fatalf("unexpected error for disabled tls config: %v", err)
	}

	if tlsConfig != nil {
		t.Fatal("expected nil tls config when mtls is not configured")
	}
}

func TestLoadServerTLSConfigEnabled(t *testing.T) {
	tempDir := t.TempDir()

	serverCertPath, serverKeyPath, caCertPath := writeMTLSFixtureFiles(t, tempDir)

	tlsConfig, err := loadServerTLSConfig(Config{
		TLSCertPath:     serverCertPath,
		TLSKeyPath:      serverKeyPath,
		TLSClientCAPath: caCertPath,
	})
	if err != nil {
		t.Fatalf("load tls config: %v", err)
	}

	if tlsConfig == nil {
		t.Fatal("expected tls config")
	}

	if tlsConfig.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Fatalf("unexpected client auth mode: got %v want %v", tlsConfig.ClientAuth, tls.RequireAndVerifyClientCert)
	}

	if len(tlsConfig.Certificates) != 1 {
		t.Fatalf("unexpected server certificate count: got %d want 1", len(tlsConfig.Certificates))
	}

	if tlsConfig.ClientCAs == nil {
		t.Fatal("expected client ca pool")
	}
}

func TestLoadServerTLSConfigInvalidClientCA(t *testing.T) {
	tempDir := t.TempDir()

	serverCertPath, serverKeyPath, _ := writeMTLSFixtureFiles(t, tempDir)
	invalidCAPath := filepath.Join(tempDir, "invalid-ca.pem")
	if err := os.WriteFile(invalidCAPath, []byte("not-a-pem"), 0o644); err != nil {
		t.Fatalf("write invalid ca file: %v", err)
	}

	_, err := loadServerTLSConfig(Config{
		TLSCertPath:     serverCertPath,
		TLSKeyPath:      serverKeyPath,
		TLSClientCAPath: invalidCAPath,
	})
	if err == nil {
		t.Fatal("expected invalid client ca parsing error")
	}
}

func writeMTLSFixtureFiles(t *testing.T, dir string) (serverCertPath, serverKeyPath, caCertPath string) {
	t.Helper()

	caCertPEM, caPrivateKeyPEM, caCert, caKey := generateCA(t)
	serverCertPEM, serverKeyPEM := generateServerCert(t, caCert, caKey)

	caCertPath = filepath.Join(dir, "ca.pem")
	serverCertPath = filepath.Join(dir, "server.pem")
	serverKeyPath = filepath.Join(dir, "server-key.pem")

	if err := os.WriteFile(caCertPath, caCertPEM, 0o644); err != nil {
		t.Fatalf("write ca cert: %v", err)
	}
	if err := os.WriteFile(serverCertPath, serverCertPEM, 0o644); err != nil {
		t.Fatalf("write server cert: %v", err)
	}
	if err := os.WriteFile(serverKeyPath, serverKeyPEM, 0o600); err != nil {
		t.Fatalf("write server key: %v", err)
	}

	_ = caPrivateKeyPEM
	return serverCertPath, serverKeyPath, caCertPath
}

func generateCA(t *testing.T) ([]byte, []byte, *x509.Certificate, *rsa.PrivateKey) {
	t.Helper()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate ca key: %v", err)
	}

	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		t.Fatalf("generate ca serial: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "ASQL Test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	if err != nil {
		t.Fatalf("create ca cert: %v", err)
	}

	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse ca cert: %v", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})

	return certPEM, keyPEM, cert, privateKey
}

func generateServerCert(t *testing.T, caCert *x509.Certificate, caKey *rsa.PrivateKey) ([]byte, []byte) {
	t.Helper()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate server key: %v", err)
	}

	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		t.Fatalf("generate server serial: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "localhost"},
		DNSNames:     []string{"localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}

	der, err := x509.CreateCertificate(rand.Reader, template, caCert, &privateKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create server cert: %v", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})

	return certPEM, keyPEM
}

func testLogger() *slog.Logger {
	return slog.Default()
}
