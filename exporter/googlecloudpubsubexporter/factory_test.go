// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package googlecloudpubsubexporter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exportertest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/googlecloudpubsubexporter/internal/metadata"
)

func TestCreateDefaultConfig(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	assert.NotNil(t, cfg, "failed to create default config")
	assert.NoError(t, componenttest.CheckConfigStruct(cfg))
}

func TestType(t *testing.T) {
	factory := NewFactory()
	assert.Equal(t, metadata.Type, factory.Type())
}

func TestCreateTraces(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	eCfg := cfg.(*Config)
	eCfg.Endpoint = "http://testing.invalid"

	te, err := factory.CreateTraces(
		t.Context(),
		exportertest.NewNopSettings(metadata.Type),
		eCfg,
	)
	assert.NoError(t, err)
	assert.NotNil(t, te, "failed to create trace exporter")
}

func TestCreateMetrics(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	eCfg := cfg.(*Config)
	eCfg.Endpoint = "http://testing.invalid"

	me, err := factory.CreateMetrics(
		t.Context(),
		exportertest.NewNopSettings(metadata.Type),
		eCfg,
	)
	assert.NoError(t, err)
	assert.NotNil(t, me, "failed to create metrics exporter")
}

func TestLogsCreateExporter(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	eCfg := cfg.(*Config)
	eCfg.Endpoint = "http://testing.invalid"

	me, err := factory.CreateLogs(
		t.Context(),
		exportertest.NewNopSettings(metadata.Type),
		eCfg,
	)
	assert.NoError(t, err)
	assert.NotNil(t, me, "failed to create logs exporter")
}

func TestEnsureExporter(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	eCfg := cfg.(*Config)
	eCfg.Endpoint = "http://testing.invalid"

	exporter1 := ensureExporter(exportertest.NewNopSettings(metadata.Type), eCfg)
	exporter2 := ensureExporter(exportertest.NewNopSettings(metadata.Type), eCfg)
	assert.Equal(t, exporter1, exporter2)
}

func TestEnsureExporterWithProtoEncoding(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	eCfg := cfg.(*Config)
	eCfg.Endpoint = "http://testing.invalid"
	eCfg.Encoding = "otlp_proto"

	exporter := ensureExporter(exportertest.NewNopSettings(metadata.Type), eCfg)
	assert.NotNil(t, exporter)
	assert.NotNil(t, exporter.tracesMarshaler)
	assert.NotNil(t, exporter.metricsMarshaler)
	assert.NotNil(t, exporter.logsMarshaler)
}

func TestEnsureExporterWithJSONEncoding(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	eCfg := cfg.(*Config)
	eCfg.Endpoint = "http://testing.invalid"
	eCfg.Encoding = "otlp_json"

	exporter := ensureExporter(exportertest.NewNopSettings(metadata.Type), eCfg)
	assert.NotNil(t, exporter)
	assert.NotNil(t, exporter.tracesMarshaler)
	assert.NotNil(t, exporter.metricsMarshaler)
	assert.NotNil(t, exporter.logsMarshaler)
}

func TestGetEncodingType(t *testing.T) {
	factory := NewFactory()

	t.Run("proto encoding", func(t *testing.T) {
		cfg := factory.CreateDefaultConfig().(*Config)
		cfg.Endpoint = "http://testing.invalid"
		cfg.Encoding = "otlp_proto"

		exporter := ensureExporter(exportertest.NewNopSettings(metadata.Type), cfg)

		enc, err := exporter.getEncodingType("traces")
		require.NoError(t, err)
		assert.Equal(t, encoding(otlpProtoTrace), enc)

		enc, err = exporter.getEncodingType("metrics")
		require.NoError(t, err)
		assert.Equal(t, encoding(otlpProtoMetric), enc)

		enc, err = exporter.getEncodingType("logs")
		require.NoError(t, err)
		assert.Equal(t, encoding(otlpProtoLog), enc)
	})

	t.Run("json encoding", func(t *testing.T) {
		cfg := factory.CreateDefaultConfig().(*Config)
		cfg.Endpoint = "http://testing.invalid"
		cfg.Encoding = "otlp_json"

		exporter := ensureExporter(exportertest.NewNopSettings(metadata.Type), cfg)

		enc, err := exporter.getEncodingType("traces")
		require.NoError(t, err)
		assert.Equal(t, encoding(otlpJSONTrace), enc)

		enc, err = exporter.getEncodingType("metrics")
		require.NoError(t, err)
		assert.Equal(t, encoding(otlpJSONMetric), enc)

		enc, err = exporter.getEncodingType("logs")
		require.NoError(t, err)
		assert.Equal(t, encoding(otlpJSONLog), enc)
	})

	t.Run("default encoding", func(t *testing.T) {
		cfg := factory.CreateDefaultConfig().(*Config)
		cfg.Endpoint = "http://testing.invalid"
		cfg.Encoding = ""

		exporter := ensureExporter(exportertest.NewNopSettings(metadata.Type), cfg)

		// Empty encoding should default to proto
		enc, err := exporter.getEncodingType("traces")
		require.NoError(t, err)
		assert.Equal(t, encoding(otlpProtoTrace), enc)

		enc, err = exporter.getEncodingType("metrics")
		require.NoError(t, err)
		assert.Equal(t, encoding(otlpProtoMetric), enc)

		enc, err = exporter.getEncodingType("logs")
		require.NoError(t, err)
		assert.Equal(t, encoding(otlpProtoLog), enc)
	})

	t.Run("invalid signal type", func(t *testing.T) {
		cfg := factory.CreateDefaultConfig().(*Config)
		cfg.Endpoint = "http://testing.invalid"
		cfg.Encoding = "otlp_proto"

		exporter := ensureExporter(exportertest.NewNopSettings(metadata.Type), cfg)

		_, err := exporter.getEncodingType("invalid")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unknown signal type")
	})
}
