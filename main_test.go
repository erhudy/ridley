package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestMain(m *testing.M) {
	logger = zap.NewNop().Sugar()
	os.Exit(m.Run())
}

func TestParseTargetHeaders_Empty(t *testing.T) {
	result, err := parseTargetHeaders("")
	assert.NoError(t, err)
	assert.Equal(t, map[string]string{}, result)
}

func TestParseTargetHeaders_SingleEntry(t *testing.T) {
	result, err := parseTargetHeaders("k=v")
	assert.NoError(t, err)
	assert.Equal(t, map[string]string{"k": "v"}, result)
}

func TestParseTargetHeaders_MultipleEntries(t *testing.T) {
	result, err := parseTargetHeaders("k1=v1,k2=v2")
	assert.NoError(t, err)
	assert.Equal(t, map[string]string{"k1": "v1", "k2": "v2"}, result)
}

func TestParseTargetHeaders_WhitespaceTrimmed(t *testing.T) {
	result, err := parseTargetHeaders("  k1 = v1 , k2 = v2 ")
	assert.NoError(t, err)
	assert.Equal(t, map[string]string{"k1": "v1", "k2": "v2"}, result)
}

func TestParseTargetHeaders_QuotedValues(t *testing.T) {
	result, err := parseTargetHeaders("k1='v1',k2=\"v2\"")
	assert.NoError(t, err)
	assert.Equal(t, map[string]string{"k1": "v1", "k2": "v2"}, result)
}

func TestParseTargetHeaders_MalformedEntry_ReturnsError(t *testing.T) {
	_, err := parseTargetHeaders("noequals")
	assert.Error(t, err)
}

func TestParseTargetHeaders_TrailingCommaIgnored(t *testing.T) {
	result, err := parseTargetHeaders("k=v,")
	assert.NoError(t, err)
	assert.Equal(t, map[string]string{"k": "v"}, result)
}
