package task

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseFps(t *testing.T) {
	assert := assert.New(t)

	// single value, invalid
	_, err := parseFps("foobar")
	assert.ErrorContains(err, "error parsing framerate:")

	// single value
	fps, err := parseFps("5")
	assert.Nil(err)
	assert.Equal(fps, 5.0)

	// fraction, invalid
	_, err = parseFps("foo/bar")
	assert.ErrorContains(err, "error parsing framerate numerator:")

	// invalid numerator
	_, err = parseFps("foo/1")
	assert.ErrorContains(err, "error parsing framerate numerator:")

	// invalid denominator
	_, err = parseFps("1/foo")
	assert.ErrorContains(err, "error parsing framerate denominator:")

	// 1/0
	_, err = parseFps("1/0")
	assert.ErrorContains(err, "invalid framerate denominator 0")

	// 0/0
	fps, err = parseFps("0/0")
	assert.Nil(err)
	assert.Equal(fps, 0.0)

	// 5/1
	fps, err = parseFps("5/1")
	assert.Nil(err)
	assert.Equal(fps, 5.0)

	// 1/1
	fps, err = parseFps("1/1")
	assert.Nil(err)
	assert.Equal(fps, 1.0)

	// 1/4
	fps, err = parseFps("1/4")
	assert.Nil(err)
	assert.Equal(fps, 0.25)
}
