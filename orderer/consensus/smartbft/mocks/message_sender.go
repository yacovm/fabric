// Code generated by mockery v1.0.0. DO NOT EDIT.

package mocks

import mock "github.com/stretchr/testify/mock"

// MessageSender is an autogenerated mock type for the MessageSender type
type MessageSender struct {
	mock.Mock
}

// SendHeartbeat provides a mock function with given fields: dest
func (_m *MessageSender) SendHeartbeat(dest uint64) {
	_m.Called(dest)
}
