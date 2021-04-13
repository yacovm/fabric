// Code generated by protoc-gen-go. DO NOT EDIT.
// source: orderer/smartbft/committee.proto

package smartbft

import (
	fmt "fmt"
	math "math"

	proto "github.com/golang/protobuf/proto"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

// CommitteeFeedback holds committee related info
type CommitteeFeedback struct {
	Suspects             []int32  `protobuf:"varint,1,rep,packed,name=suspects,proto3" json:"suspects,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *CommitteeFeedback) Reset()         { *m = CommitteeFeedback{} }
func (m *CommitteeFeedback) String() string { return proto.CompactTextString(m) }
func (*CommitteeFeedback) ProtoMessage()    {}
func (*CommitteeFeedback) Descriptor() ([]byte, []int) {
	return fileDescriptor_5339e81cc9997a76, []int{0}
}

func (m *CommitteeFeedback) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_CommitteeFeedback.Unmarshal(m, b)
}
func (m *CommitteeFeedback) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_CommitteeFeedback.Marshal(b, m, deterministic)
}
func (m *CommitteeFeedback) XXX_Merge(src proto.Message) {
	xxx_messageInfo_CommitteeFeedback.Merge(m, src)
}
func (m *CommitteeFeedback) XXX_Size() int {
	return xxx_messageInfo_CommitteeFeedback.Size(m)
}
func (m *CommitteeFeedback) XXX_DiscardUnknown() {
	xxx_messageInfo_CommitteeFeedback.DiscardUnknown(m)
}

var xxx_messageInfo_CommitteeFeedback proto.InternalMessageInfo

func (m *CommitteeFeedback) GetSuspects() []int32 {
	if m != nil {
		return m.Suspects
	}
	return nil
}

func init() {
	proto.RegisterType((*CommitteeFeedback)(nil), "smartbft.CommitteeFeedback")
}

func init() { proto.RegisterFile("orderer/smartbft/committee.proto", fileDescriptor_5339e81cc9997a76) }

var fileDescriptor_5339e81cc9997a76 = []byte{
	// 161 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x5c, 0x8e, 0x31, 0xca, 0x02, 0x31,
	0x10, 0x46, 0xf9, 0xf9, 0x51, 0x96, 0x74, 0x6e, 0x25, 0x56, 0x8b, 0x95, 0xd5, 0x4c, 0x21, 0x5e,
	0x40, 0xc1, 0x03, 0x58, 0xda, 0x6d, 0xb2, 0xb3, 0xd9, 0xa0, 0x61, 0xc2, 0x64, 0xb6, 0xf0, 0xf6,
	0x82, 0x31, 0x22, 0x96, 0xc3, 0xe3, 0xcd, 0xfb, 0x4c, 0xc7, 0x32, 0x90, 0x90, 0x60, 0x8e, 0xbd,
	0xa8, 0x1d, 0x15, 0x1d, 0xc7, 0x18, 0x54, 0x89, 0x20, 0x09, 0x2b, 0xb7, 0x4d, 0x25, 0x5b, 0x34,
	0xab, 0x53, 0x85, 0x67, 0xa2, 0xc1, 0xf6, 0xee, 0xd6, 0x6e, 0x4c, 0x93, 0xe7, 0x9c, 0xc8, 0x69,
	0x5e, 0xff, 0x75, 0xff, 0xbb, 0xc5, 0xe5, 0x73, 0x1f, 0xbd, 0x01, 0x16, 0x0f, 0xd3, 0x23, 0x91,
	0xdc, 0x69, 0xf0, 0x24, 0x30, 0xf6, 0x56, 0x82, 0x2b, 0xaf, 0x33, 0xbc, 0xe3, 0x50, 0x13, 0xd7,
	0x83, 0x0f, 0x3a, 0xcd, 0x16, 0x1c, 0x47, 0xfc, 0xd2, 0xb0, 0x68, 0x58, 0x34, 0xfc, 0xdd, 0x6c,
	0x97, 0x2f, 0xb0, 0x7f, 0x06, 0x00, 0x00, 0xff, 0xff, 0xf8, 0xb8, 0x6f, 0x72, 0xce, 0x00, 0x00,
	0x00,
}
