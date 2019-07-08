require "./spec_helper"

describe AMQ::Protocol::Table do
  it "can be encoded and decoded" do
    data = Hash(String, AMQ::Protocol::Field){
      "bool"    => true,
      "int8"    => Int8::MAX,
      "uint8"   => UInt8::MAX,
      "int16"   => Int16::MAX,
      "uint16"  => UInt16::MAX,
      "int32"   => Int32::MAX,
      "uint32"  => UInt32::MAX,
      "int64"   => Int64::MAX,
      "float32" => 0.0_f32,
      # "float64" => 0.0_f64,
      "string" => "a" * 257,
      "array"  => [
        true,
        Int8::MAX,
        UInt8::MAX,
        Int16::MAX,
        UInt16::MAX,
        Int32::MAX,
        UInt32::MAX,
        Int64::MAX,
        0.0_f32,
        # 0.0_f64,
        "a" * 257,
        "aaaa".to_slice,
        Time.unix(Time.utc_now.to_unix),
        Hash(String, AMQ::Protocol::Field){"key" => "value"},
        nil,
      ] of AMQ::Protocol::Field,
      "byte_array" => "aaaa".to_slice,
      "time"       => Time.unix(Time.utc_now.to_unix),
      "hash"       => Hash(String, AMQ::Protocol::Field){"key" => "value"},
      "nil"        => nil,
    }
    tbl = AMQ::Protocol::Table.new(data)
    io = IO::Memory.new
    io.write_bytes tbl, IO::ByteFormat::NetworkEndian
    io.pos.should eq(tbl.bytesize)
    io.pos = 0
    data2 = AMQ::Protocol::Table.from_io(io, IO::ByteFormat::NetworkEndian)
    data2.should eq tbl
  end

  it "can be modified" do
    tbl = AMQ::Protocol::Table.new(Hash(String, AMQ::Protocol::Field){
      "key" => "value"
    })
    tbl.bytesize.should eq(sizeof(UInt32) + 1 + "key".bytesize + 1 + sizeof(UInt32) + "value".bytesize)
    tbl["key"] = 1
    tbl.bytesize.should eq(sizeof(UInt32) + 1 + "key".bytesize + 1 + sizeof(Int32))
    io = IO::Memory.new
    io.write_bytes tbl, IO::ByteFormat::NetworkEndian
    io.rewind
    tbl2 = AMQ::Protocol::Table.from_io(io, IO::ByteFormat::NetworkEndian)
    tbl2.should eq tbl
    tbl2.bytesize.should eq(sizeof(UInt32) + 1 + "key".bytesize + 1 + sizeof(Int32))
  end
end


