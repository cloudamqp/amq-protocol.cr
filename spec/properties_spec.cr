require "./spec_helper"

describe AMQ::Protocol::Properties do
  it "can be encoded and decoded" do
    io = IO::Memory.new
    h = AMQ::Protocol::Table.new(Hash(String, AMQ::Protocol::Field){"s" => "båäö€", "i32" => 123, "u" => 0_u8})
    t = Time.unix(Time.utc.to_unix)
    props = AMQ::Protocol::Properties.new("application/json", "gzip", h, 1_u8, 9_u8, "correlation_id", "reply_to", "1000", "message_id", t, "type", "user_id", "app_id", "reserved1")
    io.write_bytes props, IO::ByteFormat::NetworkEndian
    io.pos.should eq props.bytesize
    io.pos = 0
    props2 = AMQ::Protocol::Properties.from_io(io, IO::ByteFormat::NetworkEndian)
    props2.should eq props
  end

  it "can skip past it" do
    io = IO::Memory.new
    h = AMQ::Protocol::Table.new(Hash(String, AMQ::Protocol::Field){"s" => "båäö€", "i32" => 123, "u" => 0_u8})
    t = Time.unix(Time.utc.to_unix)
    props = AMQ::Protocol::Properties.new("application/json", "gzip", h, 1_u8, 9_u8, "correlation_id", "reply_to", "1000", "message_id", t, "type", "user_id", "app_id", "reserved1")
    io.write_bytes props, IO::ByteFormat::NetworkEndian
    end_pos = io.pos
    io.rewind
    AMQ::Protocol::Properties.skip(io, IO::ByteFormat::NetworkEndian)
    io.pos.should eq end_pos
  end

  it "can be encoded and decoded namedtuple" do
    io = IO::Memory.new
    h = AMQ::Protocol::Table.new({s: "båäö€", i32: 123, u: 0_u8})
    t = Time.unix(Time.utc.to_unix)
    props = AMQ::Protocol::Properties.new("application/json", "gzip", h, 1_u8, 9_u8, "correlation_id", "reply_to", "1000", "message_id", t, "type", "user_id", "app_id", "reserved1")
    io.write_bytes props, IO::ByteFormat::NetworkEndian
    io.pos.should eq props.bytesize
    io.pos = 0
    props2 = AMQ::Protocol::Properties.from_io(io, IO::ByteFormat::NetworkEndian)
    props2.should eq props
  end

  it "can encode and decode invalid timestamps" do
    io = IO::Memory.new
    props = AMQ::Protocol::Properties.new(timestamp: Int64::MAX)
    io.write_bytes props, IO::ByteFormat::NetworkEndian
    io.pos.should eq props.bytesize
    io.pos = 0
    props2 = AMQ::Protocol::Properties.from_io(io, IO::ByteFormat::NetworkEndian)
    props2.should eq props
  end

  it "will raise when parsing invalid timestamp" do
    props = AMQ::Protocol::Properties.new(timestamp: Int64::MAX)

    expect_raises(OverflowError) do
      props.timestamp
    end
  end

  it "can set timestamp" do
    props = AMQ::Protocol::Properties.new
    t = Time.utc
    props.timestamp = t
    props.timestamp.should eq Time.unix(t.to_unix)
  end

  it "can unset timestamp" do
    props = AMQ::Protocol::Properties.new
    props.timestamp = nil
    props.timestamp.should be_nil
  end
end
