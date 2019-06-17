require "./spec_helper"

describe AMQ::Protocol::Properties do
  it "can be encoded and decoded" do
    io = IO::Memory.new
    h = Hash(String, AMQ::Protocol::Field){"s" => "båäö€", "i32" => 123, "u" => 0_u8}
    t = Time.unix(Time.utc_now.to_unix)
    props = AMQ::Protocol::Properties.new(AMQ::Protocol::ShortString.new("application/json"), AMQ::Protocol::ShortString.new("gzip"), h, 1_u8, 9_u8, AMQ::Protocol::ShortString.new("correlation_id"), AMQ::Protocol::ShortString.new("reply_to"), AMQ::Protocol::ShortString.new("1000"), AMQ::Protocol::ShortString.new("message_id"), t, AMQ::Protocol::ShortString.new("type"), AMQ::Protocol::ShortString.new("user_id"), AMQ::Protocol::ShortString.new("app_id"), AMQ::Protocol::ShortString.new("reserved1"))
    io.write_bytes props, IO::ByteFormat::NetworkEndian
    io.pos.should eq props.bytesize
    io.pos = 0
    props2 = AMQ::Protocol::Properties.from_io(io, IO::ByteFormat::NetworkEndian)
    props2.should eq props
  end
end
