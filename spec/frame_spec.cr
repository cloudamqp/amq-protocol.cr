require "./spec_helper"

describe AMQ::Protocol::Frame::Method::Basic::Get do
  it "can be encoded and decoded" do
    io = IO::Memory.new
    f_out = AMQ::Protocol::Frame::Method::Basic::Get.new(1_u16, 0_u16, "myqueue", true)
    f_out.to_io(io, IO::ByteFormat::NetworkEndian)
    io.rewind
    AMQ::Protocol::Frame.from_io(io, IO::ByteFormat::NetworkEndian) do |f_in|
      f_out.should eq f_in
    end
  end

  it "can be encoded to slice" do
    frame = AMQ::Protocol::Frame::Method::Queue::Declare.new(1u16, 0u16, "qname", false, true, false, false, false, AMQ::Protocol::Table.new)
    io = IO::Memory.new
    frame.to_io(io, IO::ByteFormat::SystemEndian)
    frame.to_slice.should eq io.to_slice
  end

  it "can encode Exchange::Bind frame with correct bytesize" do
    destination = "my_destination"
    source = "my_source"
    routing_key = "my_routing_key"
    arguments = AMQ::Protocol::Table.new
    frame = AMQ::Protocol::Frame::Exchange::Bind.new(1u16, 0u16, destination, source, routing_key, false, arguments)
    # Method body size: reserved1 + destination + source + routing_key + no_wait + arguments
    method_body_size = 2 + 1 + destination.bytesize + 1 + source.bytesize + 1 + routing_key.bytesize + 1 + arguments.bytesize
    # Total frame size includes 4 bytes for class_id + method_id
    expected_bytesize = method_body_size + 4
    frame.bytesize.should eq expected_bytesize
  end
end
