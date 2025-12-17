require "./spec_helper"

describe AMQ::Protocol::Frame::Method::Basic::Get do
  it "can be encoded and decoded" do
    io = IO::Memory.new
    f_out = AMQ::Protocol::Frame::Method::Basic::Get.new(1_u16, 0_u16, "myqueue", true)
    f_out.to_io(io, IO::ByteFormat::NetworkEndian)
    io.rewind
    AMQ::Protocol::Stream.new(io).next_frame do |f_in|
      f_out.should eq f_in
    end
  end

  it "can be encoded to slice" do
    frame = AMQ::Protocol::Frame::Method::Queue::Declare.new(1u16, 0u16, "qname", false, true, false, false, false, AMQ::Protocol::Table.new)
    io = IO::Memory.new
    frame.to_io(io, IO::ByteFormat::SystemEndian)
    frame.to_slice.should eq io.to_slice
  end

  it "can calculate Exchange::Bind bytesize correctly" do
    destination = "my_destination"
    source = "my_source"
    routing_key = "my_routing_key"
    arguments = AMQ::Protocol::Table.new
    frame = AMQ::Protocol::Frame::Exchange::Bind.new(1u16, 0u16, destination, source, routing_key, false, arguments)

    # Serialize to get actual byte length
    io = IO::Memory.new
    frame.to_io(io, IO::ByteFormat::NetworkEndian)
    actual_size = io.size

    # Remove frame wrapper overhead (7 bytes header + 1 byte footer)
    actual_method_size = actual_size - 8

    # The frame's bytesize should match the actual method size
    frame.bytesize.should eq actual_method_size
  end
end

describe AMQ::Protocol::Frame::Heartbeat do
  it "rejects heartbeat frames with non-zero size" do
    io = IO::Memory.new
    format = IO::ByteFormat::NetworkEndian

    # Manually construct a malformed heartbeat with non-zero size
    io.write_byte(8_u8)             # Heartbeat frame type
    io.write_bytes(0_u16, format)   # Channel 0
    io.write_bytes(100_u32, format) # Size 100 (invalid for heartbeat!)
    io.write(("x" * 100).to_slice)  # Some content
    io.write_byte(206_u8)           # Frame end marker
    io.rewind

    expect_raises(AMQ::Protocol::Error::FrameDecode, /Heartbeat frame size must be 0, got 100/) do
      AMQ::Protocol::Stream.new(io).next_frame
    end
  end

  it "rejects heartbeat frames with non-zero channel" do
    io = IO::Memory.new
    format = IO::ByteFormat::NetworkEndian

    # Manually construct a malformed heartbeat with non-zero channel
    io.write_byte(8_u8)           # Heartbeat frame type
    io.write_bytes(5_u16, format) # Channel 5 (invalid for heartbeat!)
    io.write_bytes(0_u32, format) # Size 0
    io.write_byte(206_u8)         # Frame end marker
    io.rewind

    expect_raises(AMQ::Protocol::Error::FrameDecode, /Heartbeat frame channel must be 0, got 5/) do
      AMQ::Protocol::Stream.new(io).next_frame
    end
  end
end
