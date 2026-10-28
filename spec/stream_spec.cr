require "./spec_helper"

private class ByteCountingIO < IO
  getter read_byte_calls = 0

  def initialize(@io : IO::Memory)
  end

  def read(slice : Bytes)
    @io.read(slice)
  end

  def read_byte : UInt8?
    @read_byte_calls += 1
    @io.read_byte
  end

  def write(slice : Bytes) : Nil
    @io.write(slice)
  end
end

describe AMQ::Protocol::Stream do
  it "read_byte reads through the IO's read_byte and counts against the frame size" do
    io = IO::Memory.new
    format = IO::ByteFormat::NetworkEndian
    io.write_byte(3_u8)
    io.write_bytes(1_u16, format)
    io.write_bytes(2_u32, format)
    io.write("ab".to_slice)
    io.write_byte(206_u8)
    io.rewind
    counting = ByteCountingIO.new(io)
    stream = AMQ::Protocol::Stream.new(counting)

    stream.next_frame do |frame|
      frame.should be_a(AMQ::Protocol::Frame::Body)
      stream.read_byte.should eq 'a'.ord
      stream.read_byte.should eq 'b'.ord
      counting.read_byte_calls.should eq 2
      expect_raises(AMQ::Protocol::Error::FrameSizeError) { stream.read_byte }
    end
  end

  it "read enforces frame size limit during frame parsing" do
    # Create a frame with exactly the right amount of data
    io = IO::Memory.new
    format = IO::ByteFormat::NetworkEndian

    # Write Body frame header - 100 byte body
    io.write_byte(3_u8)             # Body frame type
    io.write_bytes(1_u16, format)   # Channel 1
    io.write_bytes(100_u32, format) # Size 100 bytes
    io.write(("x" * 100).to_slice)  # Body content (exactly 100 bytes)
    io.write_byte(206_u8)           # Frame end marker

    io.rewind
    stream = AMQ::Protocol::Stream.new(io)

    # Parse the frame successfully
    frame = stream.next_frame
    frame.should be_a(AMQ::Protocol::Frame::BytesBody)
    if frame.is_a?(AMQ::Protocol::Frame::BytesBody)
      frame.body.size.should eq 100
    end
  end

  it "next_frame rejects frames that exceed max frame size" do
    # Create a frame with size that would exceed frame_max
    io = IO::Memory.new
    format = IO::ByteFormat::NetworkEndian

    io.write_byte(1_u8)                 # Method frame type
    io.write_bytes(0_u16, format)       # Channel 0
    io.write_bytes(200_000_u32, format) # Size 200KB (exceeds default 128KB)

    io.rewind
    stream = AMQ::Protocol::Stream.new(io)

    expect_raises(AMQ::Protocol::Error::FrameSizeError, /exceeds max frame size/) do
      stream.next_frame
    end
  end

  it "next_frame rejects frames with malicious size mismatches" do
    # Create a StartOk frame with a very long response string (200 KB)
    long_response = "x" * (200 * 1024)
    client_props = AMQ::Protocol::Table.new
    format = IO::ByteFormat::NetworkEndian

    io = IO::Memory.new

    # Write frame header with fake size
    io.write_byte(1_u8)              # Method frame type
    io.write_bytes(0_u16, format)    # Channel 0
    io.write_bytes(1000_u32, format) # Fake size (much smaller than actual)

    # Write the method header
    io.write_bytes(10_u16, format) # Connection class
    io.write_bytes(11_u16, format) # StartOk method

    # Write the actual (large) content
    io.write_bytes(client_props, format)
    io.write_bytes(AMQ::Protocol::ShortString.new("PLAIN"), format)
    io.write_bytes(AMQ::Protocol::LongString.new(long_response), format)
    io.write_bytes(AMQ::Protocol::ShortString.new("en_US"), format)
    io.write_byte(206_u8) # Frame end marker

    # Try to parse the frame - should raise an error
    io.rewind
    stream = AMQ::Protocol::Stream.new(io)

    expect_raises(AMQ::Protocol::Error::FrameSizeError, /Cannot allocate|frame size limit/) do
      stream.next_frame
    end
  end

  it "next_frame with block rejects malicious size mismatches" do
    # Create a StartOk frame with a very long response string (200 KB)
    long_response = "x" * (200 * 1024)
    client_props = AMQ::Protocol::Table.new
    format = IO::ByteFormat::NetworkEndian

    io = IO::Memory.new

    # Write frame header with fake size
    io.write_byte(1_u8)              # Method frame type
    io.write_bytes(0_u16, format)    # Channel 0
    io.write_bytes(1000_u32, format) # Fake size (much smaller than actual)

    # Write the method header
    io.write_bytes(10_u16, format) # Connection class
    io.write_bytes(11_u16, format) # StartOk method

    # Write the actual (large) content
    io.write_bytes(client_props, format)
    io.write_bytes(AMQ::Protocol::ShortString.new("PLAIN"), format)
    io.write_bytes(AMQ::Protocol::LongString.new(long_response), format)
    io.write_bytes(AMQ::Protocol::ShortString.new("en_US"), format)
    io.write_byte(206_u8) # Frame end marker

    # Try to parse the frame - should raise an error
    io.rewind
    stream = AMQ::Protocol::Stream.new(io)

    expect_raises(AMQ::Protocol::Error::FrameSizeError, /Cannot allocate|frame size limit/) do
      stream.next_frame { }
    end
  end

  it "next_frame successfully parses valid frames" do
    # Create a valid small frame
    io = IO::Memory.new
    format = IO::ByteFormat::NetworkEndian

    # Create a heartbeat frame (simplest case)
    io.write_byte(8_u8)           # Heartbeat frame type
    io.write_bytes(0_u16, format) # Channel 0
    io.write_bytes(0_u32, format) # Size 0
    io.write_byte(206_u8)         # Frame end marker

    io.rewind
    stream = AMQ::Protocol::Stream.new(io)

    frame = stream.next_frame
    frame.should be_a(AMQ::Protocol::Frame::Heartbeat)
  end

  it "next_frame with block successfully parses valid frames" do
    # Create a valid small frame
    io = IO::Memory.new
    format = IO::ByteFormat::NetworkEndian

    # Create a heartbeat frame (simplest case)
    io.write_byte(8_u8)           # Heartbeat frame type
    io.write_bytes(0_u16, format) # Channel 0
    io.write_bytes(0_u32, format) # Size 0
    io.write_byte(206_u8)         # Frame end marker

    io.rewind
    stream = AMQ::Protocol::Stream.new(io)

    result = nil
    stream.next_frame do |frame|
      result = frame
    end

    result.should be_a(AMQ::Protocol::Frame::Heartbeat)
  end
end
