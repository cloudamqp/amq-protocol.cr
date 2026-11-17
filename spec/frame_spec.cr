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

  it "decodes heartbeat frames with non-zero channel" do
    io = IO::Memory.new
    format = IO::ByteFormat::NetworkEndian

    # Preserve the channel so consumers can classify the frame.
    io.write_byte(8_u8)           # Heartbeat frame type
    io.write_bytes(5_u16, format) # Channel 5
    io.write_bytes(0_u32, format) # Size 0
    io.write_byte(206_u8)         # Frame end marker
    io.rewind

    frame = AMQ::Protocol::Stream.new(io).next_frame.as(AMQ::Protocol::Frame::Heartbeat)
    frame.channel.should eq 5
  end
end

describe AMQ::Protocol::Frame::Method::Basic::Publish do
  it "is encoded as AMQP 0-9-1 bytes" do
    frame = AMQ::Protocol::Frame::Method::Basic::Publish.new(1_u16, 0_u16, "amq.direct", "rk", true, false)
    io = IO::Memory.new
    frame.to_io(io, IO::ByteFormat::NetworkEndian)
    io.to_slice.should eq Bytes[
      1, 0, 1, 0, 0, 0, 21,                              # method frame, channel 1, 21 bytes
      0, 60, 0, 40,                                      # basic.publish
      0, 0,                                              # reserved1
      10, 97, 109, 113, 46, 100, 105, 114, 101, 99, 116, # "amq.direct"
      2, 114, 107,                                       # "rk"
      1,                                                 # mandatory
      206,                                               # frame end
    ]
  end

  it "is encoded with multibyte names in little endian" do
    frame = AMQ::Protocol::Frame::Method::Basic::Publish.new(1_u16, 0_u16, "åäö", "rk", false, false)
    io = IO::Memory.new
    frame.to_io(io, IO::ByteFormat::LittleEndian)
    io.to_slice.should eq Bytes[
      1, 1, 0, 17, 0, 0, 0,                  # method frame, channel 1, 17 bytes
      60, 0, 40, 0,                          # basic.publish
      0, 0,                                  # reserved1
      6, 0xc3, 0xa5, 0xc3, 0xa4, 0xc3, 0xb6, # "åäö"
      2, 114, 107,                           # "rk"
      0,                                     # no flags
      206,                                   # frame end
    ]
  end

  it "can be encoded and decoded with the longest names" do
    io = IO::Memory.new
    f_out = AMQ::Protocol::Frame::Method::Basic::Publish.new(1_u16, 0_u16, "e" * 255, "r" * 255, false, true)
    f_out.to_io(io, IO::ByteFormat::NetworkEndian)
    io.size.should eq 527
    io.rewind
    AMQ::Protocol::Stream.new(io).next_frame do |f_in|
      f_out.should eq f_in
    end
  end

  it "rejects names longer than a short string without writing anything" do
    frame = AMQ::Protocol::Frame::Method::Basic::Publish.new(1_u16, 0_u16, "", "r" * 256, false, false)
    io = IO::Memory.new
    expect_raises(ArgumentError) do
      frame.to_io(io, IO::ByteFormat::NetworkEndian)
    end
    io.size.should eq 0
  end
end

describe AMQ::Protocol::Frame::Header do
  it "is encoded as AMQP 0-9-1 bytes" do
    props = AMQ::Protocol::Properties.new(content_type: "text/plain", delivery_mode: 2_u8)
    frame = AMQ::Protocol::Frame::Header.new(1_u16, 60_u16, 0_u16, 16_u64, props)
    io = IO::Memory.new
    frame.to_io(io, IO::ByteFormat::NetworkEndian)
    io.to_slice.should eq Bytes[
      2, 0, 1, 0, 0, 0, 26,                               # header frame, channel 1, 26 bytes
      0, 60, 0, 0,                                        # class basic, weight 0
      0, 0, 0, 0, 0, 0, 0, 16,                            # body size 16
      0x90, 0,                                            # content_type and delivery_mode
      10, 116, 101, 120, 116, 47, 112, 108, 97, 105, 110, # "text/plain"
      2,                                                  # persistent
      206,                                                # frame end
    ]
  end
end

describe AMQ::Protocol::Frame::BytesBody do
  it "is encoded as AMQP 0-9-1 bytes" do
    frame = AMQ::Protocol::Frame::BytesBody.new(1_u16, 3_u32, Bytes[1, 2, 3])
    io = IO::Memory.new
    frame.to_io(io, IO::ByteFormat::NetworkEndian)
    io.to_slice.should eq Bytes[3, 0, 1, 0, 0, 0, 3, 1, 2, 3, 206]
  end

  it "is encoded the same on both sides of the size it copies up to" do
    {256, 257}.each do |size|
      body = Bytes.new(size, &.to_u8!)
      frame = AMQ::Protocol::Frame::BytesBody.new(1_u16, size.to_u32, body)
      io = IO::Memory.new
      frame.to_io(io, IO::ByteFormat::NetworkEndian)
      bytes = io.to_slice
      bytes.size.should eq size + 8
      bytes[0, 7].should eq Bytes[3, 0, 1, 0, 0, 1, size - 256]
      bytes[7, size].should eq body
      bytes[-1].should eq 206
    end
  end
end
