require "./spec_helper"

describe AMQ::Protocol::Frame::Method::Basic::Get do
  it "can be encoded and decoded" do
    io = IO::Memory.new
    f_out = AMQ::Protocol::Frame::Method::Basic::Get.new(1_u16, 0_u16, AMQ::Protocol::ShortString.new("myqueue"), true)
    f_out.to_io(io, IO::ByteFormat::NetworkEndian)
    io.rewind
    AMQ::Protocol::Frame.from_io(io, IO::ByteFormat::NetworkEndian) do |f_in|
      f_out.should eq f_in
    end
  end
end
