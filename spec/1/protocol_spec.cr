require "../spec_helper"
require "../../src/amq/protocol.cr"

describe AMQ::Protocol do
  it "defines AMQP 1.0 header" do
    AMQ::Protocol::PROTOCOL_START_1_0_0.to_slice.to_a.should eq "AMQP\x00\x01\x00\x00".bytes
  end
end
