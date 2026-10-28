require "./spec_helper"

describe AMQ::Protocol::VERSION do
  it "version matches shard version" do
    AMQ::Protocol::VERSION.should eq {{ `shards version`.strip.stringify }}
  end
end
